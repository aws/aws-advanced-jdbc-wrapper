/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.ConnectInfo;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SlidingExpirationCache;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class HikariPooledConnectionProvider implements PooledConnectionProvider,
    CanReleaseResources {

  private static final String thisClassName = HikariPooledConnectionProvider.class.getName();
  private static final Logger LOGGER = Logger.getLogger(HikariPooledConnectionProvider.class.getName());

  protected static final Map<String, HostSelector> acceptedStrategies =
      Collections.unmodifiableMap(new HashMap<String, HostSelector>() {
        {
          put(HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT, new HighestWeightHostSelector());
          put(RandomHostSelector.STRATEGY_RANDOM, new RandomHostSelector());
          put(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN, new RoundRobinHostSelector());
        }
      });

  protected static final RdsUtils rdsUtils = new RdsUtils();
  protected static long poolExpirationCheckNanos = TimeUnit.MINUTES.toNanos(30);
  protected final HikariPoolConfigurator poolConfigurator;
  protected final HikariPoolMapping poolMapping;
  protected final AcceptsUrlFunc acceptsUrlFunc;
  protected final LeastConnectionsHostSelector leastConnectionsHostSelector;

  static {
    HikariPoolsHolder.databasePools.setShouldDisposeFunc(
        (hikariDataSource) -> {
          if (hikariDataSource instanceof HikariDataSource) {
            return ((HikariDataSource) hikariDataSource).getHikariPoolMXBean().getActiveConnections() == 0;
          }
          return true;
        });
  }

  /**
   * {@link HikariPooledConnectionProvider} constructor. This class can be passed to
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} to enable internal connection pools for
   * each database instance in a cluster. By maintaining internal connection pools, the driver can
   * improve performance by reusing old {@link Connection} objects.
   *
   * @param hikariPoolConfigurator a function that returns a {@link HikariConfig} with specific
   *                               Hikari configurations. By default, the
   *                               {@link HikariPooledConnectionProvider} will configure the
   *                               jdbcUrl, exceptionOverrideClassName, username, and password. Any
   *                               additional configuration should be defined by passing in this
   *                               parameter. If no additional configuration is desired, pass in a
   *                               {@link HikariPoolConfigurator} that returns an empty
   *                               HikariConfig.
   */
  public HikariPooledConnectionProvider(HikariPoolConfigurator hikariPoolConfigurator) {
    this(hikariPoolConfigurator, null);
  }

  /**
   * {@link HikariPooledConnectionProvider} constructor. This class can be passed to
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} to enable internal connection pools for
   * each database instance in a cluster. By maintaining internal connection pools, the driver can
   * improve performance by reusing old {@link Connection} objects.
   *
   * @param hikariPoolConfigurator a function that returns a {@link HikariConfig} with specific
   *                               Hikari configurations. By default, the
   *                               {@link HikariPooledConnectionProvider} will configure the
   *                               jdbcUrl, exceptionOverrideClassName, username, and password. Any
   *                               additional configuration should be defined by passing in this
   *                               parameter. If no additional configuration is desired, pass in a
   *                               {@link HikariPoolConfigurator} that returns an empty
   *                               HikariConfig.
   * @param mapping                a function that returns a String key used for the internal
   *                               connection pool keys. An internal connection pool will be
   *                               generated for each unique key returned by this function.
   */
  public HikariPooledConnectionProvider(
      HikariPoolConfigurator hikariPoolConfigurator, HikariPoolMapping mapping) {
    this.poolConfigurator = hikariPoolConfigurator;
    this.poolMapping = mapping;
    this.acceptsUrlFunc = null;
    this.leastConnectionsHostSelector = new LeastConnectionsHostSelector(HikariPoolsHolder.databasePools);
  }

  /**
   * {@link HikariPooledConnectionProvider} constructor. This class can be passed to
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} to enable internal connection pools for
   * each database instance in a cluster. By maintaining internal connection pools, the driver can
   * improve performance by reusing old {@link Connection} objects.
   *
   * @param hikariPoolConfigurator a function that returns a {@link HikariConfig} with specific
   *                               Hikari configurations. By default, the
   *                               {@link HikariPooledConnectionProvider} will configure the
   *                               jdbcUrl, exceptionOverrideClassName, username, and password. Any
   *                               additional configuration should be defined by passing in this
   *                               parameter. If no additional configuration is desired, pass in a
   *                               {@link HikariPoolConfigurator} that returns an empty
   *                               HikariConfig.
   * @param mapping                a function that returns a String key used for the internal
   *                               connection pool keys. An internal connection pool will be
   *                               generated for each unique key returned by this function.
   * @param poolExpirationNanos    the amount of time that a pool should sit in the cache before
   *                               being marked as expired for cleanup, in nanoseconds. Expired
   *                               pools can still be used and will not be closed unless there
   *                               are no active connections.
   * @param poolCleanupNanos       the interval defining how often expired connection pools
   *                               should be cleaned up, in nanoseconds. Note that expired pools
   *                               will not be closed unless there are no active connections.
   */
  public HikariPooledConnectionProvider(
      HikariPoolConfigurator hikariPoolConfigurator,
      HikariPoolMapping mapping,
      long poolExpirationNanos,
      long poolCleanupNanos) {
    this.poolConfigurator = hikariPoolConfigurator;
    this.poolMapping = mapping;
    this.acceptsUrlFunc = null;
    poolExpirationCheckNanos = poolExpirationNanos;
    HikariPoolsHolder.databasePools.setCleanupIntervalNanos(poolCleanupNanos);
    this.leastConnectionsHostSelector = new LeastConnectionsHostSelector(HikariPoolsHolder.databasePools);
  }

  /**
   * {@link HikariPooledConnectionProvider} constructor. This class can be passed to
   * {@link Driver#setCustomConnectionProvider(ConnectionProvider)} to enable internal connection pools for
   * each database instance in a cluster. By maintaining internal connection pools, the driver can
   * improve performance by reusing old {@link Connection} objects.
   *
   * @param hikariPoolConfigurator a function that returns a {@link HikariConfig} with specific
   *                               Hikari configurations. By default, the
   *                               {@link HikariPooledConnectionProvider} will configure the
   *                               jdbcUrl, exceptionOverrideClassName, username, and password. Any
   *                               additional configuration should be defined by passing in this
   *                               parameter. If no additional configuration is desired, pass in a
   *                               {@link HikariPoolConfigurator} that returns an empty
   *                               HikariConfig.
   * @param mapping                a function that returns a String key used for the internal
   *                               connection pool keys. An internal connection pool will be
   *                               generated for each unique key returned by this function.
   * @param acceptsUrlFunc         a function that defines when an internal connection pool should be created for a
   *                               requested connection. An internal connection pool will be created when the connect
   *                               pipeline is being executed and this function returns <code>true</code>.
   * @param poolExpirationNanos    the amount of time that a pool should sit in the cache before
   *                               being marked as expired for cleanup, in nanoseconds. Expired
   *                               pools can still be used and will not be closed unless there
   *                               are no active connections.
   * @param poolCleanupNanos       the interval defining how often expired connection pools
   *                               should be cleaned up, in nanoseconds. Note that expired pools
   *                               will not be closed unless there are no active connections.
   */
  public HikariPooledConnectionProvider(
      HikariPoolConfigurator hikariPoolConfigurator,
      HikariPoolMapping mapping,
      AcceptsUrlFunc acceptsUrlFunc,
      long poolExpirationNanos,
      long poolCleanupNanos) {
    this.poolConfigurator = hikariPoolConfigurator;
    this.poolMapping = mapping;
    this.acceptsUrlFunc = acceptsUrlFunc;
    poolExpirationCheckNanos = poolExpirationNanos;
    HikariPoolsHolder.databasePools.setCleanupIntervalNanos(poolCleanupNanos);
    this.leastConnectionsHostSelector = new LeastConnectionsHostSelector(HikariPoolsHolder.databasePools);
  }


  @Override
  public boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    if (this.acceptsUrlFunc != null) {
      return this.acceptsUrlFunc.acceptsUrl(hostSpec, props);
    }

    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostSpec.getHost());
    return RdsUrlType.RDS_INSTANCE.equals(urlType);
  }

  @Override
  public boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy) {
    return acceptedStrategies.containsKey(strategy)
        || LeastConnectionsHostSelector.STRATEGY_LEAST_CONNECTIONS.equals(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts,
      @NonNull HostRole role,
      @NonNull String strategy,
      @Nullable Properties props) throws SQLException {
    if (!acceptsStrategy(role, strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, DataSourceConnectionProvider.class}));
    }
    if (LeastConnectionsHostSelector.STRATEGY_LEAST_CONNECTIONS.equals(strategy)) {
      return this.leastConnectionsHostSelector.getHost(hosts, role, props);
    } else {
      return acceptedStrategies.get(strategy).getHost(hosts, role, props);
    }
  }

  @Override
  public Connection connect(
      @NonNull String protocol,
      @NonNull Dialect dialect,
      @NonNull TargetDriverDialect targetDriverDialect,
      @NonNull HostSpec hostSpec,
      @NonNull Properties props)
      throws SQLException {

    final Properties copy = PropertyUtils.copyProperties(props);
    HostSpec connectionHostSpec = hostSpec;

    if (PropertyDefinition.ENABLE_GREEN_NODE_REPLACEMENT.getBoolean(props)
        && rdsUtils.isRdsDns(hostSpec.getHost())
        && rdsUtils.isGreenInstance(hostSpec.getHost())) {

      // check DNS for such green host name
      InetAddress resolvedAddress = null;
      try {
        resolvedAddress = InetAddress.getByName(hostSpec.getHost());
      } catch (UnknownHostException tmp) {
        // do nothing
      }

      if (resolvedAddress == null) {
        // Green node DNS doesn't exist

        final String fixedHost = rdsUtils.removeGreenInstancePrefix(hostSpec.getHost());
        connectionHostSpec = new HostSpecBuilder(hostSpec.getHostAvailabilityStrategy())
            .copyFrom(hostSpec)
            .host(fixedHost)
            .build();
      }
    }

    final HostSpec finalHostSpec = connectionHostSpec;
    dialect.prepareConnectProperties(copy, protocol, finalHostSpec);

    final HikariDataSource ds = (HikariDataSource) HikariPoolsHolder.databasePools.computeIfAbsent(
        Pair.create(hostSpec.getUrl(), getPoolKey(finalHostSpec, copy)),
        (lambdaPoolKey) -> createHikariDataSource(protocol, finalHostSpec, copy, targetDriverDialect),
        poolExpirationCheckNanos
    );

    ds.setPassword(copy.getProperty(PropertyDefinition.PASSWORD.name));

    return ds.getConnection();
  }

  // The pool key should always be retrieved using this method, because the username
  // must always be included to avoid sharing privileged connections with other users.
  protected String getPoolKey(HostSpec hostSpec, Properties props) {
    if (this.poolMapping != null) {
      return this.poolMapping.getKey(hostSpec, props);
    }

    // Otherwise use default map key
    String user = props.getProperty(PropertyDefinition.USER.name);
    return user == null ? "" : user;
  }

  @Override
  public void releaseResources() {
    HikariPoolsHolder.closeAllPools();
  }

  /**
   * Configures the default required settings for the internal connection pool.
   *
   * @param config          the {@link HikariConfig} to configure. By default, this method sets the
   *                        jdbcUrl, exceptionOverrideClassName, username, and password. The
   *                        HikariConfig passed to this method should be created via a
   *                        {@link HikariPoolConfigurator}, which allows the user to specify any
   *                        additional configuration properties.
   * @param protocol        the driver protocol that should be used to form connections
   * @param hostSpec        the host details used to form the connection
   * @param connectionProps the connection properties
   * @param targetDriverDialect the target driver dialect {@link TargetDriverDialect}
   */
  protected void configurePool(
      final HikariConfig config,
      final String protocol,
      final HostSpec hostSpec,
      final Properties connectionProps,
      final @NonNull TargetDriverDialect targetDriverDialect) {

    final Properties copy = PropertyUtils.copyProperties(connectionProps);

    ConnectInfo connectInfo;
    try {
      connectInfo = targetDriverDialect.prepareConnectInfo(
          protocol, hostSpec, copy);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    StringBuilder urlBuilder = new StringBuilder(connectInfo.url);

    final StringJoiner propsJoiner = new StringJoiner("&");
    connectInfo.props.forEach((k, v) -> {
      if (!PropertyDefinition.PASSWORD.name.equals(k) && !PropertyDefinition.USER.name.equals(k)) {
        propsJoiner.add(k + "=" + v);
      }
    });

    if (connectInfo.url.contains("?")) {
      urlBuilder.append("&").append(propsJoiner);
    } else {
      urlBuilder.append("?").append(propsJoiner);
    }

    config.setJdbcUrl(urlBuilder.toString());

    final String user = connectInfo.props.getProperty(PropertyDefinition.USER.name);
    final String password = connectInfo.props.getProperty(PropertyDefinition.PASSWORD.name);
    if (user != null) {
      config.setUsername(user);
    }
    if (password != null) {
      config.setPassword(password);
    }
  }

  /**
   * Returns the number of active connection pools.
   *
   * @return the number of active connection pools
   */
  public int getHostCount() {
    return HikariPoolsHolder.databasePools.size();
  }

  /**
   * Returns a set containing every host URL for which there are one or more connection pool(s).
   *
   * @return a set containing every host URL for which there are one or more connection pool(s).
   */
  public Set<String> getHosts() {
    return Collections.unmodifiableSet(
        HikariPoolsHolder.databasePools.getEntries().keySet().stream()
            .map(poolKey -> (String) poolKey.getValue1())
            .collect(Collectors.toSet()));
  }

  /**
   * Returns a set containing every key associated with an active connection pool.
   *
   * @return a set containing every key associated with an active connection pool
   */
  public Set<Pair> getKeys() {
    return HikariPoolsHolder.databasePools.getEntries().keySet();
  }

  @Override
  public String getTargetName() {
    return thisClassName;
  }

  /**
   * Logs information for every active connection pool.
   */
  public void logConnections() {
    LOGGER.finest(() -> {
      final StringBuilder builder = new StringBuilder();
      HikariPoolsHolder.databasePools.getEntries().forEach((key, dataSource) -> {
        builder.append("\t[ ");
        builder.append(key).append(":");
        builder.append("\n\t {");
        builder.append("\n\t\t").append(dataSource);
        builder.append("\n\t }\n");
        builder.append("\t");
      });
      return String.format("Hikari Pooled Connection: \n[\n%s\n]", builder);
    });
  }

  HikariDataSource createHikariDataSource(
      final String protocol,
      final HostSpec hostSpec,
      final Properties props,
      final @NonNull TargetDriverDialect targetDriverDialect) {

    HikariConfig config = poolConfigurator.configurePool(hostSpec, props);
    configurePool(config, protocol, hostSpec, props, targetDriverDialect);
    return new HikariDataSource(config);
  }

  // For testing purposes only
  void setDatabasePools(SlidingExpirationCache<Pair, AutoCloseable> connectionPools) {
    HikariPoolsHolder.databasePools = connectionPools;
  }
}
