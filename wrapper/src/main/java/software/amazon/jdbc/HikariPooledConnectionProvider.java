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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class HikariPooledConnectionProvider implements PooledConnectionProvider,
    CanReleaseResources {

  private static final Logger LOGGER =
      Logger.getLogger(HikariPooledConnectionProvider.class.getName());

  private static final String LEAST_CONNECTIONS_STRATEGY = "leastConnections";

  private static final RdsUtils rdsUtils = new RdsUtils();

  // 2-layer map that helps us keep track of how many pools are associated with each database
  // instance. The first layer maps from an instance URL to the 2nd map. The 2nd layer maps from
  // the key returned from the poolMapping function to the database pool.
  private static final CacheMap<PoolKey, HikariDataSource> databasePools = new CacheMap<>();
  private static final long poolExpirationNanos = TimeUnit.MINUTES.toNanos(30);
  private final HikariPoolConfigurator poolConfigurator;
  private final HikariPoolMapping poolMapping;
  protected int retries = 10;

  /**
   * {@link HikariPooledConnectionProvider} constructor. This class can be passed to
   * {@link ConnectionProviderManager#setConnectionProvider} to enable internal connection pools for
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
   * {@link ConnectionProviderManager#setConnectionProvider} to enable internal connection pools for
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
      HikariPoolConfigurator hikariPoolConfigurator,
      HikariPoolMapping mapping) {
    this.poolConfigurator = hikariPoolConfigurator;
    this.poolMapping = mapping;
  }

  @Override
  public boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostSpec.getHost());
    return RdsUrlType.RDS_INSTANCE.equals(urlType);
  }

  @Override
  public boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy) {
    return LEAST_CONNECTIONS_STRATEGY.equals(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy)
      throws SQLException {
    if (!LEAST_CONNECTIONS_STRATEGY.equals(strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, HikariPooledConnectionProvider.class}));
    }

    // Remove hosts with the wrong role
    List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec -> role.equals(hostSpec.getRole()))
        .sorted((hostSpec1, hostSpec2) ->
            getNumConnections(hostSpec1) - getNumConnections(hostSpec2))
        .collect(Collectors.toList());

    if (eligibleHosts.size() == 0) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    return eligibleHosts.get(0);
  }

  private int getNumConnections(HostSpec hostSpec) {
    int numConnections = 0;
    final String url = hostSpec.getUrl();
    for (Entry<PoolKey, HikariDataSource> entry : databasePools.getEntries().entrySet()) {
      if (!url.equals(entry.getKey().url)) {
        continue;
      }
      numConnections += entry.getValue().getHikariPoolMXBean().getActiveConnections();
    }
    return numConnections;
  }

  @Override
  public Connection connect(
      @NonNull String protocol,
      @NonNull Dialect dialect,
      @NonNull HostSpec hostSpec,
      @NonNull Properties props,
      boolean isInitialConnection)
      throws SQLException {

    final HikariDataSource ds = databasePools.computeIfAbsent(
        new PoolKey(hostSpec.getUrl(), getPoolKey(hostSpec, props)),
        (lambdaPoolKey) -> createHikariDataSource(protocol, hostSpec, props),
        poolExpirationNanos,
        (hikariDataSource) -> hikariDataSource.close()
    );

    ds.setPassword(props.getProperty(PropertyDefinition.PASSWORD.name));

    return ds.getConnection();
  }

  @Override
  public Connection connect(@NonNull String url, @NonNull Properties props) throws SQLException {
    // This method is only called by tests/benchmarks
    return null;
  }

  // The pool key should always be retrieved using this method, because the username
  // must always be included to avoid sharing privileged connections with other users.
  private String getPoolKey(HostSpec hostSpec, Properties props) {
    if (this.poolMapping != null) {
      return this.poolMapping.getKey(hostSpec, props);
    }

    // Otherwise use default map key
    String user = props.getProperty(PropertyDefinition.USER.name);
    return user == null ? "" : user;
  }

  @Override
  public void releaseResources() {
    databasePools.getEntries().forEach((poolKey, pool) -> {
      if (!pool.isClosed()) {
        pool.close();
      }
    });
    databasePools.clear();
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
   */
  protected void configurePool(
      HikariConfig config, String protocol, HostSpec hostSpec, Properties connectionProps) {
    StringBuilder urlBuilder = new StringBuilder().append(protocol).append(hostSpec.getUrl());

    final String db = PropertyDefinition.DATABASE.getString(connectionProps);
    if (!StringUtils.isNullOrEmpty(db)) {
      urlBuilder.append(db);
    }

    final StringJoiner propsJoiner = new StringJoiner("&");
    connectionProps.forEach((k, v) -> {
      if (!PropertyDefinition.PASSWORD.name.equals(k) && !PropertyDefinition.USER.name.equals(k)) {
        propsJoiner.add(k + "=" + v);
      }
    });
    urlBuilder.append("?").append(propsJoiner);

    config.setJdbcUrl(urlBuilder.toString());
    config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());

    final String user = connectionProps.getProperty(PropertyDefinition.USER.name);
    final String password = connectionProps.getProperty(PropertyDefinition.PASSWORD.name);
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
    return databasePools.size();
  }

  /**
   * Returns a set containing every host URL for which there are one or more connection pool(s).
   *
   * @return a set containing every host URL for which there are one or more connection pool(s).
   */
  public Set<String> getHosts() {
    return Collections.unmodifiableSet(
        databasePools.getEntries().keySet().stream()
            .map(poolKey -> poolKey.url)
            .collect(Collectors.toSet()));
  }

  /**
   * Returns a set containing every key associated with an active connection pool.
   *
   * @return a set containing every key associated with an active connection pool
   */
  public Set<PoolKey> getKeys() {
    return databasePools.getEntries().keySet();
  }

  /**
   * Logs information for every active connection pool.
   */
  public void logConnections() {
    LOGGER.finest(() -> {
      final StringBuilder builder = new StringBuilder();
      databasePools.getEntries().forEach((key, dataSource) -> {
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

  HikariDataSource createHikariDataSource(String protocol, HostSpec hostSpec, Properties props) {
    HikariConfig config = poolConfigurator.configurePool(hostSpec, props);
    configurePool(config, protocol, hostSpec, props);
    return new HikariDataSource(config);
  }

  public static class PoolKey {
    private final @NonNull String url;
    private final @NonNull String extraKey;

    public PoolKey(final @NonNull String url, final @NonNull String extraKey) {
      this.url = url;
      this.extraKey = extraKey;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((url == null) ? 0 : url.hashCode()) + ((extraKey == null) ? 0 : extraKey.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final PoolKey other = (PoolKey) obj;
      return this.url.equals(other.url) && this.extraKey.equals(other.extraKey);
    }

    @Override
    public String toString() {
      return "PoolKey [url=" + url + ", extraKey=" + extraKey + "]";
    }

  }
}
