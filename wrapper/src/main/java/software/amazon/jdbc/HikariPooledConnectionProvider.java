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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.util.HikariCPSQLException;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class HikariPooledConnectionProvider implements PooledConnectionProvider,
    CanReleaseResources {

  private static final Logger LOGGER =
      Logger.getLogger(HikariPooledConnectionProvider.class.getName());

  private static final RdsUtils rdsUtils = new RdsUtils();
  private static final Map<String, HikariDataSource> databasePools = new ConcurrentHashMap<>();
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
    this(hikariPoolConfigurator, (hostSpec, properties) -> hostSpec.getUrl());
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
    return false;
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy) {
    // This class does not accept any strategy, so the ConnectionProviderManager should prevent us
    // from getting here.
    return null;
  }

  @Override
  public Connection connect(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props)
      throws SQLException {
    final HikariDataSource ds = databasePools.computeIfAbsent(
        poolMapping.getKey(hostSpec, props),
        url -> createHikariDataSource(protocol, hostSpec, props)
    );

    Connection conn = ds.getConnection();
    int count = 0;
    while (conn != null && count++ < retries && !conn.isValid(3)) {
      ds.evictConnection(conn);
      conn = ds.getConnection();
    }
    return conn;
  }

  @Override
  public Connection connect(
      @NonNull String url, @NonNull Properties props) throws SQLException {
    // This method is only called by tests/benchmarks
    return null;
  }

  @Override
  public void releaseResources() {
    databasePools.forEach((String url, HikariDataSource ds) -> ds.close());
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

    if (connectionProps.containsKey("permitMysqlScheme")) {
      urlBuilder.append("?permitMysqlScheme");
    }

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
   * Returns a set containing every key associated with an active connection pool.
   *
   * @return a set containing every key associated with an active connection pool
   */
  public Set<String> getHosts() {
    return Collections.unmodifiableSet(databasePools.keySet());
  }

  /**
   * Logs information for every active connection pool.
   */
  public void logConnections() {
    LOGGER.finest(() -> {
      final StringBuilder builder = new StringBuilder();
      databasePools.forEach((key, dataSource) -> {
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
}
