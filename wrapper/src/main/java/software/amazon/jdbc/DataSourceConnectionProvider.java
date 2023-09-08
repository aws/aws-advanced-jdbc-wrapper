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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.exceptions.SQLLoginException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * This class is a basic implementation of {@link ConnectionProvider} interface. It creates and
 * returns an instance of PgConnection.
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

  private static final Logger LOGGER =
      Logger.getLogger(DataSourceConnectionProvider.class.getName());
  private static final Map<String, HostSelector> acceptedStrategies =
      Collections.unmodifiableMap(new HashMap<String, HostSelector>() {
        {
          put(RandomHostSelector.STRATEGY_RANDOM, new RandomHostSelector());
          put(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN, new RoundRobinHostSelector());
        }
      });
  private final @NonNull DataSource dataSource;
  private final @NonNull TargetDriverDialect targetDriverDialect;


  private final ReentrantLock lock = new ReentrantLock();

  public DataSourceConnectionProvider(
      final @NonNull DataSource dataSource,
      final @NonNull TargetDriverDialect targetDriverDialect) {
    this.dataSource = dataSource;
    this.targetDriverDialect = targetDriverDialect;
  }

  /**
   * Indicates whether this ConnectionProvider can provide connections for the given host and
   * properties. Some ConnectionProvider implementations may not be able to handle certain URL
   * types or properties.
   *
   * @param protocol The connection protocol (example "jdbc:mysql://")
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props    The Properties to use for the connection
   * @return true if this ConnectionProvider can provide connections for the given URL, otherwise
   *         return false
   */
  @Override
  public boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    return true;
  }

  @Override
  public boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy) {
    return acceptedStrategies.containsKey(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy, @Nullable Properties props)
      throws SQLException {
    if (!acceptedStrategies.containsKey(strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, DataSourceConnectionProvider.class}));
    }

    return acceptedStrategies.get(strategy).getHost(hosts, role, props);
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param protocol The connection protocol (example "jdbc:mysql://")
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  @Override
  public Connection connect(
      final @NonNull String protocol,
      final @NonNull Dialect dialect,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props)
      throws SQLException {

    final Properties copy = PropertyUtils.copyProperties(props);

    Connection conn;

    if (this.lock.isLocked()) {

      LOGGER.finest(() -> "Use a separate DataSource object to create a connection.");
      // use a new data source instance to instantiate a connection
      final DataSource ds = createDataSource();
      this.targetDriverDialect.prepareDataSource(
          ds,
          protocol,
          hostSpec,
          copy);
      conn = ds.getConnection();

    } else {

      // Data Source object could be shared between different threads while failover in progress.
      // That's why it's important to configure Data Source object and get connection atomically.
      this.lock.lock();
      LOGGER.finest(() -> "Use main DataSource object to create a connection.");
      try {
        this.targetDriverDialect.prepareDataSource(
            this.dataSource,
            protocol,
            hostSpec,
            copy);
        conn = this.dataSource.getConnection();
      } finally {
        this.lock.unlock();
      }
    }

    if (conn == null) {
      throw new SQLLoginException(Messages.get("ConnectionProvider.noConnection"));
    }

    return conn;
  }

  /**
   * Called once per connection that needs to be created.
   *
   * @param url The connection URL
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  @Override
  @Deprecated
  public Connection connect(final @NonNull String url, final @NonNull Properties props) throws SQLException {
    final Properties copy = PropertyUtils.copyProperties(props);
    copy.setProperty("url", url);
    PropertyDefinition.removeAllExceptCredentials(copy);
    PropertyUtils.applyProperties(this.dataSource, copy);
    return this.dataSource.getConnection();
  }

  private DataSource createDataSource() throws SQLException {
    try {
      return WrapperUtils.createInstance(this.dataSource.getClass(), DataSource.class, null);
    } catch (final InstantiationException instEx) {
      throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getState(), instEx);
    }
  }
}
