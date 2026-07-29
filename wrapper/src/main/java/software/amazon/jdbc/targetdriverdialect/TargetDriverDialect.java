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

package software.amazon.jdbc.targetdriverdialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;

public interface TargetDriverDialect {

  boolean isDialect(java.sql.Driver driver);

  boolean isDialect(final String dataSourceClass);

  ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException;

  void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException;

  /**
   * Applies AWS Wrapper connection settings that a target data source can only receive through
   * driver-specific bean setters (for example connect/socket timeouts, whose unit differs per
   * driver), and returns the connection URL to apply to that data source.
   *
   * <p>This is used by the XA datasource path, which configures a target driver
   * {@link javax.sql.XADataSource} directly instead of going through
   * {@link #prepareConnectInfo(String, HostSpec, Properties)}. Because that path hands the target
   * only its own (non-wrapper) properties, wrapper properties such as {@code socketTimeout} would
   * otherwise be dropped. Dialects whose target data source cannot express a setting as a bean
   * property may instead add it to the returned URL.
   *
   * <p>The default implementation applies nothing and returns {@code url} unchanged.
   *
   * @param dataSource the target driver data source being configured.
   * @param url        the target driver URL, with AWS Wrapper parameters already removed.
   * @param props      the effective connection properties, including AWS Wrapper properties.
   * @return the URL to apply to the target data source.
   * @throws SQLException if a setting cannot be applied to the target data source.
   */
  default String prepareTargetDataSource(
      final @NonNull CommonDataSource dataSource,
      final @NonNull String url,
      final @NonNull Properties props) throws SQLException {
    return url;
  }

  boolean isDriverRegistered() throws SQLException;

  void registerDriver() throws SQLException;

  /**
   * Attempts to communicate to a database node in order to measure network latency.
   * Some database protocols may not support the simplest "ping" packet. In this case,
   * it's recommended to execute a simple connection validation, or the simplest SQL
   * query like "SELECT 1".
   *
   * @param connection The database connection to a node to ping.
   * @return True, if operation is succeeded. False, otherwise.
   */
  boolean ping(final @NonNull Connection connection);

  Set<String> getAllowedOnConnectionMethodNames();

  String getSQLState(final Throwable throwable);

  Set<String> getNetworkBoundMethodNames(final @Nullable Properties properties);

  void abortConnection(final @NonNull Connection connectionToAbort, final @NonNull Executor abortExecutor)
      throws SQLException;

  @Nullable String getSQLQueryString(PreparedStatement ps);

  void registerDataType(final @NonNull Connection connection,  final @NonNull String typeName,
      final @NonNull String className)
      throws SQLException;

  void setEncryptedParameter(final @NonNull PreparedStatement ps, int paramIndex, byte[] encrypted)
      throws SQLException;

  byte @Nullable [] getEncryptedBytes(final @NonNull ResultSet rs, Object columnRef) throws SQLException;

  /**
   * Allows each target driver dialect to perform last-minute adjustments to the wrapper's
   * internal state after the initial connection is established. This includes transferring
   * driver-specific connection properties (e.g. PostgreSQL's {@code currentSchema}) into
   * the wrapper's session state so they are preserved across connection switches
   * (failover, read-write splitting, initial connection strategy, etc.).
   *
   * <p>Each dialect implementation should inspect the provided properties for driver-specific
   * settings that affect session state and register them with the appropriate services
   * (e.g. {@link software.amazon.jdbc.states.SessionStateService}).
   *
   * <p>This method is called once after the initial connection is established in
   * {@code ConnectionWrapper.init()}.
   *
   * @param pluginService the plugin service providing access to session state and other services
   * @param props         the connection properties that may contain driver-specific settings
   * @throws SQLException if an error occurs while updating internal state
   */
  void updateInternalState(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) throws SQLException;
}
