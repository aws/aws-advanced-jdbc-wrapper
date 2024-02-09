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
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;

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
}
