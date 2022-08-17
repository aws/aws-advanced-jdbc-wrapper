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

package software.aws.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Implement this interface in order to handle physical connection creation process.
 */
public interface ConnectionProvider {

  /**
   * Called once per connection that needs to be created.
   *
   * @param protocol The connection protocol (example "jdbc:mysql://")
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props    The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  Connection connect(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props)
      throws SQLException;

  /**
   * Called once per connection that needs to be created.
   *
   * @param url The connection URL
   * @param props The Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  Connection connect(@NonNull String url, @NonNull Properties props)
      throws SQLException; // TODO: do we need this method?
}
