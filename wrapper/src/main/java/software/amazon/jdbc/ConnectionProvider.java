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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.dialect.Dialect;

/**
 * Implement this interface in order to handle the physical connection creation process.
 */
public interface ConnectionProvider {
  /**
   * Indicates whether this ConnectionProvider can provide connections for the given host and
   * properties. Some ConnectionProvider implementations may not be able to handle certain URL
   * types or properties.
   *
   * @param protocol the connection protocol (example "jdbc:mysql://")
   * @param hostSpec the HostSpec containing the host-port information for the host to connect to
   * @param props    the Properties to use for the connection
   * @return true if this ConnectionProvider can provide connections for the given URL, otherwise
   *         return false
   */
  boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props);

  /**
   * Indicates whether the selection strategy is supported by the connection provider.
   *
   * @param role     determines if the connection provider should return a reader host or a writer
   *                 host
   * @param strategy the selection strategy to use
   * @return whether the strategy is supported
   */
  boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy);

  /**
   * Return a reader or a writer node using the specified strategy. This method should raise an
   * {@link UnsupportedOperationException} if the specified strategy is unsupported.
   *
   * @param hosts    the list of {@link HostSpec} to select from
   * @param role     determines if the connection provider should return a writer or a reader
   * @param strategy the strategy determining how the {@link HostSpec} should be selected, e.g.,
   *                 random or round-robin
   * @return the {@link HostSpec} selected using the specified strategy
   * @throws SQLException                  if an error occurred while returning the hosts
   * @throws UnsupportedOperationException if the strategy is unsupported by the provider
   */
  HostSpec getHostSpecByStrategy(
      @NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy)
      throws SQLException;

  /**
   * Called once per connection that needs to be created.
   *
   * @param protocol the connection protocol (example "jdbc:mysql://")
   * @param dialect  the database dialect
   * @param hostSpec the HostSpec containing the host-port information for the host to connect to
   * @param props    the Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  Connection connect(
      @NonNull String protocol,
      @NonNull Dialect dialect,
      @NonNull HostSpec hostSpec,
      @NonNull Properties props,
      boolean isInitialConnection)
      throws SQLException;

  /**
   * Called once per connection that needs to be created.
   *
   * @param url   the connection URL
   * @param props the Properties to use for the connection
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  Connection connect(@NonNull String url, @NonNull Properties props)
      throws SQLException; // TODO: this method is only called by tests/benchmarks and can likely be deprecated
}
