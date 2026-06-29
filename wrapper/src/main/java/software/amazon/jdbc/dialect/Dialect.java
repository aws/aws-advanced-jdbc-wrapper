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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.Pair;

public interface Dialect {

  boolean isDialect(Connection connection);

  int getDefaultPort();

  List</* dialect code */ String> getDialectUpdateCandidates();

  ExceptionHandler getExceptionHandler();

  HostListProviderSupplier getHostListProviderSupplier();

  String getHostAliasQuery();

  void prepareConnectProperties(
      final @NonNull Properties connectProperties,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec);

  EnumSet<FailoverRestriction> getFailoverRestrictions();

  String getServerVersionQuery();

  HostRole getHostRole(Connection connection) throws SQLException;

  @Nullable Pair<String, String> getHostId(Connection connection) throws SQLException;

  /**
   * Filters the provided list of hosts by the set of accessible AWS regions.
   * Implementations that do not support multi-region clusters should return the original list unchanged.
   * Dialects supporting multi-region clusters (e.g., Global Aurora) should restrict the host list
   * to nodes in accessible regions.
   *
   * @param hosts             the list of hosts to filter
   * @param accessibleRegions the set of accessible AWS regions (lowercase), or null for no restriction
   * @return the filtered list of hosts, or the original list if no filtering is applied
   */
  default List<HostSpec> filterAvailableHosts(
      @NonNull List<HostSpec> hosts, @Nullable Set<String> accessibleRegions) {
    // Default: no region filtering. Multi-region dialects (e.g., Global Aurora) override this.
    return hosts;
  }
}
