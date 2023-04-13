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
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.exceptions.ExceptionHandler;

/**
 * Interface for retrieving the current active {@link Connection} and its {@link HostSpec}.
 */
public interface PluginService extends ExceptionHandler {

  Connection getCurrentConnection();

  HostSpec getCurrentHostSpec();

  void setCurrentConnection(final @NonNull Connection connection, final @NonNull HostSpec hostSpec)
      throws SQLException;

  EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException;

  List<HostSpec> getHosts();

  HostSpec getInitialConnectionHostSpec();

  boolean acceptsStrategy(HostRole role, String strategy) throws SQLException;

  HostSpec getHostSpecByStrategy(HostRole role, String strategy) throws SQLException;

  HostRole getHostRole(Connection conn) throws SQLException;

  void setAvailability(Set<String> hostAliases, HostAvailability availability);

  boolean isExplicitReadOnly();

  boolean isReadOnly();

  boolean isInTransaction();

  HostListProvider getHostListProvider();

  void refreshHostList() throws SQLException;

  void refreshHostList(Connection connection) throws SQLException;

  void forceRefreshHostList() throws SQLException;

  void forceRefreshHostList(Connection connection) throws SQLException;

  Connection connect(HostSpec hostSpec, Properties props) throws SQLException;

  Connection forceConnect(HostSpec hostSpec, Properties props) throws SQLException;
}
