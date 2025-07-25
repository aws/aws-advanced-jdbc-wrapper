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

package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.storage.StorageService;

// Reject an attempt to open a new connection.
public class RejectConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(RejectConnectRouting.class.getName());

  public RejectConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public Connection apply(ConnectionPlugin plugin, HostSpec hostSpec, Properties props, boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc, StorageService storageService,
      PluginService pluginService) throws SQLException {

    LOGGER.finest(() -> Messages.get("bgd.inProgressCantConnect"));
    throw new SQLException(Messages.get("bgd.inProgressCantConnect"));
  }
}
