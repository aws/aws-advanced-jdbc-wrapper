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
import software.amazon.jdbc.util.storage.StorageService;

public abstract class BaseConnectRouting extends BaseRouting implements ConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(BaseConnectRouting.class.getName());

  protected final String hostAndPort; // if value is provided then host is mandatory and port is optional.
  protected final BlueGreenRole role;

  public BaseConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    this.hostAndPort = hostAndPort == null ? null : hostAndPort.toLowerCase();
    this.role = role;
  }

  @Override
  public boolean isMatch(HostSpec hostSpec, BlueGreenRole hostRole) {
    return (this.hostAndPort == null || this.hostAndPort.equals(
                hostSpec == null ? null : hostSpec.getHostAndPort().toLowerCase()))
        && (this.role == null || this.role.equals(hostRole));
  }

  @Override
  public abstract Connection apply(
      ConnectionPlugin plugin,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc,
      StorageService storageService,
      PluginService pluginService)
      throws SQLException;

  @Override
  public String toString() {
    return String.format("%s [%s, %s]",
        super.toString(),
        this.hostAndPort == null ? "<null>" : this.hostAndPort,
        this.role == null ? "<null>" : this.role.toString());
  }
}
