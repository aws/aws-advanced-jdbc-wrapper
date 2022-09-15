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

package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.util.Messages;

public class ReadWriteSplittingPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Collections.singletonList("*")));
  private final PluginService pluginService;
  private final Properties properties;
  private Connection writerConnection;
  private Connection readerConnection;

  ReadWriteSplittingPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.properties = properties;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final Connection currentConnection = connectFunc.call();
    if (isWriter(this.pluginService.getCurrentHostSpec())) {
      setWriterConnection(currentConnection);
    } else {
      setReaderConnection(currentConnection);
    }
    return currentConnection;
  }

  /**
   * Checks if the given host index points to the primary host.
   *
   * @param hostSpec The current host.
   * @return true if so
   */
  private boolean isWriter(final @NonNull HostSpec hostSpec) {
    return HostRole.WRITER.equals(hostSpec.getRole());
  }

  private void setWriterConnection(Connection conn) {
    this.writerConnection = conn;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setWriterConnection",
            new Object[] {
                this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  private void setReaderConnection(Connection conn) {
    this.readerConnection = conn;
    LOGGER.finest(
        () -> Messages.get(
            "ReadWriteSplittingPlugin.setReaderConnection",
            new Object[] {
                this.pluginService.getCurrentHostSpec().getUrl()}));
  }

  @Override
  public void releaseResources() {
    closeAllConnections();
  }

  private void closeAllConnections() {
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.closingInternalConnections"));
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    closeInternalConnection(this.readerConnection, currentConnection);
    closeInternalConnection(this.writerConnection, currentConnection);

    if (this.readerConnection != currentConnection) {
      this.readerConnection = null;
    }

    if (this.writerConnection != currentConnection) {
      this.readerConnection = null;
    }
  }

  private void closeInternalConnection(Connection internalConnection, Connection currentConnection) {
    try {
      if (internalConnection != null && internalConnection != currentConnection && !internalConnection.isClosed()) {
        internalConnection.close();
      }
    } catch (SQLException e) {
      // ignore
    }
  }
}
