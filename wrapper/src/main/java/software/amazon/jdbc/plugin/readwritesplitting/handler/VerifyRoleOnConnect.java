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

package software.amazon.jdbc.plugin.readwritesplitting.handler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.Messages;

/**
 * Topology {@link InitialConnectionHandler}: validates the reader host-selector strategy and, for
 * the initial connection, verifies and corrects the connection role. Ports the legacy
 * {@code ReadWriteSplittingPlugin.connect}.
 */
public class VerifyRoleOnConnect implements InitialConnectionHandler {

  private static final Logger LOGGER = Logger.getLogger(VerifyRoleOnConnect.class.getName());

  private final String readerSelectorStrategy;
  private final boolean verifyInitialConnectionRole;

  public VerifyRoleOnConnect(final String readerSelectorStrategy, final boolean verifyInitialConnectionRole) {
    this.readerSelectorStrategy = readerSelectorStrategy;
    this.verifyInitialConnectionRole = verifyInitialConnectionRole;
  }

  @Override
  public Connection onConnect(
      final RwSplitContext ctx,
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    if (!ctx.pluginService().acceptsStrategy(hostSpec.getRole(), this.readerSelectorStrategy)) {
      throw new UnsupportedOperationException(
          Messages.get("ReadWriteSplittingPlugin.unsupportedHostSpecSelectorStrategy",
              new Object[] {this.readerSelectorStrategy}));
    }

    final Connection currentConnection = connectFunc.call();

    final HostListProviderService hostListProviderService = ctx.hostListProviderService();
    if (!isInitialConnection
        || hostListProviderService == null
        || hostListProviderService.isStaticHostListProvider()) {
      return currentConnection;
    }

    if (!this.verifyInitialConnectionRole) {
      return currentConnection;
    }

    final HostRole currentRole = ctx.pluginService().getHostRole(currentConnection);
    if (currentRole == null || HostRole.UNKNOWN.equals(currentRole)) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.errorVerifyingInitialHostSpecRole"));
      return null;
    }

    final HostSpec currentHost = ctx.pluginService().getInitialConnectionHostSpec();
    if (currentRole.equals(currentHost.getRole())) {
      LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.initialConnectionRoleCheckNoUpdate",
          new Object[] {currentHost.getHostAndPort(), currentHost.getRole(), currentRole}));
      return currentConnection;
    }

    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.initialConnectionRoleCheckUpdated",
        new Object[] {currentHost.getHostAndPort(), currentHost.getRole(), currentRole}));
    final HostSpec updatedRoleHostSpec = new HostSpec(currentHost, currentRole);
    hostListProviderService.setInitialConnectionHostSpec(updatedRoleHostSpec);
    return currentConnection;
  }
}
