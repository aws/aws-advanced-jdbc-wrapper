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
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;
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

    // A host without a role cannot be matched against a host-selector strategy, so it is treated
    // as "strategy not accepted" (same handling as in AuroraInitialConnectionStrategyPlugin).
    final HostRole hostRole = hostSpec.getRole();
    if (hostRole == null
        || !ctx.pluginService().acceptsStrategy(hostRole, this.readerSelectorStrategy)) {
      throw new UnsupportedOperationException(
          Messages.get("ReadWriteSplittingPlugin.unsupportedHostSpecSelectorStrategy",
              new Object[] {this.readerSelectorStrategy}));
    }

    final Connection currentConnection = connectFunc.call();

    final HostListProviderService hostListProviderService = ctx.hostListProviderService();
    if (!isInitialConnection || hostListProviderService == null) {
      return currentConnection;
    }

    if (!this.verifyInitialConnectionRole) {
      return currentConnection;
    }

    final HostListProvider hostListProvider = hostListProviderService.getHostListProvider();
    final boolean staticHostList = hostListProvider instanceof StaticHostListProvider;

    final HostRole currentRole;
    try {
      currentRole = ctx.pluginService().getHostRole(currentConnection);
    } catch (final SQLException | RuntimeException e) {
      // A topology-backed provider supplies roles of its own, so a role that cannot be measured
      // means the connection is unusable for routing and the connection attempt fails, as before.
      // A static host list already carries a declared role, so keep using it rather than refusing
      // the connection: some databases reachable through a connection-string host list cannot
      // report their role at all.
      if (!staticHostList) {
        throw e;
      }
      LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.staticHostListRoleNotVerified",
          new Object[] {hostSpec.getHostAndPort(), hostSpec.getRole(), e.getMessage()}));
      return currentConnection;
    }

    if (currentRole == null || HostRole.UNKNOWN.equals(currentRole)) {
      if (staticHostList) {
        LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.staticHostListRoleNotVerified",
            new Object[] {hostSpec.getHostAndPort(), hostSpec.getRole(), currentRole}));
        return currentConnection;
      }
      final String message = Messages.get("ReadWriteSplittingPlugin.errorVerifyingInitialHostSpecRole");
      ctx.logAndThrow(message);
      // logAndThrow always throws; this statement is unreachable and only exists so the method has
      // no null return path.
      throw new ReadWriteSplittingSQLException(message);
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
    if (staticHostList) {
      correctStaticHostListRole(ctx, (StaticHostListProvider) hostListProvider, currentHost, currentRole);
    }
    return currentConnection;
  }

  /**
   * Applies a measured role back to a static host list.
   *
   * <p>A topology-backed provider re-reads roles from the database on every refresh, so correcting
   * the initial host spec is enough. A static provider never does: it derives roles from the
   * connection string alone, so without this the corrected role would be invisible to reader and
   * writer selection, both of which read {@code pluginService.getHosts()}.
   *
   * <p>A mismatch on a static list also means the connection string itself is wrong or has gone
   * stale (for example, the host listed first is no longer the writer), which the user has to fix,
   * so it is reported at {@code WARNING}.
   */
  private void correctStaticHostListRole(
      final RwSplitContext ctx,
      final StaticHostListProvider hostListProvider,
      final HostSpec host,
      final HostRole measuredRole)
      throws SQLException {

    LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.staticHostListRoleCorrected",
        new Object[] {host.getHostAndPort(), host.getRole(), measuredRole}));

    if (hostListProvider.updateHostRole(host.getHostAndPort(), measuredRole)) {
      // Republish the host list so that getHosts() reports the corrected role.
      ctx.pluginService().refreshHostList();
    }
  }
}
