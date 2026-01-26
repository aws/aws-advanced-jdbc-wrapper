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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.CacheItem;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.Utils;

public class ReadWriteSplittingPlugin extends AbstractReadWriteSplittingPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingPlugin.class.getName());

  private final String readerSelectorStrategy;
  protected List<HostSpec> hosts;

  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "readerHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host.");

  static {
    PropertyDefinition.registerPluginProperties(ReadWriteSplittingPlugin.class);
  }

  public ReadWriteSplittingPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    super(pluginService, properties);
    this.readerSelectorStrategy = READER_HOST_SELECTOR_STRATEGY.getString(properties);
  }

  /**
   * For testing purposes only.
   */
  ReadWriteSplittingPlugin(
      final PluginService pluginService,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final Connection writerConnection,
      final Connection readerConnection) {
    this(pluginService, properties);
    this.hostListProviderService = hostListProviderService;
    this.writerConnection = writerConnection;
    this.readerCacheItem = new CacheItem<>(readerConnection, this.getKeepAliveTimeout(false));
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    if (!this.pluginService.acceptsStrategy(hostSpec.getRole(), this.readerSelectorStrategy)) {
      throw new UnsupportedOperationException(
          Messages.get("ReadWriteSplittingPlugin.unsupportedHostSpecSelectorStrategy",
              new Object[] {this.readerSelectorStrategy}));
    }

    final Connection currentConnection = connectFunc.call();
    if (!isInitialConnection || this.hostListProviderService.isStaticHostListProvider()) {
      return currentConnection;
    }

    final HostRole currentRole = this.pluginService.getHostRole(currentConnection);
    if (currentRole == null || HostRole.UNKNOWN.equals(currentRole)) {
      logAndThrowException(
          Messages.get("ReadWriteSplittingPlugin.errorVerifyingInitialHostSpecRole"));
      return null;
    }

    final HostSpec currentHost = this.pluginService.getInitialConnectionHostSpec();
    if (currentRole.equals(currentHost.getRole())) {
      return currentConnection;
    }

    final HostSpec updatedRoleHostSpec = new HostSpec(currentHost, currentRole);
    this.hostListProviderService.setInitialConnectionHostSpec(updatedRoleHostSpec);
    return currentConnection;
  }

  @Override
  protected boolean isWriter(final @NonNull HostSpec hostSpec) {
    return HostRole.WRITER.equals(hostSpec.getRole());
  }

  @Override
  protected boolean isReader(final @NonNull HostSpec hostSpec) {
    return HostRole.READER.equals(hostSpec.getRole());
  }

  @Override
  protected void refreshAndStoreTopology(Connection currentConnection) throws SQLException {
    if (this.isConnectionUsable(currentConnection)) {
      try {
        this.pluginService.refreshHostList();
      } catch (final SQLException e) {
        // ignore
      }
    }

    this.hosts = this.pluginService.getHosts();
    if (Utils.isNullOrEmpty(this.hosts)) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.emptyHostList"));
    }
    this.writerHostSpec = getWriterHost(this.hosts);
  }

  @Override
  protected void initializeWriterConnection() throws SQLException {
    final Connection conn = this.pluginService.connect(writerHostSpec, this.properties, this);
    this.isWriterConnFromInternalPool = Boolean.TRUE.equals(this.pluginService.isPooledConnection());
    setWriterConnection(conn, writerHostSpec);
    switchCurrentConnectionTo(this.writerConnection, writerHostSpec);
  }

  @Override
  protected void initializeReaderConnection() throws SQLException {
    if (this.hosts.size() == 1) {
      if (!isConnectionUsable(this.writerConnection)) {
        initializeWriterConnection();
      }
      LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.noReadersFound",
          new Object[] {writerHostSpec.getHostAndPort()}));
    } else {
      openNewReaderConnection();
      LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
          new Object[] {this.readerHostSpec.getHostAndPort()}));
    }
  }

  @Override
  protected void closeReaderIfNecessary() {
    if (this.readerHostSpec != null && !Utils.containsHostAndPort(hosts, this.readerHostSpec.getHostAndPort())) {
      // The old reader cannot be used anymore because it is no longer in the list of allowed hosts.
      LOGGER.finest(
          Messages.get(
              "ReadWriteSplittingPlugin.previousReaderNotAllowed",
              new Object[] {this.readerHostSpec, LogUtils.logTopology(hosts, "")}));
      closeReaderConnectionIfIdle();
    }
  }

  private HostSpec getWriterHost(final @NonNull List<HostSpec> hosts) throws SQLException {
    HostSpec writerHost = Utils.getWriter(hosts);
    if (writerHost == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noWriterFound"));
    }

    return writerHost;
  }

  private void openNewReaderConnection() throws SQLException {
    Connection conn = null;
    HostSpec readerHost = null;

    int connAttempts = this.pluginService.getHosts().size() * 2;
    for (int i = 0; i < connAttempts; i++) {
      HostSpec hostSpec = this.pluginService.getHostSpecByStrategy(HostRole.READER, this.readerSelectorStrategy);
      try {
        conn = this.pluginService.connect(hostSpec, this.properties, this);
        this.isReaderConnFromInternalPool = Boolean.TRUE.equals(this.pluginService.isPooledConnection());
        readerHost = hostSpec;
        break;
      } catch (final SQLException e) {
        if (LOGGER.isLoggable(Level.WARNING)) {
          LOGGER.log(Level.WARNING,
              Messages.get(
                  "ReadWriteSplittingPlugin.failedToConnectToReader",
                  new Object[]{
                      hostSpec.getHostAndPort()}),
              e);
        }
      }
    }

    if (conn == null || readerHost == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noReadersAvailable"),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
      return;
    }

    final HostSpec finalReaderHost = readerHost;
    LOGGER.finest(
        () -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
            new Object[] {finalReaderHost.getHostAndPort()}));
    setReaderConnection(conn, readerHost);
    switchCurrentConnectionTo(this.readerCacheItem.get(), this.readerHostSpec);
  }

  @Override
  protected boolean shouldUpdateReaderConnection(Connection currentConnection, HostSpec currentHost) {
    return isReader(currentHost);
  }

  @Override
  protected boolean shouldUpdateWriterConnection(Connection currentConnection, HostSpec currentHost) {
    return isWriter(currentHost);
  }
}
