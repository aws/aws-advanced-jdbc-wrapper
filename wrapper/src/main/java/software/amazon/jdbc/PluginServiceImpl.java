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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;

public class PluginServiceImpl implements PluginService, CanReleaseResources, HostListProviderService,
    PluginManagerService {

  private static final Logger LOGGER = Logger.getLogger(PluginServiceImpl.class.getName());

  protected final ConnectionPluginManager pluginManager;
  private final Properties props;
  private final String originalUrl;
  private final String driverProtocol;
  protected volatile HostListProvider hostListProvider;
  protected List<HostSpec> hosts = new ArrayList<>();
  protected Connection currentConnection;
  protected HostSpec currentHostSpec;
  private boolean isInTransaction;
  private boolean explicitReadOnly;

  public PluginServiceImpl(
      @NonNull ConnectionPluginManager pluginManager,
      @NonNull Properties props,
      @NonNull String originalUrl,
      String targetDriverProtocol) {
    this.pluginManager = pluginManager;
    this.props = props;
    this.originalUrl = originalUrl;
    this.driverProtocol = targetDriverProtocol;
  }

  @Override
  public Connection getCurrentConnection() {
    return this.currentConnection;
  }

  @Override
  public HostSpec getCurrentHostSpec() {
    if (this.currentHostSpec == null) {
      if (this.getHosts().isEmpty()) {
        throw new RuntimeException("Current host list is empty.");
      }
      this.currentHostSpec = this.getHosts().get(0);
    }
    return this.currentHostSpec;
  }

  @Override
  public void setCurrentConnection(
      final @NonNull Connection connection, final @NonNull HostSpec hostSpec) throws SQLException {

    setCurrentConnection(connection, hostSpec, null);
  }

  @Override
  public synchronized EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException {

    if (this.currentConnection == null) {
      // setting up an initial connection

      this.currentConnection = connection;
      this.currentHostSpec = hostSpec;

      EnumSet<NodeChangeOptions> changes = EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION);
      this.pluginManager.notifyConnectionChanged(changes, skipNotificationForThisPlugin);

      return changes;

    } else {
      // update an existing connection

      EnumSet<NodeChangeOptions> changes = compare(this.currentConnection, this.currentHostSpec,
          connection, hostSpec);

      if (!changes.isEmpty()) {

        final Connection oldConnection = this.currentConnection;

        this.currentConnection = connection;
        this.currentHostSpec = hostSpec;
        this.setInTransaction(false);

        EnumSet<OldConnectionSuggestedAction> pluginOpinions = this.pluginManager.notifyConnectionChanged(
            changes, skipNotificationForThisPlugin);

        boolean shouldCloseConnection =
            changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)
                && !oldConnection.isClosed()
                && !pluginOpinions.contains(OldConnectionSuggestedAction.PRESERVE);

        if (shouldCloseConnection) {
          try {
            oldConnection.close();
          } catch (SQLException e) {
            // Ignore any exception
          }
        }
      }
      return changes;
    }
  }

  protected EnumSet<NodeChangeOptions> compare(
      final @NonNull Connection connA,
      final @NonNull HostSpec hostSpecA,
      final @NonNull Connection connB,
      final @NonNull HostSpec hostSpecB) {

    EnumSet<NodeChangeOptions> changes = EnumSet.noneOf(NodeChangeOptions.class);

    if (connA != connB) {
      changes.add(NodeChangeOptions.CONNECTION_OBJECT_CHANGED);
    }

    if (!hostSpecA.getHost().equals(hostSpecB.getHost())
        || hostSpecA.getPort() != hostSpecB.getPort()) {
      changes.add(NodeChangeOptions.HOSTNAME);
    }

    if (hostSpecA.getRole() != hostSpecB.getRole()) {
      if (hostSpecB.getRole() == HostRole.WRITER) {
        changes.add(NodeChangeOptions.PROMOTED_TO_WRITER);
      } else if (hostSpecB.getRole() == HostRole.READER) {
        changes.add(NodeChangeOptions.PROMOTED_TO_READER);
      }
    }

    if (hostSpecA.getAvailability() != hostSpecB.getAvailability()) {
      if (hostSpecB.getAvailability() == HostAvailability.AVAILABLE) {
        changes.add(NodeChangeOptions.WENT_UP);
      } else if (hostSpecB.getAvailability() == HostAvailability.NOT_AVAILABLE) {
        changes.add(NodeChangeOptions.WENT_DOWN);
      }
    }

    if (!changes.isEmpty()) {
      changes.add(NodeChangeOptions.NODE_CHANGED);
    }

    return changes;
  }

  @Override
  public List<HostSpec> getHosts() {
    if (this.hosts.isEmpty()) {
      try {
        this.refreshHostList();
      } catch (SQLException e) {
        LOGGER.log(Level.FINEST, "Exception while getting a host list.", e);
      }
    }
    return this.hosts;
  }

  @Override
  public void setAvailability(final @NonNull Set<String> hostAliases, final @NonNull HostAvailability availability) {

    if (hostAliases.isEmpty()) {
      return;
    }

    List<HostSpec> hostsToChange = this.getHosts().stream()
        .filter((host) -> hostAliases.contains(host.asAlias())
            || host.getAliases().stream().anyMatch((hostAlias) -> hostAliases.contains(hostAlias)))
        .distinct()
        .collect(Collectors.toList());

    if (hostsToChange.isEmpty()) {
      LOGGER.log(Level.FINEST, String.format("Can't find any host by the following aliases: %s.", hostAliases));
      return;
    }

    Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();
    for (HostSpec host : hostsToChange) {
      HostAvailability currentAvailability = host.getAvailability();
      host.setAvailability(availability);
      if (currentAvailability != availability) {
        EnumSet<NodeChangeOptions> hostChanges;
        if (availability == HostAvailability.AVAILABLE) {
          hostChanges = EnumSet.of(NodeChangeOptions.WENT_UP, NodeChangeOptions.NODE_CHANGED);
        } else {
          hostChanges = EnumSet.of(NodeChangeOptions.WENT_DOWN, NodeChangeOptions.NODE_CHANGED);
        }
        changes.put(host.getUrl(), hostChanges);
      }
    }

    if (!changes.isEmpty()) {
      this.pluginManager.notifyNodeListChanged(changes);
    }
  }

  @Override
  public boolean isExplicitReadOnly() {
    return this.explicitReadOnly;
  }

  @Override
  public boolean isReadOnly() {
    return isExplicitReadOnly() || (this.currentHostSpec != null && this.currentHostSpec.getRole() != HostRole.WRITER);
  }

  @Override
  public boolean isInTransaction() {
    return this.isInTransaction;
  }

  @Override
  public void setReadOnly(final boolean readOnly) {
    this.explicitReadOnly = readOnly;
  }

  @Override
  public void setInTransaction(boolean inTransaction) {
    this.isInTransaction = inTransaction;
  }

  @Override
  public HostListProvider getHostListProvider() {
    if (this.hostListProvider == null) {
      synchronized (this) {
        if (this.hostListProvider == null) {
          this.hostListProvider = new ConnectionStringHostListProvider(this.props, this.originalUrl);
          this.currentHostSpec = this.getCurrentHostSpec();
        }
      }
    }
    return this.hostListProvider;
  }

  @Override
  public void refreshHostList() throws SQLException {
    setNodeList(this.hosts, this.getHostListProvider().refresh());
  }

  @Override
  public void forceRefreshHostList() throws SQLException {
    setNodeList(this.hosts, this.getHostListProvider().forceRefresh());
  }

  void setNodeList(@Nullable final List<HostSpec> oldHosts,
      @Nullable final List<HostSpec> newHosts) {

    Map<String, HostSpec> oldHostMap = oldHosts == null
        ? new HashMap<>()
        : oldHosts.stream().collect(Collectors.toMap(HostSpec::getUrl, (value) -> value));

    Map<String, HostSpec> newHostMap = newHosts == null
        ? new HashMap<>()
        : newHosts.stream().collect(Collectors.toMap(HostSpec::getUrl, (value) -> value));

    Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();

    for (Entry<String, HostSpec> entry : oldHostMap.entrySet()) {
      HostSpec correspondingNewHost = newHostMap.get(entry.getKey());
      if (correspondingNewHost == null) {
        // host deleted
        changes.put(entry.getKey(), EnumSet.of(NodeChangeOptions.NODE_DELETED));
      } else {
        // host maybe changed
        EnumSet<NodeChangeOptions> hostChanges = compare(null, entry.getValue(), null,
            correspondingNewHost);
        if (!hostChanges.isEmpty()) {
          changes.put(entry.getKey(), hostChanges);
        }
      }
    }

    for (Entry<String, HostSpec> entry : newHostMap.entrySet()) {
      if (!oldHostMap.containsKey(entry.getKey())) {
        // host added
        changes.put(entry.getKey(), EnumSet.of(NodeChangeOptions.NODE_ADDED));
      }
    }

    if (!changes.isEmpty()) {
      this.hosts = newHosts != null ? newHosts : new ArrayList<>();
      this.pluginManager.notifyNodeListChanged(changes);
    }
  }

  @Override
  public boolean isStaticHostListProvider() {
    return this.getHostListProvider() instanceof StaticHostListProvider;
  }

  @Override
  public void setHostListProvider(HostListProvider hostListProvider) {
    this.hostListProvider = hostListProvider;
  }

  @Override
  public Connection connect(HostSpec hostSpec, Properties props) throws SQLException {
    return this.pluginManager.connect(this.driverProtocol, hostSpec, props, this.currentConnection == null);
  }

  @Override
  public void releaseResources() {
    LOGGER.log(Level.FINE, "releasing resources");

    try {
      if (this.currentConnection != null && !this.currentConnection.isClosed()) {
        this.currentConnection.close();
      }
    } catch (SQLException e) {
      // Ignore an exception
    }

    if (this.hostListProvider != null && this.hostListProvider instanceof CanReleaseResources) {
      CanReleaseResources canReleaseResourcesObject = (CanReleaseResources) this.hostListProvider;
      canReleaseResourcesObject.releaseResources();
    }
  }
}
