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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;
import software.amazon.jdbc.util.ExpiringCache;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class PluginServiceImpl implements PluginService, CanReleaseResources,
    HostListProviderService, PluginManagerService {

  private static final Logger LOGGER = Logger.getLogger(PluginServiceImpl.class.getName());
  private static final int DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min

  protected static final ExpiringCache<String, HostAvailability> hostAvailabilityExpiringCache = new ExpiringCache<>(
      DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_MS);
  protected final ConnectionPluginManager pluginManager;
  private final Properties props;
  private final String originalUrl;
  private final String driverProtocol;
  protected volatile HostListProvider hostListProvider;
  protected List<HostSpec> hosts = new ArrayList<>();
  protected Connection currentConnection;
  protected HostSpec currentHostSpec;
  protected HostSpec initialConnectionHostSpec;
  private boolean isInTransaction;
  private boolean explicitReadOnly;
  private final ExceptionManager exceptionManager;

  public PluginServiceImpl(
      @NonNull final ConnectionPluginManager pluginManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      final String targetDriverProtocol) {
    this(pluginManager, new ExceptionManager(), props, originalUrl, targetDriverProtocol);
  }

  public PluginServiceImpl(
      @NonNull final ConnectionPluginManager pluginManager,
      @NonNull final ExceptionManager exceptionManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      final String targetDriverProtocol) {
    this.pluginManager = pluginManager;
    this.props = props;
    this.originalUrl = originalUrl;
    this.driverProtocol = targetDriverProtocol;
    this.exceptionManager = exceptionManager;
  }

  @Override
  public Connection getCurrentConnection() {
    return this.currentConnection;
  }

  @Override
  public HostSpec getCurrentHostSpec() {
    if (this.currentHostSpec == null) {
      this.currentHostSpec = this.initialConnectionHostSpec;

      if (this.currentHostSpec == null) {
        if (this.getHosts().isEmpty()) {
          throw new RuntimeException(Messages.get("PluginServiceImpl.hostListEmpty"));
        }
        this.currentHostSpec = this.getWriter(this.getHosts());
        if (this.currentHostSpec == null) {
          this.currentHostSpec = this.getHosts().get(0);
        }
      }
      if (this.currentHostSpec == null) {
        throw new RuntimeException("Current host is undefined.");
      }
      LOGGER.finest(() -> "Set current host to " + this.currentHostSpec);
    }
    return this.currentHostSpec;
  }

  public void setInitialConnectionHostSpec(final @NonNull HostSpec initialConnectionHostSpec) {
    this.initialConnectionHostSpec = initialConnectionHostSpec;
  }

  public HostSpec getInitialConnectionHostSpec() {
    return this.initialConnectionHostSpec;
  }

  private HostSpec getWriter(final @NonNull List<HostSpec> hosts) {
    for (final HostSpec hostSpec : hosts) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        return hostSpec;
      }
    }
    return null;
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
      @Nullable final ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException {

    if (this.currentConnection == null) {
      // setting up an initial connection

      this.currentConnection = connection;
      this.currentHostSpec = hostSpec;

      final EnumSet<NodeChangeOptions> changes = EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION);
      this.pluginManager.notifyConnectionChanged(changes, skipNotificationForThisPlugin);

      return changes;

    } else {
      // update an existing connection

      final EnumSet<NodeChangeOptions> changes = compare(this.currentConnection, this.currentHostSpec,
          connection, hostSpec);

      if (!changes.isEmpty()) {

        final Connection oldConnection = this.currentConnection;

        this.currentConnection = connection;
        this.currentHostSpec = hostSpec;
        this.setInTransaction(false);

        final EnumSet<OldConnectionSuggestedAction> pluginOpinions = this.pluginManager.notifyConnectionChanged(
            changes, skipNotificationForThisPlugin);

        final boolean shouldCloseConnection =
            changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)
                && !oldConnection.isClosed()
                && !pluginOpinions.contains(OldConnectionSuggestedAction.PRESERVE);

        if (shouldCloseConnection) {
          try {
            oldConnection.close();
          } catch (final SQLException e) {
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

    final EnumSet<NodeChangeOptions> changes = EnumSet.noneOf(NodeChangeOptions.class);

    if (connA != connB) {
      changes.add(NodeChangeOptions.CONNECTION_OBJECT_CHANGED);
    }

    changes.addAll(compare(hostSpecA, hostSpecB));
    return changes;
  }

  protected EnumSet<NodeChangeOptions> compare(
      final @NonNull HostSpec hostSpecA,
      final @NonNull HostSpec hostSpecB) {

    final EnumSet<NodeChangeOptions> changes = EnumSet.noneOf(NodeChangeOptions.class);

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
    return this.hosts;
  }

  @Override
  public void setAvailability(final @NonNull Set<String> hostAliases, final @NonNull HostAvailability availability) {

    if (hostAliases.isEmpty()) {
      return;
    }

    final List<HostSpec> hostsToChange = this.getHosts().stream()
        .filter((host) -> hostAliases.contains(host.asAlias())
            || host.getAliases().stream().anyMatch(hostAliases::contains))
        .distinct()
        .collect(Collectors.toList());

    if (hostsToChange.isEmpty()) {
      LOGGER.finest(() -> Messages.get("PluginServiceImpl.hostsChangelistEmpty"));
      return;
    }

    final Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();
    for (final HostSpec host : hostsToChange) {
      final HostAvailability currentAvailability = host.getAvailability();
      host.setAvailability(availability);
      hostAvailabilityExpiringCache.put(host.getUrl(), availability);
      if (currentAvailability != availability) {
        final EnumSet<NodeChangeOptions> hostChanges;
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
  public void setInTransaction(final boolean inTransaction) {
    this.isInTransaction = inTransaction;
  }

  @Override
  public HostListProvider getHostListProvider() {
    return this.hostListProvider;
  }

  @Override
  public void refreshHostList() throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().refresh();
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.hosts, updatedHostList);
    }
  }

  @Override
  public void refreshHostList(final Connection connection) throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().refresh(connection);
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.hosts, updatedHostList);
    }
  }

  @Override
  public void forceRefreshHostList() throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().forceRefresh();
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.hosts, updatedHostList);
    }
  }

  @Override
  public void forceRefreshHostList(final Connection connection) throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().forceRefresh(connection);
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.hosts, updatedHostList);
    }
  }

  void setNodeList(@Nullable final List<HostSpec> oldHosts,
                   @Nullable final List<HostSpec> newHosts) {

    final Map<String, HostSpec> oldHostMap = oldHosts == null
        ? new HashMap<>()
        : oldHosts.stream().collect(Collectors.toMap(HostSpec::getUrl, (value) -> value));

    final Map<String, HostSpec> newHostMap = newHosts == null
        ? new HashMap<>()
        : newHosts.stream().collect(Collectors.toMap(HostSpec::getUrl, (value) -> value));

    final Map<String, EnumSet<NodeChangeOptions>> changes = new HashMap<>();

    for (final Entry<String, HostSpec> entry : oldHostMap.entrySet()) {
      final HostSpec correspondingNewHost = newHostMap.get(entry.getKey());
      if (correspondingNewHost == null) {
        // host deleted
        changes.put(entry.getKey(), EnumSet.of(NodeChangeOptions.NODE_DELETED));
      } else {
        // host maybe changed
        final EnumSet<NodeChangeOptions> hostChanges = compare(entry.getValue(), correspondingNewHost);
        if (!hostChanges.isEmpty()) {
          changes.put(entry.getKey(), hostChanges);
        }
      }
    }

    for (final Entry<String, HostSpec> entry : newHostMap.entrySet()) {
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
  public void setHostListProvider(final HostListProvider hostListProvider) {
    this.hostListProvider = hostListProvider;
  }

  @Override
  public Connection connect(final HostSpec hostSpec, final Properties props) throws SQLException {
    return this.pluginManager.connect(
        this.driverProtocol,
        hostSpec,
        props,
        this.currentConnection == null);
  }

  private void updateHostAvailability(final List<HostSpec> hosts) {
    for (HostSpec host : hosts) {
      final HostAvailability availability = hostAvailabilityExpiringCache.get(host.getUrl());
      if (availability != null) {
        host.setAvailability(availability);
      }
    }
  }

  @Override
  public void releaseResources() {
    LOGGER.fine(() -> Messages.get("PluginServiceImpl.releaseResources"));

    try {
      if (this.currentConnection != null && !this.currentConnection.isClosed()) {
        this.currentConnection.close();
      }
    } catch (final SQLException e) {
      // Ignore an exception
    }

    if (this.hostListProvider != null && this.hostListProvider instanceof CanReleaseResources) {
      final CanReleaseResources canReleaseResourcesObject = (CanReleaseResources) this.hostListProvider;
      canReleaseResourcesObject.releaseResources();
    }
  }

  @Override
  public boolean isNetworkException(Throwable throwable) {
    return this.exceptionManager.isNetworkException(this.driverProtocol, throwable);
  }

  @Override
  public boolean isNetworkException(String sqlState) {
    return this.exceptionManager.isNetworkException(this.driverProtocol, sqlState);
  }

  @Override
  public boolean isLoginException(Throwable throwable) {
    return this.exceptionManager.isLoginException(this.driverProtocol, throwable);

  }

  @Override
  public boolean isLoginException(String sqlState) {
    return this.exceptionManager.isLoginException(this.driverProtocol, sqlState);
  }

  public TelemetryFactory getTelemetryFactory() {
    return this.pluginManager.getTelemetryFactory();
  }

}
