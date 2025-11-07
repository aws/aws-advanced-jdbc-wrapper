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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.HostListProviderSupplier;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategyFactory;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.CacheMap;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * A {@link PluginService} containing some methods that are not intended to be called. This class is intended to be used
 * by monitors, which require a {@link PluginService}, but are not expected to need or use some of the methods defined
 * by the {@link PluginService} interface. The methods that are not expected to be called will log a warning or throw an
 * {@link UnsupportedOperationException} when called.
 */
public class PartialPluginService implements PluginService, CanReleaseResources, HostListProviderService,
    PluginManagerService {

  private static final Logger LOGGER = Logger.getLogger(PluginServiceImpl.class.getName());
  protected static final long DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO = TimeUnit.MINUTES.toNanos(5);
  protected static final int DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS = 5000;

  protected static final CacheMap<String, HostAvailability> hostAvailabilityExpiringCache = new CacheMap<>();
  protected final FullServicesContainer servicesContainer;
  protected final ConnectionPluginManager pluginManager;
  protected final Properties props;
  protected volatile HostListProvider hostListProvider;
  protected List<HostSpec> allHosts = new ArrayList<>();
  protected HostSpec currentHostSpec;
  protected HostSpec initialConnectionHostSpec;
  protected boolean isInTransaction;
  protected final ExceptionManager exceptionManager;
  protected final @Nullable ExceptionHandler exceptionHandler;
  protected final String originalUrl;
  protected final String driverProtocol;
  protected TargetDriverDialect targetDriverDialect;
  protected Dialect dbDialect;
  protected @Nullable final ConfigurationProfile configurationProfile;
  protected final ConnectionProviderManager connectionProviderManager;

  public PartialPluginService(
      @NonNull final FullServicesContainer servicesContainer,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      @NonNull final String targetDriverProtocol,
      @NonNull final TargetDriverDialect targetDriverDialect,
      @NonNull final Dialect dbDialect) {
    this(
        servicesContainer,
        new ExceptionManager(),
        props,
        originalUrl,
        targetDriverProtocol,
        targetDriverDialect,
        dbDialect,
        null);
  }

  public PartialPluginService(
      @NonNull final FullServicesContainer servicesContainer,
      @NonNull final ExceptionManager exceptionManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      @NonNull final String targetDriverProtocol,
      @NonNull final TargetDriverDialect targetDriverDialect,
      @NonNull final Dialect dbDialect,
      @Nullable final ConfigurationProfile configurationProfile) {
    this.servicesContainer = servicesContainer;
    this.servicesContainer.setHostListProviderService(this);
    this.servicesContainer.setPluginService(this);
    this.servicesContainer.setPluginManagerService(this);

    this.pluginManager = servicesContainer.getConnectionPluginManager();
    this.props = props;
    this.originalUrl = originalUrl;
    this.driverProtocol = targetDriverProtocol;
    this.targetDriverDialect = targetDriverDialect;
    this.dbDialect = dbDialect;
    this.configurationProfile = configurationProfile;
    this.exceptionManager = exceptionManager;

    this.connectionProviderManager = new ConnectionProviderManager(
        this.pluginManager.getDefaultConnProvider(),
        this.pluginManager.getEffectiveConnProvider());

    this.exceptionHandler = this.configurationProfile != null && this.configurationProfile.getExceptionHandler() != null
        ? this.configurationProfile.getExceptionHandler()
        : null;

    HostListProviderSupplier supplier = this.dbDialect.getHostListProviderSupplier();
    this.hostListProvider = supplier.getProvider(this.props, this.originalUrl, this.servicesContainer);
  }

  @Override
  public Connection getCurrentConnection() {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getCurrentConnection"}));
  }

  @Override
  public HostSpec getCurrentHostSpec() {
    if (this.currentHostSpec == null) {
      this.currentHostSpec = this.initialConnectionHostSpec;

      if (this.currentHostSpec == null) {
        if (this.getAllHosts().isEmpty()) {
          throw new RuntimeException(Messages.get("PluginServiceImpl.hostListEmpty"));
        }

        this.currentHostSpec = Utils.getWriter(this.getAllHosts());
        final List<HostSpec> allowedHosts = this.getHosts();
        if (!Utils.containsHostAndPort(allowedHosts, this.currentHostSpec.getHostAndPort())) {
          throw new RuntimeException(
              Messages.get("PluginServiceImpl.currentHostNotAllowed",
                  new Object[] {
                      currentHostSpec == null ? "<null>" : currentHostSpec.getHostAndPort(),
                      LogUtils.logTopology(allowedHosts, "")})
          );
        }

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

  @Override
  public HostSpec getInitialConnectionHostSpec() {
    return this.initialConnectionHostSpec;
  }

  @Override
  public String getOriginalUrl() {
    return this.originalUrl;
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"acceptsStrategy"}));
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getHostSpecByStrategy"}));
  }

  @Override
  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy) throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getHostSpecByStrategy"}));
  }

  @Override
  public HostRole getHostRole(Connection conn) throws SQLException {
    return this.hostListProvider.getHostRole(conn);
  }

  @Override
  public ConnectionProvider getDefaultConnectionProvider() {
    return this.connectionProviderManager.getDefaultProvider();
  }

  @Deprecated
  public boolean isPooledConnectionProvider(HostSpec host, Properties props) {
    final ConnectionProvider connectionProvider =
        this.connectionProviderManager.getConnectionProvider(this.driverProtocol, host, props);
    return (connectionProvider instanceof PooledConnectionProvider);
  }

  @Override
  public String getDriverProtocol() {
    return this.driverProtocol;
  }

  @Override
  public void setCurrentConnection(
      final @NonNull Connection connection, final @NonNull HostSpec hostSpec) throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"setCurrentConnection"}));
  }

  @Override
  public EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable final ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"setCurrentConnection"}));
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
  public List<HostSpec> getAllHosts() {
    return this.allHosts;
  }

  @Override
  public List<HostSpec> getHosts() {
    AllowedAndBlockedHosts hostPermissions = this.servicesContainer.getStorageService().get(
        AllowedAndBlockedHosts.class, this.initialConnectionHostSpec.getUrl());
    if (hostPermissions == null) {
      return this.allHosts;
    }

    List<HostSpec> hosts = this.allHosts;
    Set<String> allowedHostIds = hostPermissions.getAllowedHostIds();
    Set<String> blockedHostIds = hostPermissions.getBlockedHostIds();

    if (!Utils.isNullOrEmpty(allowedHostIds)) {
      hosts = hosts.stream()
          .filter((hostSpec -> allowedHostIds.contains(hostSpec.getHostId())))
          .collect(Collectors.toList());
    }

    if (!Utils.isNullOrEmpty(blockedHostIds)) {
      hosts = hosts.stream()
          .filter((hostSpec -> !blockedHostIds.contains(hostSpec.getHostId())))
          .collect(Collectors.toList());
    }

    return hosts;
  }

  @Override
  public void setAvailability(final @NonNull Set<String> hostAliases, final @NonNull HostAvailability availability) {

    if (hostAliases.isEmpty()) {
      return;
    }

    final List<HostSpec> hostsToChange = this.getAllHosts().stream()
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
      hostAvailabilityExpiringCache.put(host.getUrl(), availability,
          DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
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
  public boolean isInTransaction() {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"isInTransaction"}));
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
    if (!Objects.equals(updatedHostList, this.allHosts)) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.allHosts, updatedHostList);
    }
  }

  @Override
  public void refreshHostList(final Connection connection) throws SQLException {
    // TODO: refresh
    final List<HostSpec> updatedHostList = this.getHostListProvider().refresh();
    if (!Objects.equals(updatedHostList, this.allHosts)) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.allHosts, updatedHostList);
    }
  }

  @Override
  public void forceRefreshHostList() throws SQLException {
    this.forceRefreshHostList(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS);
  }

  @Override
  public boolean forceRefreshHostList(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException {

    final HostListProvider hostListProvider = this.getHostListProvider();
    try {
      final List<HostSpec> updatedHostList = hostListProvider.forceRefresh(shouldVerifyWriter, timeoutMs);
      if (updatedHostList != null) {
        updateHostAvailability(updatedHostList);
        setNodeList(this.allHosts, updatedHostList);
        return true;
      }
    } catch (TimeoutException ex) {
      // do nothing.
      LOGGER.finest(Messages.get("PluginServiceImpl.forceRefreshTimeout", new Object[] {timeoutMs}));
    }
    return false;
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

    for (final Map.Entry<String, HostSpec> entry : oldHostMap.entrySet()) {
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

    for (final Map.Entry<String, HostSpec> entry : newHostMap.entrySet()) {
      if (!oldHostMap.containsKey(entry.getKey())) {
        // host added
        changes.put(entry.getKey(), EnumSet.of(NodeChangeOptions.NODE_ADDED));
      }
    }

    if (!changes.isEmpty()) {
      this.allHosts = newHosts != null ? newHosts : new ArrayList<>();
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
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"connect"}));
  }

  @Override
  public Connection connect(
      final HostSpec hostSpec,
      final Properties props,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"connect"}));
  }

  @Override
  public Connection forceConnect(
      final HostSpec hostSpec,
      final Properties props)
      throws SQLException {
    return this.forceConnect(hostSpec, props, null);
  }

  @Override
  public Connection forceConnect(
      final HostSpec hostSpec,
      final Properties props,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException {
    return this.pluginManager.forceConnect(
        this.driverProtocol, hostSpec, props, true, pluginToSkip);
  }

  private void updateHostAvailability(final List<HostSpec> hosts) {
    for (final HostSpec host : hosts) {
      final HostAvailability availability = hostAvailabilityExpiringCache.get(host.getUrl());
      if (availability != null) {
        host.setAvailability(availability);
      }
    }
  }

  @Override
  public void releaseResources() {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"releaseResources"}));
  }

  @Override
  public boolean isNetworkException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isNetworkException(throwable, targetDriverDialect);
    }
    return this.exceptionManager.isNetworkException(this.dbDialect, throwable, targetDriverDialect);
  }

  @Override
  public boolean isNetworkException(final String sqlState) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isNetworkException(sqlState);
    }
    return this.exceptionManager.isNetworkException(this.dbDialect, sqlState);
  }

  @Override
  public boolean isLoginException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isLoginException(throwable, targetDriverDialect);
    }
    return this.exceptionManager.isLoginException(this.dbDialect, throwable, targetDriverDialect);
  }

  @Override
  public boolean isLoginException(final String sqlState) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isLoginException(sqlState);
    }
    return this.exceptionManager.isLoginException(this.dbDialect, sqlState);
  }

  @Override
  public Dialect getDialect() {
    return this.dbDialect;
  }

  @Override
  public TargetDriverDialect getTargetDriverDialect() {
    return this.targetDriverDialect;
  }

  @Override
  public void updateDialect(final @NonNull Connection connection) {
    // do nothing. This method is called after connecting in DefaultConnectionPlugin but the dialect passed to the
    // constructor should already be updated and verified.
  }

  @Override
  public HostSpec identifyConnection(Connection connection) throws SQLException {
    return this.getHostListProvider().identifyConnection(connection);
  }

  @Override
  public void fillAliases(Connection connection, HostSpec hostSpec) throws SQLException {
    if (hostSpec == null) {
      return;
    }

    if (!hostSpec.getAliases().isEmpty()) {
      LOGGER.finest(() -> Messages.get("PluginServiceImpl.nonEmptyAliases", new Object[] {hostSpec.getAliases()}));
      return;
    }

    hostSpec.addAlias(hostSpec.asAlias());

    // Add the host name and port, this host name is usually the internal IP address.
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet rs = stmt.executeQuery(this.getDialect().getHostAliasQuery())) {
        while (rs.next()) {
          hostSpec.addAlias(rs.getString(1));
        }
      }
    } catch (final SQLException sqlException) {
      // log and ignore
      LOGGER.finest(() -> Messages.get("PluginServiceImpl.failedToRetrieveHostPort"));
    }

    // Add the instance endpoint if the current connection is associated with a topology aware database cluster.
    final HostSpec host = this.identifyConnection(connection);
    if (host != null) {
      hostSpec.addAlias(host.asAliases().toArray(new String[] {}));
    }
  }

  @Override
  public HostSpecBuilder getHostSpecBuilder() {
    return new HostSpecBuilder(new HostAvailabilityStrategyFactory().create(this.props));
  }

  @Override
  public Properties getProperties() {
    return this.props;
  }

  public TelemetryFactory getTelemetryFactory() {
    return this.pluginManager.getTelemetryFactory();
  }

  public String getTargetName() {
    return this.pluginManager.getDefaultConnProvider().getTargetName();
  }

  @Override
  public @NonNull SessionStateService getSessionStateService() {
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getSessionStateService"}));
  }

  public <T> T getPlugin(final Class<T> pluginClazz) {
    for (ConnectionPlugin p : this.pluginManager.plugins) {
      if (pluginClazz.isAssignableFrom(p.getClass())) {
        return pluginClazz.cast(p);
      }
    }
    return null;
  }

  public boolean isPluginInUse(final Class<? extends ConnectionPlugin> pluginClazz) {
    try {
      return this.pluginManager.isWrapperFor(pluginClazz);
    } catch (SQLException e) {
      return false;
    }
  }

  @Override
  public Boolean isPooledConnection() {
    // This service implementation doesn't support call context.
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getSessionStateService"}));
  }

  @Override
  public void setIsPooledConnection(Boolean pooledConnection) {
    // This service implementation doesn't support call context.
    // Do nothing.
  }

  @Override
  public void resetCallContext() {
    // This service implementation doesn't support call context.
    throw new UnsupportedOperationException(
        Messages.get("PartialPluginService.unexpectedMethodCall", new Object[] {"getSessionStateService"}));
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == PluginService.class) {
      return iface.cast(this);
    }

    return this.pluginManager.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == PluginService.class) {
      return true;
    }

    return this.pluginManager.isWrapperFor(iface);
  }

  public static void clearCache() {
    hostAvailabilityExpiringCache.clear();
  }
}
