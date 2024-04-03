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
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.dialect.DialectProvider;
import software.amazon.jdbc.dialect.HostListProviderSupplier;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategyFactory;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.states.SessionStateServiceImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class PluginServiceImpl implements PluginService, CanReleaseResources,
    HostListProviderService, PluginManagerService {

  private static final Logger LOGGER = Logger.getLogger(PluginServiceImpl.class.getName());
  protected static final long DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO = TimeUnit.MINUTES.toNanos(5);

  protected static final CacheMap<String, HostAvailability> hostAvailabilityExpiringCache = new CacheMap<>();

  protected static final CacheMap<String, Object> statusesExpiringCache = new CacheMap<>();
  protected static final long DEFAULT_STATUS_CACHE_EXPIRE_NANO = TimeUnit.MINUTES.toNanos(60);

  protected final ConnectionPluginManager pluginManager;
  private final Properties props;
  private final String originalUrl;
  private final String driverProtocol;
  protected volatile HostListProvider hostListProvider;
  protected List<HostSpec> allHosts = new ArrayList<>();
  protected AtomicReference<AllowedAndBlockedHosts> allowedAndBlockedHosts = new AtomicReference<>();
  protected Connection currentConnection;
  protected HostSpec currentHostSpec;
  protected HostSpec initialConnectionHostSpec;
  private boolean isInTransaction;
  private final ExceptionManager exceptionManager;
  protected final @Nullable ExceptionHandler exceptionHandler;
  protected final DialectProvider dialectProvider;
  protected Dialect dialect;
  protected TargetDriverDialect targetDriverDialect;
  protected @Nullable final ConfigurationProfile configurationProfile;
  protected final ConnectionProviderManager connectionProviderManager;

  protected final SessionStateService sessionStateService;

  protected final ReentrantLock connectionSwitchLock = new ReentrantLock();

  public PluginServiceImpl(
      @NonNull final ConnectionPluginManager pluginManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      @NonNull final String targetDriverProtocol,
      @NonNull final TargetDriverDialect targetDriverDialect)
      throws SQLException {

    this(pluginManager,
        new ExceptionManager(),
        props,
        originalUrl,
        targetDriverProtocol,
        null,
        targetDriverDialect,
        null,
        null);
  }

  public PluginServiceImpl(
      @NonNull final ConnectionPluginManager pluginManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      @NonNull final String targetDriverProtocol,
      @NonNull final TargetDriverDialect targetDriverDialect,
      @Nullable final ConfigurationProfile configurationProfile) throws SQLException {
    this(pluginManager,
        new ExceptionManager(),
        props,
        originalUrl,
        targetDriverProtocol,
        null,
        targetDriverDialect,
        configurationProfile,
        null);
  }

  public PluginServiceImpl(
      @NonNull final ConnectionPluginManager pluginManager,
      @NonNull final ExceptionManager exceptionManager,
      @NonNull final Properties props,
      @NonNull final String originalUrl,
      @NonNull final String targetDriverProtocol,
      @Nullable final DialectProvider dialectProvider,
      @NonNull final TargetDriverDialect targetDriverDialect,
      @Nullable final ConfigurationProfile configurationProfile,
      @Nullable final SessionStateService sessionStateService) throws SQLException {
    this.pluginManager = pluginManager;
    this.props = props;
    this.originalUrl = originalUrl;
    this.driverProtocol = targetDriverProtocol;
    this.configurationProfile = configurationProfile;
    this.exceptionManager = exceptionManager;
    this.dialectProvider = dialectProvider != null ? dialectProvider : new DialectManager(this);
    this.targetDriverDialect = targetDriverDialect;
    this.connectionProviderManager = new ConnectionProviderManager(
        this.pluginManager.getDefaultConnProvider(),
        this.pluginManager.getEffectiveConnProvider());

    this.sessionStateService = sessionStateService != null
        ? sessionStateService
        : new SessionStateServiceImpl(this, this.props);

    this.exceptionHandler = this.configurationProfile != null && this.configurationProfile.getExceptionHandler() != null
        ? this.configurationProfile.getExceptionHandler()
        : null;

    this.dialect = this.configurationProfile != null && this.configurationProfile.getDialect() != null
        ? this.configurationProfile.getDialect()
        : this.dialectProvider.getDialect(this.driverProtocol, this.originalUrl, this.props);
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
        if (this.getAllHosts().isEmpty()) {
          throw new RuntimeException(Messages.get("PluginServiceImpl.hostListEmpty"));
        }

        this.currentHostSpec = this.getWriter(this.getAllHosts());
        final List<HostSpec> allowedHosts = this.getHosts();
        if (!Utils.containsUrl(allowedHosts, this.currentHostSpec.getUrl())) {
          throw new RuntimeException(
              Messages.get("PluginServiceImpl.currentHostNotAllowed",
                  new Object[] {
                      currentHostSpec == null ? "<null>" : currentHostSpec.getUrl(),
                      Utils.logTopology(allowedHosts, "")})
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
  public void setAllowedAndBlockedHosts(AllowedAndBlockedHosts allowedAndBlockedHosts) {
    this.allowedAndBlockedHosts.set(allowedAndBlockedHosts);
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) throws SQLException {
    return this.pluginManager.acceptsStrategy(role, strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) throws SQLException {
    return this.pluginManager.getHostSpecByStrategy(role, strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy) throws SQLException {
    return this.pluginManager.getHostSpecByStrategy(hosts, role, strategy);
  }

  @Override
  public HostRole getHostRole(Connection conn) throws SQLException {
    return this.hostListProvider.getHostRole(conn);
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
  @Deprecated
  public ConnectionProvider getConnectionProvider() {
    return this.pluginManager.defaultConnProvider;
  }

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

    setCurrentConnection(connection, hostSpec, null);
  }

  @Override
  public EnumSet<NodeChangeOptions> setCurrentConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec,
      @Nullable final ConnectionPlugin skipNotificationForThisPlugin)
      throws SQLException {

    connectionSwitchLock.lock();
    try {

      if (this.currentConnection == null) {
        // setting up an initial connection

        this.currentConnection = connection;
        this.currentHostSpec = hostSpec;
        this.sessionStateService.reset();

        final EnumSet<NodeChangeOptions> changes = EnumSet.of(NodeChangeOptions.INITIAL_CONNECTION);
        this.pluginManager.notifyConnectionChanged(changes, skipNotificationForThisPlugin);

        return changes;

      } else {
        // update an existing connection

        final EnumSet<NodeChangeOptions> changes = compare(this.currentConnection, this.currentHostSpec,
            connection, hostSpec);

        if (!changes.isEmpty()) {

          final Connection oldConnection = this.currentConnection;
          final boolean isInTransaction = this.isInTransaction;
          this.sessionStateService.begin();

          try {
            this.currentConnection = connection;
            this.currentHostSpec = hostSpec;

            this.sessionStateService.applyCurrentSessionState(connection);
            this.setInTransaction(false);

            if (isInTransaction && PropertyDefinition.ROLLBACK_ON_SWITCH.getBoolean(this.props)) {
              try {
                oldConnection.rollback();
              } catch (final SQLException e) {
                // Ignore any exception
              }
            }

            final EnumSet<OldConnectionSuggestedAction> pluginOpinions = this.pluginManager.notifyConnectionChanged(
                changes, skipNotificationForThisPlugin);

            final boolean shouldCloseConnection =
                changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)
                    && !oldConnection.isClosed()
                    && !pluginOpinions.contains(OldConnectionSuggestedAction.PRESERVE);

            if (shouldCloseConnection) {
              try {
                this.sessionStateService.applyPristineSessionState(oldConnection);
              } catch (final SQLException e) {
                // Ignore any exception
              }

              try {
                oldConnection.close();
              } catch (final SQLException e) {
                // Ignore any exception
              }
            }
          } finally {
            this.sessionStateService.complete();
          }
        }
        return changes;
      }
    } finally {
      connectionSwitchLock.unlock();
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
  public List<HostSpec> getAllHosts() {
    return this.allHosts;
  }

  @Override
  public List<HostSpec> getHosts() {
    AllowedAndBlockedHosts hostPermissions = this.allowedAndBlockedHosts.get();
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
    return this.isInTransaction;
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
    final List<HostSpec> updatedHostList = this.getHostListProvider().refresh(connection);
    if (!Objects.equals(updatedHostList, this.allHosts)) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.allHosts, updatedHostList);
    }
  }

  @Override
  public void forceRefreshHostList() throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().forceRefresh();
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.allHosts, updatedHostList);
    }
  }

  @Override
  public void forceRefreshHostList(final Connection connection) throws SQLException {
    final List<HostSpec> updatedHostList = this.getHostListProvider().forceRefresh(connection);
    if (updatedHostList != null) {
      updateHostAvailability(updatedHostList);
      setNodeList(this.allHosts, updatedHostList);
    }
  }

  @Override
  public boolean forceRefreshHostList(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException {

    final HostListProvider hostListProvider = this.getHostListProvider();
    if (!(hostListProvider instanceof BlockingHostListProvider)) {
      throw new UnsupportedOperationException(
          Messages.get("PluginServiceImpl.requiredBlockingHostListProvider",
              new Object[]{hostListProvider.getClass().getName()}));
    }

    try {
      final List<HostSpec> updatedHostList =
          ((BlockingHostListProvider) hostListProvider).forceRefresh(shouldVerifyWriter, timeoutMs);
      if (updatedHostList != null) {
        updateHostAvailability(updatedHostList);
        setNodeList(this.allHosts, updatedHostList);
        return true;
      }
    } catch (TimeoutException ex) {
      // do nothing.
      LOGGER.finest(Messages.get("PluginServiceImpl.forceRefreshTimeout", new Object[]{timeoutMs}));
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
    return this.connect(hostSpec, props, null);
  }

  @Override
  public Connection connect(
      final HostSpec hostSpec,
      final Properties props,
      final @Nullable ConnectionPlugin pluginToSkip)
      throws SQLException {
    return this.pluginManager.connect(
        this.driverProtocol, hostSpec, props, this.currentConnection == null, pluginToSkip);
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
        this.driverProtocol, hostSpec, props, this.currentConnection == null, pluginToSkip);
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
    LOGGER.finest(() -> Messages.get("PluginServiceImpl.releaseResources"));

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
    return this.isNetworkException(throwable, this.targetDriverDialect);
  }

  @Override
  public boolean isNetworkException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isNetworkException(throwable, targetDriverDialect);
    }
    return this.exceptionManager.isNetworkException(this.dialect, throwable, targetDriverDialect);
  }

  @Override
  public boolean isNetworkException(final String sqlState) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isNetworkException(sqlState);
    }
    return this.exceptionManager.isNetworkException(this.dialect, sqlState);
  }

  @Override
  public boolean isLoginException(Throwable throwable) {
    return this.isLoginException(throwable, this.targetDriverDialect);
  }

  @Override
  public boolean isLoginException(final Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isLoginException(throwable, targetDriverDialect);
    }
    return this.exceptionManager.isLoginException(this.dialect, throwable, targetDriverDialect);
  }

  @Override
  public boolean isLoginException(final String sqlState) {
    if (this.exceptionHandler != null) {
      return this.exceptionHandler.isLoginException(sqlState);
    }
    return this.exceptionManager.isLoginException(this.dialect, sqlState);
  }

  @Override
  public Dialect getDialect() {
    return this.dialect;
  }

  @Override
  public TargetDriverDialect getTargetDriverDialect() {
    return this.targetDriverDialect;
  }

  public void updateDialect(final @NonNull Connection connection) throws SQLException {
    final Dialect originalDialect = this.dialect;
    this.dialect = this.dialectProvider.getDialect(
        this.originalUrl,
        this.initialConnectionHostSpec,
        connection);
    if (originalDialect == this.dialect) {
      return;
    }

    final HostListProviderSupplier supplier = this.dialect.getHostListProvider();
    this.setHostListProvider(supplier.getProvider(props, this.originalUrl, this, this));
    this.refreshHostList(connection);
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
      LOGGER.finest(() -> Messages.get("PluginServiceImpl.nonEmptyAliases", new Object[]{hostSpec.getAliases()}));
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
      hostSpec.addAlias(host.asAliases().toArray(new String[]{}));
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
    return this.sessionStateService;
  }

  public <T> T getPlugin(final Class<T> pluginClazz) {
    for (ConnectionPlugin p : this.pluginManager.plugins) {
      if (pluginClazz.isAssignableFrom(p.getClass())) {
        return pluginClazz.cast(p);
      }
    }
    return null;
  }

  public static void clearCache() {
    hostAvailabilityExpiringCache.clear();
  }

  public <T> void setStatus(final Class<T> clazz, final @Nullable T status, final boolean clusterBound) {
    String clusterId = null;
    if (clusterBound) {
      try {
        clusterId = this.hostListProvider.getClusterId();
      } catch (Exception ex) {
        // do nothing
      }
    }
    this.setStatus(clazz, status, clusterId);
  }

  public <T> void setStatus(final Class<T> clazz, final @Nullable T status, final String key) {
    final String cacheKey = this.getStatusCacheKey(clazz, key);
    if (status == null) {
      statusesExpiringCache.remove(cacheKey);
    } else {
      statusesExpiringCache.put(cacheKey, status, DEFAULT_STATUS_CACHE_EXPIRE_NANO);
    }
  }

  public <T> T getStatus(final @NonNull Class<T> clazz, final boolean clusterBound) {
    String clusterId = null;
    if (clusterBound) {
      try {
        clusterId = this.hostListProvider.getClusterId();
      } catch (Exception ex) {
        // do nothing
      }
    }
    return this.getStatus(clazz, clusterId);
  }

  public <T> T getStatus(final @NonNull Class<T> clazz, String key) {
    return clazz.cast(statusesExpiringCache.get(this.getStatusCacheKey(clazz, key)));
  }

  protected <T> String getStatusCacheKey(final Class<T> clazz, final String key) {
    return String.format("%s::%s", key == null ? "" : key.trim().toLowerCase(), clazz.getName());
  }

  public boolean isPluginInUse(final Class<? extends ConnectionPlugin> pluginClazz) {
    try {
      return this.pluginManager.isWrapperFor(pluginClazz);
    } catch (SQLException e) {
      return false;
    }
  }
}
