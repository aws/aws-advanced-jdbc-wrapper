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

package software.amazon.jdbc.util.monitoring;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.DriverConnectionProvider;
import software.amazon.jdbc.TargetDriverHelper;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostlistprovider.monitoring.ClusterTopologyMonitorImpl;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.connection.ConnectionServiceImpl;
import software.amazon.jdbc.util.events.DataAccessEvent;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.events.EventSubscriber;
import software.amazon.jdbc.util.storage.ExternallyManagedCache;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.Topology;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class MonitorServiceImpl implements MonitorService, EventSubscriber {
  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());
  protected static final long DEFAULT_CLEANUP_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
  protected static final Map<Class<? extends Monitor>, Supplier<CacheContainer>> defaultSuppliers;

  static {
    Map<Class<? extends Monitor>, Supplier<CacheContainer>> suppliers = new HashMap<>();
    Set<MonitorErrorResponse> recreateOnError =
        new HashSet<>(Collections.singletonList(MonitorErrorResponse.RECREATE));
    MonitorSettings defaultSettings = new MonitorSettings(
        TimeUnit.MINUTES.toNanos(5), TimeUnit.MINUTES.toNanos(1), recreateOnError);

    suppliers.put(
        CustomEndpointMonitorImpl.class, () -> new CacheContainer(defaultSettings, AllowedAndBlockedHosts.class));
    MonitorSettings topologySettings =
        new MonitorSettings(TimeUnit.MINUTES.toNanos(5), TimeUnit.MINUTES.toNanos(3), recreateOnError);
    suppliers.put(ClusterTopologyMonitorImpl.class, () -> new CacheContainer(topologySettings, Topology.class));
    defaultSuppliers = Collections.unmodifiableMap(suppliers);
  }

  protected final EventPublisher publisher;
  protected final Map<Class<? extends Monitor>, CacheContainer> monitorCaches = new ConcurrentHashMap<>();
  protected final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor((r -> {
    final Thread thread = new Thread(r);
    thread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(thread.getName())) {
      thread.setName(thread.getName() + "-msi");
    }
    return thread;
  }));


  public MonitorServiceImpl(EventPublisher publisher) {
    this(DEFAULT_CLEANUP_INTERVAL_NANOS, publisher);
  }

  /**
   * Constructs a MonitorServiceImpl instance, subscribes to the given publisher's data access events, and submits a
   * cleanup thread to supervise submitted monitors.
   *
   * @param cleanupIntervalNanos the interval at which the cleanup thread should check on submitted monitors, in
   *                             nanoseconds.
   * @param publisher            the publisher to subscribe to for data access events.
   */
  public MonitorServiceImpl(long cleanupIntervalNanos, EventPublisher publisher) {
    this.publisher = publisher;
    this.publisher.subscribe(this, new HashSet<>(Collections.singletonList(DataAccessEvent.class)));
    initCleanupThread(cleanupIntervalNanos);
  }

  protected void initCleanupThread(long cleanupIntervalNanos) {
    cleanupExecutor.scheduleAtFixedRate(
        this::checkMonitors, cleanupIntervalNanos, cleanupIntervalNanos, TimeUnit.NANOSECONDS);
  }

  protected void checkMonitors() {
    LOGGER.finest(Messages.get("MonitorServiceImpl.checkingMonitors"));
    for (CacheContainer container : monitorCaches.values()) {
      ExternallyManagedCache<Object, MonitorItem> cache = container.getCache();
      // Note: the map returned by getEntries is a copy of the ExternallyManagedCache map
      for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
        Object key = entry.getKey();
        MonitorItem removedItem = cache.removeIf(key, mi -> mi.getMonitor().getState() == MonitorState.STOPPED);
        if (removedItem != null) {
          removedItem.getMonitor().stop();
          continue;
        }

        MonitorSettings monitorSettings = container.getSettings();
        removedItem = cache.removeIf(key, mi -> mi.getMonitor().getState() == MonitorState.ERROR);
        if (removedItem != null) {
          LOGGER.finest(
              Messages.get("MonitorServiceImpl.removedErrorMonitor", new Object[] {removedItem.getMonitor()}));
          handleMonitorError(container, key, removedItem);
          continue;
        }

        long inactiveTimeoutNanos = monitorSettings.getInactiveTimeoutNanos();
        removedItem = cache.removeIf(
            key, mi -> System.nanoTime() - mi.getMonitor().getLastActivityTimestampNanos() > inactiveTimeoutNanos);
        if (removedItem != null) {
          // Monitor has been inactive for longer than the inactive timeout and is considered stuck.
          LOGGER.fine(
              Messages.get("MonitorServiceImpl.monitorStuck",
                  new Object[] {removedItem.getMonitor(), TimeUnit.NANOSECONDS.toMillis(inactiveTimeoutNanos)}));
          handleMonitorError(container, key, removedItem);
          continue;
        }

        removedItem = cache.removeExpiredIf(key, mi -> mi.getMonitor().canDispose());
        if (removedItem != null) {
          LOGGER.fine(
              Messages.get("MonitorServiceImpl.removedExpiredMonitor", new Object[] {removedItem.getMonitor()}));
          removedItem.getMonitor().stop();
        }
      }
    }
  }

  protected void handleMonitorError(
      CacheContainer cacheContainer,
      Object key,
      MonitorItem errorMonitorItem) {
    Monitor monitor = errorMonitorItem.getMonitor();
    monitor.stop();

    Set<MonitorErrorResponse> errorResponses = cacheContainer.getSettings().getErrorResponses();
    if (errorResponses.contains(MonitorErrorResponse.RECREATE)) {
      cacheContainer.getCache().computeIfAbsent(key, k -> {
        LOGGER.fine(Messages.get("MonitorServiceImpl.recreatingMonitor", new Object[] {monitor}));
        MonitorItem newMonitorItem = new MonitorItem(errorMonitorItem.getMonitorSupplier());
        newMonitorItem.getMonitor().start();
        return newMonitorItem;
      });
    }
  }

  @Override
  public <T extends Monitor> void registerMonitorTypeIfAbsent(
      Class<T> monitorClass,
      long expirationTimeoutNanos,
      long heartbeatTimeoutNanos,
      Set<MonitorErrorResponse> errorResponses,
      @Nullable Class<?> producedDataClass) {
    monitorCaches.computeIfAbsent(
        monitorClass,
        mc -> new CacheContainer(
            new MonitorSettings(expirationTimeoutNanos, heartbeatTimeoutNanos, errorResponses),
            producedDataClass));
  }

  @Override
  public <T extends Monitor> T runIfAbsent(
      Class<T> monitorClass,
      Object key,
      StorageService storageService,
      TelemetryFactory telemetryFactory,
      String originalUrl,
      String driverProtocol,
      TargetDriverDialect driverDialect,
      Dialect dbDialect,
      Properties originalProps,
      MonitorInitializer initializer) throws SQLException {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      Supplier<CacheContainer> supplier = defaultSuppliers.get(monitorClass);
      if (supplier == null) {
        throw new IllegalStateException(
            Messages.get("MonitorServiceImpl.monitorTypeNotRegistered", new Object[] {monitorClass}));
      }

      cacheContainer = monitorCaches.computeIfAbsent(monitorClass, k -> supplier.get());
    }

    TargetDriverHelper helper = new TargetDriverHelper();
    java.sql.Driver driver = helper.getTargetDriver(originalUrl, originalProps);
    final ConnectionProvider defaultConnectionProvider = new DriverConnectionProvider(driver);
    final Properties propsCopy = PropertyUtils.copyProperties(originalProps);
    final ConnectionServiceImpl connectionService = new ConnectionServiceImpl(
        storageService,
        this,
        telemetryFactory,
        defaultConnectionProvider,
        originalUrl,
        driverProtocol,
        driverDialect,
        dbDialect,
        propsCopy);

    Monitor monitor = cacheContainer.getCache().computeIfAbsent(key, k -> {
      MonitorItem monitorItem = new MonitorItem(() -> initializer.createMonitor(
          connectionService,
          connectionService.getPluginService()));
      monitorItem.getMonitor().start();
      return monitorItem;
    }).getMonitor();

    if (monitorClass.isInstance(monitor)) {
      return monitorClass.cast(monitor);
    }

    throw new IllegalStateException(
        Messages.get("MonitorServiceImpl.unexpectedMonitorClass", new Object[] {monitorClass, monitor}));
  }

  @Override
  public @Nullable <T extends Monitor> T get(Class<T> monitorClass, Object key) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      return null;
    }

    MonitorItem item = cacheContainer.getCache().get(key);
    if (item == null) {
      return null;
    }

    Monitor monitor = item.getMonitor();
    if (monitorClass.isInstance(monitor)) {
      return monitorClass.cast(monitor);
    }

    LOGGER.fine(
        Messages.get(
            "MonitorServiceImpl.monitorClassMismatch",
            new Object[]{key, monitorClass, monitor, monitor.getClass()}));
    return null;
  }

  @Override
  public <T extends Monitor> T remove(Class<T> monitorClass, Object key) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      return null;
    }

    MonitorItem item = cacheContainer.getCache().removeIf(
        key, monitorItem -> monitorClass.isInstance(monitorItem.getMonitor()));
    if (item == null) {
      return null;
    }

    return monitorClass.cast(item.getMonitor());
  }

  @Override
  public <T extends Monitor> void stopAndRemove(Class<T> monitorClass, Object key) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.stopAndRemoveMissingMonitorType", new Object[] {monitorClass, key}));
      return;
    }

    MonitorItem monitorItem = cacheContainer.getCache().remove(key);
    if (monitorItem != null) {
      monitorItem.getMonitor().stop();
    }
  }

  @Override
  public <T extends Monitor> void stopAndRemoveMonitors(Class<T> monitorClass) {
    CacheContainer cacheContainer = monitorCaches.get(monitorClass);
    if (cacheContainer == null) {
      LOGGER.fine(Messages.get("MonitorServiceImpl.stopAndRemoveMonitorsMissingType", new Object[] {monitorClass}));
      return;
    }

    ExternallyManagedCache<Object, MonitorItem> cache = cacheContainer.getCache();
    for (Map.Entry<Object, MonitorItem> entry : cache.getEntries().entrySet()) {
      MonitorItem monitorItem = cache.remove(entry.getKey());
      if (monitorItem != null) {
        monitorItem.getMonitor().stop();
      }
    }
  }

  @Override
  public void stopAndRemoveAll() {
    for (Class<? extends Monitor> monitorClass : monitorCaches.keySet()) {
      stopAndRemoveMonitors(monitorClass);
    }
  }

  @Override
  public void releaseResources() {
    cleanupExecutor.shutdownNow();
    stopAndRemoveAll();
  }

  @Override
  public void processEvent(Event event) {
    if (!(event instanceof DataAccessEvent)) {
      return;
    }

    DataAccessEvent accessEvent = (DataAccessEvent) event;
    for (CacheContainer container : monitorCaches.values()) {
      if (container.getProducedDataClass() == null
          || !accessEvent.getDataClass().equals(container.getProducedDataClass())) {
        continue;
      }

      // The data produced by the monitor in this cache with this key has been accessed recently, so we extend the
      // monitor's expiration.
      container.getCache().extendExpiration(accessEvent.getKey());
    }
  }

  /**
   * A container that holds a cache of monitors of a given type with the related settings and info for that type.
   */
  protected static class CacheContainer {
    private @NonNull final MonitorSettings settings;
    private @NonNull final ExternallyManagedCache<Object, MonitorItem> cache;
    private @Nullable final Class<?> producedDataClass;

    /**
     * Constructs a CacheContainer instance. As part of the constructor, a new cache will be created based on the given
     * settings.
     *
     * @param settings          the settings for the cache and monitor type.
     * @param producedDataClass the class of the data produced by the monitor type, if it produces any data.
     */
    public CacheContainer(@NonNull final MonitorSettings settings, @Nullable Class<?> producedDataClass) {
      this.settings = settings;
      this.cache = new ExternallyManagedCache<>(settings.getExpirationTimeoutNanos());
      this.producedDataClass = producedDataClass;
    }

    public @NonNull MonitorSettings getSettings() {
      return settings;
    }

    public @NonNull ExternallyManagedCache<Object, MonitorItem> getCache() {
      return cache;
    }

    public @Nullable Class<?> getProducedDataClass() {
      return producedDataClass;
    }
  }

  /**
   * A container object that holds a monitor together with the supplier used to generate the monitor. The supplier can
   * be used to recreate the monitor if it encounters an error or becomes stuck.
   */
  protected static class MonitorItem {
    private @NonNull final Supplier<? extends Monitor> monitorSupplier;
    private @NonNull final Monitor monitor;

    /**
     * Constructs a MonitorItem instance. As part of the constructor, a new monitor will be created using the given
     * monitor supplier.
     *
     * @param monitorSupplier a supplier lambda that produces a monitor.
     */
    protected MonitorItem(@NonNull Supplier<? extends Monitor> monitorSupplier) {
      this.monitorSupplier = monitorSupplier;
      this.monitor = monitorSupplier.get();
    }

    public @NonNull Supplier<? extends Monitor> getMonitorSupplier() {
      return monitorSupplier;
    }

    public @NonNull Monitor getMonitor() {
      return monitor;
    }
  }
}
