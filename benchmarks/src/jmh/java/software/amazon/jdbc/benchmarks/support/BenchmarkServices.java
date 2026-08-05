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

package software.amazon.jdbc.benchmarks.support;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.UnknownDialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.efm.base.ConnectionContextService;
import software.amazon.jdbc.plugin.efm.base.SimpleConnectionContextServiceImpl;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.GenericTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.monitoring.MonitorService;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.StorageServiceImpl;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Lightweight stand-ins for the driver's service layer, so the wrapper's real code paths can be
 * benchmarked without a database and without Mockito.
 *
 * <p>The wrapper reaches the service layer several times per JDBC call
 * ({@code getCurrentConnection}, {@code resetCallContext}, {@code isXaTransactionActive}, ...). With
 * Mockito those calls cost more than the wrapper code being measured. These stand-ins are
 * {@link Proxy} instances that read and write plain fields for the methods the hot paths use and
 * return type-appropriate defaults for everything else, so what is measured is the wrapper.
 *
 * <p>{@code PluginServiceImpl} itself is therefore not what is measured here - it is stood in for.
 * Benchmarking the real {@code PluginServiceImpl} needs a real host list provider and dialect
 * detection; see {@code PluginServiceBenchmarks} for the parts of it that can be measured directly.
 *
 * <p>Not thread-safe by design: the state is plain fields. Every benchmark using this must run
 * single-threaded or hold one instance per thread.
 */
public final class BenchmarkServices {

  private BenchmarkServices() {
  }

  /** Mutable state shared by the service stand-ins, mirroring what PluginServiceImpl would hold. */
  public static final class State {
    public Connection currentConnection;
    public HostSpec currentHostSpec;
    public HostSpec initialHostSpec;
    public HostSpec routedHostSpec;
    public List<HostSpec> hosts = new ArrayList<>();
    public boolean inTransaction;
    public boolean xaTransactionActive;
    public Boolean pooledConnection = Boolean.FALSE;
    public Properties props = new Properties();
    public Dialect dialect;
    public TargetDriverDialect targetDriverDialect;
    public SessionStateService sessionStateService;
    public TelemetryFactory telemetryFactory;
    public final PluginCallContext callContext = new PluginCallContext();

    /** Counts host-list refreshes so a benchmark can assert the pipeline actually ran. */
    public long refreshCount;

    /** Supplies connections for {@code connect}/{@code forceConnect}, used by monitoring plugins. */
    public Supplier<Connection> monitoringConnectionSupplier = () -> FakeJdbc.connection(true, 1);

    /**
     * Returned by {@code PluginService.getDefaultConnectionProvider()}.
     *
     * <p>Needed because {@code MonitorServiceImpl} builds its own minimal services container - with a
     * real {@code PartialPluginService} and a real plugin chain - from this provider the first time a
     * monitor is created. Leaving it null makes any monitoring plugin fail at that point.
     */
    public ConnectionProvider defaultConnectionProvider;
  }

  /**
   * Builds a state object for a single-host topology on the given connection.
   */
  public static State state(final Connection connection, final TelemetryFactory telemetryFactory) {
    final State state = new State();
    state.currentConnection = connection;
    state.dialect = dialect();
    state.targetDriverDialect = targetDriverDialect();
    state.sessionStateService = sessionStateService();
    state.telemetryFactory = telemetryFactory;
    state.defaultConnectionProvider = new FakeConnectionProvider(connection);
    state.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-0.XYZ.us-east-2.rds.amazonaws.com")
        .hostId("instance-0")
        .port(5432)
        .role(HostRole.WRITER)
        .build();
    state.initialHostSpec = state.currentHostSpec;
    state.hosts = Collections.singletonList(state.currentHostSpec);
    return state;
  }

  /** A {@link PluginService} backed by {@code state}. */
  public static PluginService pluginService(final State state) {
    return proxy(PluginService.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getCurrentConnection":
          return state.currentConnection;
        case "setCurrentConnection":
          state.currentConnection = (Connection) args[0];
          state.currentHostSpec = (HostSpec) args[1];
          // The three-argument overload returns the set of observed changes.
          return method.getReturnType() == EnumSet.class
              ? EnumSet.noneOf(NodeChangeOptions.class)
              : null;
        case "getCurrentHostSpec":
          return state.currentHostSpec;
        case "getInitialConnectionHostSpec":
          return state.initialHostSpec;
        case "getRoutedHostSpec":
          return state.routedHostSpec;
        case "setRoutedHostSpec":
          state.routedHostSpec = (HostSpec) args[0];
          return null;
        case "getHosts":
        case "getAllHosts":
          return state.hosts;
        case "refreshHostList":
          state.refreshCount++;
          return null;
        case "getDialect":
          return state.dialect;
        case "getTargetDriverDialect":
          return state.targetDriverDialect;
        case "getSessionStateService":
          return state.sessionStateService;
        case "getCallContext":
          return state.callContext;
        case "getTelemetryFactory":
          return state.telemetryFactory;
        case "getProperties":
          return state.props;
        case "isInTransaction":
          return state.inTransaction;
        case "setInTransaction":
          state.inTransaction = (Boolean) args[0];
          return null;
        case "isXaTransactionActive":
          return state.xaTransactionActive;
        case "isPooledConnection":
          return state.pooledConnection;
        case "setIsPooledConnection":
          state.pooledConnection = (Boolean) args[0];
          return null;
        case "getHostSpecBuilder":
          return new HostSpecBuilder(new SimpleHostAvailabilityStrategy());
        case "getDefaultConnectionProvider":
          return state.defaultConnectionProvider;
        case "getTargetName":
          return "fake";
        case "getDriverProtocol":
          return "jdbc:postgresql://";
        case "getOriginalUrl":
          return "jdbc:aws-wrapper:postgresql://instance-0.XYZ.us-east-2.rds.amazonaws.com";
        case "isDialectConfirmed":
          return true;
        case "isStaticHostListProvider":
          return true;
        case "connect":
        case "forceConnect":
          // Monitoring plugins (efm, efm2) open their own probe connection from a background thread
          // through here. Handing back a fake connection keeps the benchmark free of network I/O and
          // of DNS timeouts that would otherwise show up as measurement noise.
          return state.monitoringConnectionSupplier.get();
        default:
          return Defaults.forProxyMethod(proxy, method, args);
      }
    });
  }

  /** A {@link PluginManagerService} backed by {@code state}. */
  public static PluginManagerService pluginManagerService(final State state) {
    return proxy(PluginManagerService.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "setInTransaction":
          state.inTransaction = (Boolean) args[0];
          return null;
        case "setIsPooledConnection":
          state.pooledConnection = (Boolean) args[0];
          return null;
        case "resetCallContext":
          state.callContext.reset();
          return null;
        default:
          return Defaults.forProxyMethod(proxy, method, args);
      }
    });
  }

  /**
   * A {@link FullServicesContainer} wiring the given services together. The storage service is the
   * real {@link StorageServiceImpl}; callers must release it (see {@link #releaseStorage}).
   */
  public static FullServicesContainer servicesContainer(
      final State state,
      final PluginService pluginService,
      final PluginManagerService pluginManagerService,
      final ConnectionPluginManager pluginManager,
      final StorageService storageService) {
    return servicesContainer(
        state, pluginService, pluginManagerService, pluginManager, storageService, true);
  }

  /**
   * As above, but lets the caller disable the {@link ImportantEventService}.
   *
   * <p>Worth a parameter because the service is enabled by default in production and
   * {@code DefaultConnectionPlugin.execute} calls {@code registerEvent} on every JDBC call, which
   * allocates an event and an {@link java.time.Instant}. Benchmarking both settings prices that.
   */
  public static FullServicesContainer servicesContainer(
      final State state,
      final PluginService pluginService,
      final PluginManagerService pluginManagerService,
      final ConnectionPluginManager pluginManager,
      final StorageService storageService,
      final boolean importantEventsEnabled) {
    final ImportantEventService eventService = importantEventService(importantEventsEnabled);
    final HostListProviderService hostListProviderService =
        proxy(HostListProviderService.class, Defaults::forProxyMethod);
    // Real implementations: the monitoring plugins register monitor types and acquire connection
    // contexts on their execute path, so standing these in would remove the very work being measured.
    // The monitor service is the driver's process-wide singleton, matching production.
    final MonitorService monitorService = CoreServicesContainer.getInstance().getMonitorService();
    final ConnectionContextService connectionContextService = new SimpleConnectionContextServiceImpl();
    final EventPublisher eventPublisher = new NoOpEventPublisher();
    return proxy(FullServicesContainer.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getPluginService":
          return pluginService;
        case "getPluginManagerService":
          return pluginManagerService;
        case "getConnectionPluginManager":
          return pluginManager;
        case "getHostListProviderService":
          return hostListProviderService;
        case "getImportantEventService":
          return eventService;
        case "getStorageService":
          return storageService;
        case "getMonitorService":
          return monitorService;
        case "getConnectionContextService":
          return connectionContextService;
        case "getEventPublisher":
          return eventPublisher;
        case "getDefaultConnectionProvider":
          return state.defaultConnectionProvider;
        case "getTelemetryFactory":
          return state.telemetryFactory;
        default:
          return Defaults.forProxyMethod(proxy, method, args);
      }
    });
  }

  public static StorageService storageService() {
    return new StorageServiceImpl(new NoOpEventPublisher());
  }

  public static void releaseStorage(final StorageService storageService) {
    if (storageService instanceof StorageServiceImpl) {
      ((StorageServiceImpl) storageService).releaseResources();
    }
  }

  /**
   * The real {@link ImportantEventService}. It is a concrete class, not an interface, and it is
   * enabled by default in production, so the real one is used rather than a stand-in: its
   * {@code registerEvent} call on every execute is part of the cost being measured.
   *
   * @param enabled whether events are recorded; false matches
   *                {@code -Daws.jdbc.config.exception.context.enabled=false}
   */
  public static ImportantEventService importantEventService(final boolean enabled) {
    return new ImportantEventService(enabled, 60_000L);
  }

  public static SessionStateService sessionStateService() {
    return proxy(SessionStateService.class, Defaults::forProxyMethod);
  }

  /**
   * The driver's own fallback dialects, not stand-ins.
   *
   * <p>Real plugins read these during construction and on the hot path - the Aurora connection
   * tracker builds its subscription set from
   * {@code TargetDriverDialect.getNetworkBoundMethodNames}, for example - so a proxy returning null
   * would both crash and misrepresent the cost. {@code UnknownDialect} and
   * {@code GenericTargetDriverDialect} are what the driver itself falls back to for an unrecognised
   * target, which makes them the honest choice here.
   */
  public static Dialect dialect() {
    return new UnknownDialect();
  }

  public static TargetDriverDialect targetDriverDialect() {
    return new GenericTargetDriverDialect();
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(final Class<T> iface, final InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(
        BenchmarkServices.class.getClassLoader(), new Class<?>[] {iface}, handler);
  }

  /** Shared proxy fallback: {@link Object} methods plus type-appropriate defaults. */
  static final class Defaults {
    private Defaults() {
    }

    static Object forProxyMethod(final Object proxy, final Method method, final Object[] args) {
      switch (method.getName()) {
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == (args == null ? null : args[0]);
        case "toString":
          return "BenchmarkServices:" + method.getDeclaringClass().getSimpleName();
        case "isWrapperFor":
          return false;
        case "unwrap":
          return proxy;
        default:
          return zero(method.getReturnType());
      }
    }

    private static Object zero(final Class<?> type) {
      if (!type.isPrimitive()) {
        return null;
      }
      if (type == boolean.class) {
        return false;
      }
      if (type == char.class) {
        return (char) 0;
      }
      if (type == byte.class) {
        return (byte) 0;
      }
      if (type == short.class) {
        return (short) 0;
      }
      if (type == int.class) {
        return 0;
      }
      if (type == long.class) {
        return 0L;
      }
      if (type == float.class) {
        return 0f;
      }
      if (type == double.class) {
        return 0d;
      }
      return null;
    }
  }
}
