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

package software.amazon.jdbc.plugin.efm.v3;

import static software.amazon.jdbc.plugin.efm.base.HostMonitoringConnectionBasePlugin.FAILURE_DETECTION_ENABLED;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * Enhanced failure monitoring, third implementation ({@code efm3}).
 *
 * <p>Unlike {@code efm} and {@code efm2}, this plugin registers the connection with the host monitor once instead of
 * registering every JDBC call. The per-call work is two atomic writes to a {@link MonitoredConnectionSlot} that only
 * this connection's thread writes, with no allocation, no lock, and no shared state. Host resolution happens once per
 * physical connection and never mutates the plugin service.
 */
public class HostMonitoringConnectionPluginV3 extends AbstractConnectionPlugin
    implements CanReleaseResources, StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(HostMonitoringConnectionPluginV3.class.getName());

  protected final @NonNull PluginService pluginService;
  protected final @NonNull Properties properties;
  protected final @NonNull Supplier<HostMonitorServiceV3Impl> monitorServiceSupplier;
  protected final @NonNull RdsUtils rdsHelper;
  protected final boolean isEnabled;
  protected final Set<String> subscribedMethods;

  protected @Nullable HostMonitorServiceV3Impl monitorService;
  protected volatile @Nullable MonitoredConnectionSlot slot;

  public HostMonitoringConnectionPluginV3(
      final @NonNull FullServicesContainer servicesContainer, final @NonNull Properties properties) {
    this(
        servicesContainer,
        properties,
        () -> new HostMonitorServiceV3Impl(servicesContainer, properties),
        new RdsUtils());
  }

  protected HostMonitoringConnectionPluginV3(
      final @NonNull FullServicesContainer servicesContainer,
      final @NonNull Properties properties,
      final @NonNull Supplier<HostMonitorServiceV3Impl> monitorServiceSupplier,
      final @NonNull RdsUtils rdsHelper) {
    this.pluginService = servicesContainer.getPluginService();
    this.properties = properties;
    this.monitorServiceSupplier = monitorServiceSupplier;
    this.rdsHelper = rdsHelper;
    this.isEnabled = FAILURE_DETECTION_ENABLED.getBoolean(properties);

    final Set<String> methods = new HashSet<>();
    if (this.isEnabled) {
      methods.add(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName);
      methods.addAll(this.pluginService.getTargetDriverDialect().getNetworkBoundMethodNames(properties));
    }
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return this.subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable Object[] jdbcMethodArgs)
      throws E {

    if (!this.isEnabled || !this.subscribedMethods.contains(methodName)) {
      return jdbcMethodFunc.call();
    }

    final MonitoredConnectionSlot slot;
    try {
      slot = this.getSlot();
    } catch (final SQLException e) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
    }

    if (slot.enter(System.nanoTime())) {
      // The monitor aborted this connection while it was idle. The call below fails on the dead connection; make sure
      // the host is reported unavailable before that happens.
      this.pluginService.setAvailability(slot.getHostSpec(), HostAvailability.NOT_AVAILABLE);
    }

    try {
      final HostMonitorV3Impl monitor = slot.getMonitor();
      if (monitor == null || monitor.isStopped()) {
        try {
          this.getMonitorService().register(slot);
        } catch (final SQLException e) {
          throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
        }
      }

      return jdbcMethodFunc.call();

    } finally {
      if (slot.exit(System.nanoTime())) {
        this.pluginService.setAvailability(slot.getHostSpec(), HostAvailability.NOT_AVAILABLE);
      }
    }
  }

  /**
   * Returns the slot for the current physical connection, creating it when the connection is new or has changed. The
   * host is resolved once per connection: an instance endpoint is used as is, anything else is identified through the
   * connection and falls back to the endpoint itself when the host cannot be identified (RDS Proxy, for example).
   */
  protected MonitoredConnectionSlot getSlot() throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final MonitoredConnectionSlot existing = this.slot;
    if (existing != null && existing.getConnection() == currentConnection) {
      return existing;
    }

    if (existing != null) {
      this.getMonitorService().unregister(existing);
    }

    final HostSpec monitoringHostSpec = this.resolveMonitoringHostSpec(currentConnection);
    final MonitoredConnectionSlot created =
        new MonitoredConnectionSlot(currentConnection, monitoringHostSpec, System.nanoTime());
    this.slot = created;
    return created;
  }

  protected HostSpec resolveMonitoringHostSpec(final @NonNull Connection connection) throws SQLException {
    final HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();
    final RdsUrlType rdsUrlType = this.rdsHelper.identifyRdsType(currentHostSpec.getHost());
    if (rdsUrlType == RdsUrlType.RDS_INSTANCE) {
      return currentHostSpec;
    }

    try {
      final HostSpec identified = this.pluginService.identifyConnection(connection, currentHostSpec);
      return identified != null ? identified : currentHostSpec;
    } catch (final SQLException e) {
      LOGGER.finest(() -> Messages.get(
          "HostMonitoringConnectionPlugin.errorIdentifyingConnection", new Object[] {e}));
      throw e;
    }
  }

  protected HostMonitorServiceV3Impl getMonitorService() {
    HostMonitorServiceV3Impl service = this.monitorService;
    if (service == null) {
      service = this.monitorServiceSupplier.get();
      this.monitorService = service;
    }
    return service;
  }

  protected void dropSlot() {
    final MonitoredConnectionSlot existing = this.slot;
    if (existing != null) {
      this.slot = null;
      final HostMonitorV3Impl monitor = existing.getMonitor();
      if (monitor != null) {
        monitor.unregister(existing);
      }
    }
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    if (changes.contains(NodeChangeOptions.HOSTNAME)
        || changes.contains(NodeChangeOptions.NODE_CHANGED)
        || changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED)) {
      // The connection or its host changed; the slot is rebuilt on the next monitored call.
      this.dropSlot();
    }
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void releaseResources() {
    this.dropSlot();
    this.monitorService = null;
  }

  // Checker Framework: snapshot values are intentionally nullable, but the StateSnapshotProvider
  // contract types them as Pair<String, Object> (non-null Object). Widening that interface across
  // all implementers is out of scope, so the nullable value inference is suppressed locally.
  @Override
  @SuppressWarnings("type.arguments.not.inferred")
  public List<Pair<String, Object>> getSnapshotState() {
    final List<Pair<String, Object>> state = new ArrayList<>();
    PropertyUtils.addSnapshotState(state, "properties", this.properties);
    final MonitoredConnectionSlot current = this.slot;
    state.add(Pair.create("slot", current != null ? current.toString() : null));
    WrapperUtils.addSnapshotState(state, "monitorService", this.monitorService);
    state.add(Pair.create("isEnabled", this.isEnabled));
    return state;
  }
}
