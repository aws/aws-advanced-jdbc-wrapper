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

package software.amazon.jdbc.plugin.efm2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;

/**
 * Monitor the server while the connection is executing methods for more sophisticated failure
 * detection.
 */
public class HostMonitoringConnectionPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER =
      Logger.getLogger(HostMonitoringConnectionPlugin.class.getName());

  public static final AwsWrapperProperty FAILURE_DETECTION_ENABLED =
      new AwsWrapperProperty(
          "failureDetectionEnabled",
          "true",
          "Enable failure detection logic (aka node monitoring thread).");

  public static final AwsWrapperProperty FAILURE_DETECTION_TIME =
      new AwsWrapperProperty(
          "failureDetectionTime",
          "30000",
          "Interval in millis between sending SQL to the server and the first probe to database node.");

  public static final AwsWrapperProperty FAILURE_DETECTION_INTERVAL =
      new AwsWrapperProperty(
          "failureDetectionInterval",
          "5000",
          "Interval in millis between probes to database node.");

  public static final AwsWrapperProperty FAILURE_DETECTION_COUNT =
      new AwsWrapperProperty(
          "failureDetectionCount",
          "3",
          "Number of failed connection checks before considering database node unhealthy.");

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Collections.singletonList("*")));

  protected @NonNull Properties properties;
  private final @NonNull Supplier<MonitorService> monitorServiceSupplier;
  private final @NonNull PluginService pluginService;
  private MonitorService monitorService;
  private final RdsUtils rdsHelper;
  private HostSpec monitoringHostSpec;

  static {
    PropertyDefinition.registerPluginProperties(HostMonitoringConnectionPlugin.class);
    PropertyDefinition.registerPluginProperties("monitoring-");
  }

  /**
   * Initialize the node monitoring plugin.
   *
   * @param pluginService A service allowing the plugin to retrieve the current active connection
   *     and its connection settings.
   * @param properties The property set used to initialize the active connection.
   */
  public HostMonitoringConnectionPlugin(
      final @NonNull PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService, properties, () -> new MonitorServiceImpl(pluginService), new RdsUtils());
  }

  HostMonitoringConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull Supplier<MonitorService> monitorServiceSupplier,
      final RdsUtils rdsHelper) {
    if (pluginService == null) {
      throw new IllegalArgumentException("pluginService");
    }
    if (properties == null) {
      throw new IllegalArgumentException("properties");
    }
    if (monitorServiceSupplier == null) {
      throw new IllegalArgumentException("monitorServiceSupplier");
    }
    this.pluginService = pluginService;
    this.properties = properties;
    this.monitorServiceSupplier = monitorServiceSupplier;
    this.rdsHelper = rdsHelper;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  /**
   * Executes the given SQL function with {@link MonitorImpl} if connection monitoring is enabled.
   * Otherwise, executes the SQL function directly.
   */
  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    // update config settings since they may change
    final boolean isEnabled = FAILURE_DETECTION_ENABLED.getBoolean(this.properties);

    if (!isEnabled || !SubscribedMethodHelper.NETWORK_BOUND_METHODS.contains(methodName)) {
      return jdbcMethodFunc.call();
    }

    final int failureDetectionTimeMillis = FAILURE_DETECTION_TIME.getInteger(this.properties);
    final int failureDetectionIntervalMillis =
        FAILURE_DETECTION_INTERVAL.getInteger(this.properties);
    final int failureDetectionCount = FAILURE_DETECTION_COUNT.getInteger(this.properties);

    initMonitorService();

    T result;
    MonitorConnectionContext monitorContext = null;

    try {
      LOGGER.finest(
          () -> Messages.get(
              "HostMonitoringConnectionPlugin.activatedMonitoring",
              new Object[] {methodName}));

      final HostSpec monitoringHostSpec = this.getMonitoringHostSpec();

      monitorContext =
          this.monitorService.startMonitoring(
              this.pluginService.getCurrentConnection(), // abort this connection if needed
              monitoringHostSpec,
              this.properties,
              failureDetectionTimeMillis,
              failureDetectionIntervalMillis,
              failureDetectionCount);

      result = jdbcMethodFunc.call();

    } finally {
      if (monitorContext != null) {
        this.monitorService.stopMonitoring(monitorContext, this.pluginService.getCurrentConnection());
      }

      LOGGER.finest(
          () -> Messages.get(
              "HostMonitoringConnectionPlugin.monitoringDeactivated",
              new Object[] {methodName}));
    }

    return result;
  }

  private void initMonitorService() {
    if (this.monitorService == null) {
      this.monitorService = this.monitorServiceSupplier.get();
    }
  }

  /** Call this plugin's monitor service to release all resources associated with this plugin. */
  @Override
  public void releaseResources() {
    if (this.monitorService != null) {
      this.monitorService.releaseResources();
    }

    this.monitorService = null;
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    if (changes.contains(NodeChangeOptions.HOSTNAME)
        || changes.contains(NodeChangeOptions.NODE_CHANGED)) {

      // Reset monitoring HostSpec since the associated connection has changed.
      this.monitoringHostSpec = null;
    }

    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public Connection connect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final Connection conn = connectFunc.call();

    if (conn != null) {
      final RdsUrlType type = this.rdsHelper.identifyRdsType(hostSpec.getHost());
      if (type.isRdsCluster()) {
        hostSpec.resetAliases();
        this.pluginService.fillAliases(conn, hostSpec);
      }
    }

    return conn;
  }

  public HostSpec getMonitoringHostSpec() {
    if (this.monitoringHostSpec == null) {
      this.monitoringHostSpec = this.pluginService.getCurrentHostSpec();
      final RdsUrlType rdsUrlType = this.rdsHelper.identifyRdsType(monitoringHostSpec.getUrl());

      try {
        if (rdsUrlType.isRdsCluster()) {
          LOGGER.finest("Monitoring HostSpec is associated with a cluster endpoint, "
              + "plugin needs to identify the cluster connection.");
          this.monitoringHostSpec = this.pluginService.identifyConnection(this.pluginService.getCurrentConnection());
          if (this.monitoringHostSpec == null) {
            throw new RuntimeException(Messages.get(
                "HostMonitoringConnectionPlugin.unableToIdentifyConnection",
                new Object[] {
                    this.pluginService.getCurrentHostSpec().getHost(),
                    this.pluginService.getHostListProvider()}));
          }
          this.pluginService.fillAliases(this.pluginService.getCurrentConnection(), monitoringHostSpec);
        }
      } catch (SQLException e) {
        // Log and throw.
        LOGGER.finest(Messages.get("HostMonitoringConnectionPlugin.errorIdentifyingConnection", new Object[] {e}));
        throw new RuntimeException(e);
      }
    }
    return this.monitoringHostSpec;
  }
}
