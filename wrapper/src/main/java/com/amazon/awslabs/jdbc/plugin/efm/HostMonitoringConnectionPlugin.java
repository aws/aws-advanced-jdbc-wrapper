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

package com.amazon.awslabs.jdbc.plugin.efm;

import com.amazon.awslabs.jdbc.HostAvailability;
import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.NodeChangeOptions;
import com.amazon.awslabs.jdbc.OldConnectionSuggestedAction;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.ProxyDriverProperty;
import com.amazon.awslabs.jdbc.cleanup.CanReleaseResources;
import com.amazon.awslabs.jdbc.plugin.AbstractConnectionPlugin;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Monitor the server while the connection is executing methods for more sophisticated failure
 * detection.
 */
public class HostMonitoringConnectionPlugin extends AbstractConnectionPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER =
      Logger.getLogger(HostMonitoringConnectionPlugin.class.getName());

  protected static final ProxyDriverProperty FAILURE_DETECTION_ENABLED =
      new ProxyDriverProperty(
          "failureDetectionEnabled",
          "true",
          "Enable failure detection logic (aka node monitoring thread).");

  protected static final ProxyDriverProperty FAILURE_DETECTION_TIME =
      new ProxyDriverProperty(
          "failureDetectionTime",
          "30000",
          "Interval in millis between sending SQL to the server and the first probe to database node.");

  protected static final ProxyDriverProperty FAILURE_DETECTION_INTERVAL =
      new ProxyDriverProperty(
          "failureDetectionInterval",
          "5000",
          "Interval in millis between probes to database node.");

  protected static final ProxyDriverProperty FAILURE_DETECTION_COUNT =
      new ProxyDriverProperty(
          "failureDetectionCount",
          "3",
          "Number of failed connection checks before considering database node unhealthy.");

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("*")));

  private static final String MYSQL_RETRIEVE_HOST_PORT_SQL =
      "SELECT CONCAT(@@hostname, ':', @@port)";
  private static final String PG_RETRIEVE_HOST_PORT_SQL =
      "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";

  private static final List<String> METHODS_TO_SKIP_MONITORING =
      Arrays.asList(".get", ".abort", ".close", ".next", ".create");

  protected @NonNull Properties properties;
  private MonitorService monitorService;
  private final @NonNull Supplier<MonitorService> monitorServiceSupplier;
  private final Set<String> nodeKeys = new HashSet<>();
  private final @NonNull PluginService pluginService;

  /**
   * Initialize the node monitoring plugin.
   *
   * @param pluginService A service allowing the plugin to retrieve the current active connection
   *     and its connection settings.
   * @param properties The property set used to initialize the active connection.
   */
  public HostMonitoringConnectionPlugin(
      final @NonNull PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService, properties, () -> new MonitorServiceImpl(pluginService));
  }

  HostMonitoringConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull Supplier<MonitorService> monitorServiceSupplier) {

    this.pluginService = pluginService;
    this.properties = properties;
    this.monitorServiceSupplier = monitorServiceSupplier;
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

    if (!isEnabled || !this.doesNeedMonitoring(methodName)) {
      return jdbcMethodFunc.call();
    }

    final int failureDetectionTimeMillis = FAILURE_DETECTION_TIME.getInteger(this.properties);
    final int failureDetectionIntervalMillis = FAILURE_DETECTION_INTERVAL.getInteger(this.properties);
    final int failureDetectionCount = FAILURE_DETECTION_COUNT.getInteger(this.properties);

    initMonitorService();

    T result;
    MonitorConnectionContext monitorContext = null;

    try {
      LOGGER.log(
          Level.FINEST, String.format("Executing method %s, monitoring is activated", methodName));

      this.nodeKeys.clear();
      this.nodeKeys.addAll(this.pluginService.getCurrentHostSpec().getAliases());

      monitorContext =
          this.monitorService.startMonitoring(
              this.pluginService.getCurrentConnection(), // abort this connection if needed
              this.nodeKeys,
              this.pluginService.getCurrentHostSpec(),
              this.properties,
              failureDetectionTimeMillis,
              failureDetectionIntervalMillis,
              failureDetectionCount);

      result = jdbcMethodFunc.call();

    } finally {
      if (monitorContext != null) {
        this.monitorService.stopMonitoring(monitorContext);

        boolean isConnectionClosed = false;
        try {
          isConnectionClosed = this.pluginService.getCurrentConnection().isClosed();
        } catch (SQLException e) {
          throw castException(exceptionClass, e);
        }

        if (monitorContext.isNodeUnhealthy()) {

          this.pluginService.setAvailability(this.nodeKeys, HostAvailability.NOT_AVAILABLE);

          if (!isConnectionClosed) {
            abortConnection();
            throw castException(
                exceptionClass,
                new SQLException(
                    String.format(
                        "Node [%s] is unavailable.",
                        this.pluginService.getCurrentHostSpec().asAlias())));
          }
        }
      }
      LOGGER.log(
          Level.FINEST, String.format("Executed method %s, monitoring is deactivated", methodName));
    }

    return result;
  }

  private <E extends Exception> E castException(
      final Class<E> exceptionClass, final SQLException exceptionToCast) {
    if (exceptionClass.isAssignableFrom(SQLException.class)) {
      return exceptionClass.cast(exceptionToCast);
    } else {
      return exceptionClass.cast(new RuntimeException(exceptionToCast));
    }
  }

  void abortConnection() {
    try {
      this.pluginService.getCurrentConnection().close();
    } catch (SQLException sqlEx) {
      // ignore
    }
  }

  /**
   * Checks whether the JDBC method passed to this connection plugin requires monitoring.
   *
   * @param methodName Name of the JDBC method.
   * @return true if the method requires monitoring; false otherwise.
   */
  protected boolean doesNeedMonitoring(String methodName) {

    for (final String method : METHODS_TO_SKIP_MONITORING) {
      if (methodName.contains(method)) {
        return false;
      }
    }

    // Monitor all the other methods
    return true;
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

  /**
   * Generate a set of node keys representing the node to monitor.
   *
   * @param driverProtocol Driver protocol for provided connection
   * @param connection the connection to a specific node.
   * @param hostSpec host details to add node keys to
   */
  private void generateHostAliases(
      final @NonNull String driverProtocol,
      final @NonNull Connection connection,
      final @NonNull HostSpec hostSpec) {

    hostSpec.addAlias(hostSpec.asAlias());

    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(getHostPortSql(driverProtocol))) {
        while (rs.next()) {
          hostSpec.addAlias(rs.getString(1));
        }
      }
    } catch (SQLException sqlException) {
      // log and ignore
      LOGGER.log(Level.FINEST, "Could not retrieve Host:Port for connection.");
    }
  }

  private String getHostPortSql(final @NonNull String driverProtocol) {
    if (driverProtocol.startsWith("jdbc:postgresql:")) {
      return PG_RETRIEVE_HOST_PORT_SQL;
    } else if (driverProtocol.startsWith("jdbc:mysql:")) {
      return MYSQL_RETRIEVE_HOST_PORT_SQL;
    } else {
      throw new UnsupportedOperationException(
          String.format("Driver protocol '%s' is not supported.", driverProtocol));
    }
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {

    if (changes.contains(NodeChangeOptions.WENT_DOWN)
        || changes.contains(NodeChangeOptions.NODE_DELETED)) {
      if (!this.nodeKeys.isEmpty()) {
        this.monitorService.stopMonitoringForAllConnections(this.nodeKeys);
      }
      this.nodeKeys.clear();
      this.nodeKeys.addAll(this.pluginService.getCurrentHostSpec().getAliases());
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

    Connection conn = connectFunc.call();

    if (conn != null) {
      generateHostAliases(driverProtocol, conn, hostSpec);
    }

    return conn;
  }
}
