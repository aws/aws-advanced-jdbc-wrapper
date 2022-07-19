/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.PluginService;
import software.aws.rds.jdbc.proxydriver.ProxyDriverProperty;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one or more
 * active connections.
 */
public class MonitorServiceImpl implements MonitorService {

  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());

  protected static final ProxyDriverProperty MONITOR_DISPOSAL_TIME_MS =
      new ProxyDriverProperty(
          "monitorDisposalTime",
          "60000",
          "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");

  MonitorThreadContainer threadContainer;

  final MonitorInitializer monitorInitializer;

  public MonitorServiceImpl(final @NonNull PluginService pluginService) {
    this(
        (hostSpec, properties, monitorService) ->
            new MonitorImpl(
                pluginService,
                hostSpec,
                properties,
                MONITOR_DISPOSAL_TIME_MS.getInt(properties),
                monitorService),
        () ->
            Executors.newCachedThreadPool(
                r -> {
                  final Thread monitoringThread = new Thread(r);
                  monitoringThread.setDaemon(true);
                  return monitoringThread;
                }));
  }

  MonitorServiceImpl(
      MonitorInitializer monitorInitializer,
      ExecutorServiceInitializer executorServiceInitializer) {

    this.monitorInitializer = monitorInitializer;
    this.threadContainer = MonitorThreadContainer.getInstance(executorServiceInitializer);
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      Connection connectionToAbort,
      Set<String> nodeKeys,
      HostSpec hostSpec,
      Properties properties,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    if (nodeKeys.isEmpty()) {
      LOGGER.log(
          Level.WARNING,
          String.format("Empty alias set passed for %s. Set should not be empty.", hostSpec));
      hostSpec.addAlias(hostSpec.asAlias());
    }

    final Monitor monitor = getMonitor(nodeKeys, hostSpec, properties);

    final MonitorConnectionContext context =
        new MonitorConnectionContext(
            connectionToAbort,
            nodeKeys,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount);

    monitor.startMonitoring(context);
    this.threadContainer.addTask(monitor);

    return context;
  }

  @Override
  public void stopMonitoring(@NonNull MonitorConnectionContext context) {

    // Any 1 node is enough to find the monitor containing the context
    // All nodes will map to the same monitor
    final String node = this.threadContainer.getNode(context.getHostAliases());

    if (node == null) {
      LOGGER.log(
          Level.WARNING,
          "Invalid context passed into DefaultMonitorService. Could not find any NodeKey from context.");
      return;
    }

    this.threadContainer.getMonitor(node).stopMonitoring(context);
  }

  @Override
  public void stopMonitoringForAllConnections(@NonNull Set<String> nodeKeys) {
    final String node = this.threadContainer.getNode(nodeKeys);
    if (node == null) {
      LOGGER.log(
          Level.FINEST,
          "Invalid node key passed into DefaultMonitorService. No existing monitor for the given set of node keys.");
      return;
    }
    final Monitor monitor = this.threadContainer.getMonitor(node);
    monitor.clearContexts();
    this.threadContainer.resetResource(monitor);
  }

  @Override
  public void releaseResources() {
    this.threadContainer = null;
    MonitorThreadContainer.releaseInstance();
  }

  @Override
  public synchronized void notifyUnused(Monitor monitor) {
    if (monitor == null) {
      LOGGER.log(Level.WARNING, "Parameter 'monitor' should not be null.");
      return;
    }

    // Remove monitor from the maps
    this.threadContainer.releaseResource(monitor);
  }

  /**
   * Get or create a {@link MonitorImpl} for a server.
   *
   * @param nodeKeys All references to the server requiring monitoring.
   * @param hostSpec Information such as hostname of the server.
   * @param properties The user configuration for the current connection.
   * @return A {@link MonitorImpl} object associated with a specific server.
   */
  protected Monitor getMonitor(Set<String> nodeKeys, HostSpec hostSpec, Properties properties) {
    return this.threadContainer.getOrCreateMonitor(
        nodeKeys, () -> monitorInitializer.createMonitor(hostSpec, properties, this));
  }
}
