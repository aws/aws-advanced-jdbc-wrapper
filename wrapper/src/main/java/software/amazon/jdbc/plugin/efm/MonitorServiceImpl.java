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

package software.amazon.jdbc.plugin.efm;

import java.sql.Connection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one or more
 * active connections.
 */
public class MonitorServiceImpl implements MonitorService {

  private static final Logger LOGGER = Logger.getLogger(MonitorServiceImpl.class.getName());

  protected static final AwsWrapperProperty MONITOR_DISPOSAL_TIME_MS =
      new AwsWrapperProperty(
          "monitorDisposalTime",
          "60000",
          "Interval in milliseconds for a monitor to be considered inactive and to be disposed.");

  private MonitorThreadContainer threadContainer;

  final MonitorInitializer monitorInitializer;
  private Set<String> cachedMonitorNodeKeys = null;
  private Monitor cachedMonitor = null;

  public MonitorServiceImpl(final @NonNull PluginService pluginService) {
    this(
        (hostSpec, properties, monitorService) ->
            new MonitorImpl(
                pluginService,
                hostSpec,
                properties,
                MONITOR_DISPOSAL_TIME_MS.getLong(properties),
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
      LOGGER.warning(
          () -> Messages.get(
              "MonitorServiceImpl.emptyAliasSet",
              new Object[] {hostSpec}));
      hostSpec.addAlias(hostSpec.asAlias());
    }

    Monitor monitor;
    if (this.cachedMonitor == null
        || this.cachedMonitorNodeKeys == null
        || !this.cachedMonitorNodeKeys.equals(nodeKeys)) {

      monitor = getMonitor(nodeKeys, hostSpec, properties);
      this.cachedMonitor = monitor;
      this.cachedMonitorNodeKeys = Collections.unmodifiableSet(nodeKeys);
    } else {
      monitor = this.cachedMonitor;
    }

    final MonitorConnectionContext context =
        new MonitorConnectionContext(
            monitor,
            connectionToAbort,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount);

    monitor.startMonitoring(context);

    return context;
  }

  @Override
  public void stopMonitoring(@NonNull MonitorConnectionContext context) {
    Monitor monitor = context.getMonitor();
    monitor.stopMonitoring(context);
  }

  @Override
  public void stopMonitoringForAllConnections(@NonNull Set<String> nodeKeys) {
    Monitor monitor;
    for (String nodeKey : nodeKeys) {
      monitor = this.threadContainer.getMonitor(nodeKey);
      if (monitor != null) {
        monitor.clearContexts();
        this.threadContainer.resetResource(monitor);
        return;
      }
    }
  }

  @Override
  public void releaseResources() {
    this.threadContainer = null;
    MonitorThreadContainer.releaseInstance();
  }

  @Override
  public void notifyUnused(Monitor monitor) {
    if (monitor == null) {
      LOGGER.warning(() -> Messages.get("MonitorServiceImpl.nullMonitorParam"));
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

  MonitorThreadContainer getThreadContainer() {
    return this.threadContainer;
  }
}
