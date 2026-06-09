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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.ServiceUtility;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;

public class ClusterTopologyMonitorImpl extends AbstractMonitor
    implements ClusterTopologyMonitor, StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(ClusterTopologyMonitorImpl.class.getName());

  protected static final String MONITORING_PROPERTY_PREFIX = "topology-monitoring-";
  protected static final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected static final RdsUtils rdsHelper = new RdsUtils();
  protected static final long monitorTerminationTimeoutSec = 30;
  protected static final int closeConnectionNetworkTimeoutMs = 500;
  protected static final int defaultConnectionTimeoutMs = 5000;
  protected static final int defaultSocketTimeoutMs = 5000;

  // Keep monitoring topology at a high rate for 30s after failover.
  protected static final long highRefreshPeriodAfterPanicNano = TimeUnit.SECONDS.toNanos(30);

  private static final long INITIAL_BACKOFF_MS = 100;
  private static final long MAX_BACKOFF_MS = 10000;
  private static final Random random = new Random();
  private static final long STABLE_TOPOLOGIES_DURATION_NANO = TimeUnit.SECONDS.toNanos(15);

  protected final AtomicReference<HostSpec> writerHostSpec = new AtomicReference<>(null);
  /**
   * The last writer we believed to be the cluster's writer, retained even after {@link #writerHostSpec} is
   * cleared on errors. Used by panic-mode node threads as a baseline for writer-change detection.
   */
  protected volatile @Nullable HostSpec lastKnownWriterHostSpec = null;
  protected AtomicConnection monitoringConnection;

  protected final Object topologyUpdated = new Object();
  protected final AtomicBoolean requestToUpdateTopology = new AtomicBoolean(false);
  protected final ConcurrentHashMap<String, Boolean> submittedNodes = new ConcurrentHashMap<>();

  protected final ResourceLock nodeExecutorLock = new ResourceLock();
  protected final AtomicBoolean nodeThreadsStop = new AtomicBoolean(false);
  protected final AtomicReference<HostSpec> nodeThreadsWriterHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<List<HostSpec>> nodeThreadsLatestTopology = new AtomicReference<>(null);
  // Stores connections opened by node monitoring threads, keyed by HostSpec.
  // These connections are harvested by the main loop when panic mode resolves and offered to the
  // connection handler so that monitoring can use the most-preferred one without re-opening connections.
  protected final ConcurrentHashMap<HostSpec, AtomicConnection> nodeThreadsConnections =
      new ConcurrentHashMap<>();

  protected final Map<String, List<HostSpec>> readerTopologiesById = new ConcurrentHashMap<>();
  // Tracks whether all node monitors have completed at least one work cycle, even if an exception occurs. We use this
  // map to guard against concluding all reader topologies are stable when not all monitors have booted up yet.
  protected final Map<String, Boolean> completedOneCycle = new ConcurrentHashMap<>();
  protected long stableTopologiesStartNano;
  // When comparing topologies, we don't want to check HostSpec.weight, which is used in HostSpec#equals. We will use
  // this function to compare the other fields.
  protected Function<HostSpec, List<Object>> hostSpecExtractor =
      host -> Arrays.asList(host.getHost(), host.getPort(), host.getAvailability(), host.getRole());

  protected final long refreshRateNano;
  protected final long highRefreshRateNano;
  protected final TopologyUtils topologyUtils;
  protected final FullServicesContainer servicesContainer;
  protected final Properties properties;
  protected final Properties monitoringProperties;
  protected final HostSpec initialHostSpec;
  protected final HostSpec instanceTemplate;

  protected ExecutorService nodeExecutorService = null;
  protected long highRefreshRateEndTimeNano = 0;
  protected String clusterId;
  protected boolean logUnclosedConnections = false;
  protected MonitoringConnectionHandler connectionHandler;

  public ClusterTopologyMonitorImpl(
      final FullServicesContainer servicesContainer,
      final TopologyUtils topologyUtils,
      final String clusterId,
      final HostSpec initialHostSpec,
      final Properties properties,
      final HostSpec instanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano) {
    super(monitorTerminationTimeoutSec);

    this.servicesContainer = servicesContainer;
    this.topologyUtils = topologyUtils;
    this.clusterId = clusterId;
    this.initialHostSpec = initialHostSpec;
    this.instanceTemplate = instanceTemplate;
    this.properties = properties;
    this.refreshRateNano = refreshRateNano;
    this.highRefreshRateNano = highRefreshRateNano;

    this.logUnclosedConnections = PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.getBoolean(properties);
    this.monitoringConnection = new AtomicConnection(this, this.logUnclosedConnections);

    this.monitoringProperties = PropertyUtils.copyProperties(properties);
    this.properties.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              this.monitoringProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.properties.getProperty(p));
              this.monitoringProperties.remove(p);
            });

    // Set default values if they are not provided.
    if (PropertyDefinition.SOCKET_TIMEOUT.getString(this.monitoringProperties) == null) {
      PropertyDefinition.SOCKET_TIMEOUT.set(
          this.monitoringProperties, String.valueOf(defaultSocketTimeoutMs));
    }
    if (PropertyDefinition.CONNECT_TIMEOUT.getString(this.monitoringProperties) == null) {
      PropertyDefinition.CONNECT_TIMEOUT.set(
          this.monitoringProperties, String.valueOf(defaultConnectionTimeoutMs));
    }
  }

  /**
   * Returns the connection handler, creating it lazily on first access.
   * Lazy initialization is required so that subclass state (e.g., shadowed fields) is fully initialized
   * before {@link #createConnectionHandler()} is invoked.
   */
  protected MonitoringConnectionHandler getConnectionHandler() {
    if (this.connectionHandler == null) {
      this.connectionHandler = this.createConnectionHandler();
    }
    return this.connectionHandler;
  }

  @Override
  public boolean canDispose() {
    return true;
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean verifyTopology, final long timeoutMs)
      throws SQLException, TimeoutException {

    final long currentTimeNano = System.nanoTime();
    try {
      if (verifyTopology) {
        // Enter panic mode, which will verify the topology for us.
        this.monitoringConnection.set(null);
      }

      return this.waitForTopologyUpdate(timeoutMs);
    } finally {
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.topologyUpdated",
          new Object[] {TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - currentTimeNano)}));
    }
  }

  protected List<HostSpec> waitForTopologyUpdate(final long timeoutMs) throws TimeoutException {
    List<HostSpec> currentHosts = getStoredHosts();
    List<HostSpec> latestHosts;

    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);

      // Notify the monitoring thread, which may be sleeping, that topology should be refreshed immediately.
      this.requestToUpdateTopology.notifyAll();
    }

    if (timeoutMs == 0) {
      LOGGER.finest(() ->
          LogUtils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.timeoutSetToZero")));
      return currentHosts;
    }

    final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    // Note: we are checking reference equality instead of value equality. We will break out of the loop if there is a
    // new entry in the topology cache, even if the value of the hosts in latestHosts is the same as currentHosts.
    while ((currentHosts == (latestHosts = getStoredHosts()) && System.nanoTime() < end)) {
      try {
        synchronized (this.topologyUpdated) {
          this.topologyUpdated.wait(1000);
        }
      } catch (InterruptedException ex) {
        LOGGER.fine(() -> Messages.get("ClusterTopologyMonitorImpl.interrupted"));
        Thread.currentThread().interrupt();
        return null;
      }
    }

    if (System.nanoTime() >= end) {
      throw new TimeoutException(
          Messages.get("ClusterTopologyMonitorImpl.topologyNotUpdated", new Object[] {timeoutMs}));
    }

    return latestHosts;
  }

  protected List<HostSpec> getStoredHosts() {
    Topology topology =
        this.servicesContainer.getStorageService().get(Topology.class, this.clusterId, false);
    return topology == null ? null : topology.getHosts();
  }

  @Override
  public void stop() {
    this.stop.set(true);
    this.nodeThreadsStop.set(true);

    this.closeNodeMonitors();
    this.nodeThreadConnectionCleanUp();
    this.monitoringConnection.set(null);

    // This code interrupts the waiting/sleeping cycle in the monitoring thread.
    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);
      this.requestToUpdateTopology.notifyAll();
    }

    super.stop();
  }

  @Override
  public void close() {
    if (this.connectionHandler != null) {
      this.connectionHandler.close();
    }
    this.closeNodeMonitors();
    this.nodeThreadConnectionCleanUp();
    this.monitoringConnection.clean();
    this.servicesContainer.getEventPublisher().unsubscribe(
        this, Collections.singleton(MonitorResetEvent.class));
  }

  @Override
  public void monitor() throws Exception {
    try {
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.startMonitoringThread",
          new Object[] {this.clusterId, this.initialHostSpec.getHost()}));
      this.servicesContainer.getEventPublisher().subscribe(
          this, Collections.singleton(MonitorResetEvent.class));

      while (!this.stop.get() && !Thread.currentThread().isInterrupted()) {
        this.lastActivityTimestampNanos.set(System.nanoTime());

        if (this.isInPanicMode()) {
          if (this.submittedNodes.isEmpty()) {
            LOGGER.finest(() -> Messages.get("ClusterTopologyMonitorImpl.startingNodeMonitoringThreads"));

            // Start node monitors.
            this.nodeThreadsStop.set(false);
            this.nodeThreadConnectionCleanUp();
            this.nodeThreadsWriterHostSpec.set(null);
            this.nodeThreadsLatestTopology.set(null);

            List<HostSpec> hosts = getStoredHosts();
            if (hosts == null) {
              // Use any available connection to get the topology.
              hosts = this.openAnyConnectionAndUpdateTopology();
            }

            this.closeNodeMonitors();
            this.createNodeExecutorService();

            if (hosts != null && this.monitoringConnection.get() == null) {
              final List<HostSpec> monitoredHosts = this.filterHostsForNodeMonitoring(hosts);
              final boolean someRegionsInaccessible = monitoredHosts.size() < hosts.size();
              final HostSpec baselineWriter = this.lastKnownWriterHostSpec;
              for (HostSpec hostSpec : monitoredHosts) {
                // A list is used to store the exception since lambdas require references to outer variables to be
                // final. This allows us to identify if an error occurred while creating the node monitoring worker.
                final List<Exception> exceptionList = new ArrayList<>();
                this.submittedNodes.computeIfAbsent(hostSpec.getHost(),
                    (key) -> {
                      final ExecutorService nodeExecutorServiceCopy = this.nodeExecutorService;
                      if (nodeExecutorServiceCopy != null) {
                        try {
                          this.nodeExecutorService.submit(
                              this.getNodeMonitoringWorker(
                                  hostSpec, baselineWriter, someRegionsInaccessible));
                        } catch (SQLException e) {
                          exceptionList.add(e);
                          return null;
                        }
                      }
                      return true;
                    });

                if (!exceptionList.isEmpty()) {
                  throw exceptionList.get(0);
                }
              }
              // We do not call nodeExecutorService.shutdown() here since more node monitors may be submitted later.
            }
            // We will try again in the next iteration.
          } else {
            // The node monitors are running, so we check if the writer has been detected.
            final HostSpec writerConnectionHostSpec = this.nodeThreadsWriterHostSpec.get();
            if (writerConnectionHostSpec != null) {
              LOGGER.finest(() -> Messages.get(
                      "ClusterTopologyMonitorImpl.writerPickedUpFromNodeMonitors",
                      new Object[] {writerConnectionHostSpec}));

              this.writerHostSpec.set(writerConnectionHostSpec);
              this.lastKnownWriterHostSpec = writerConnectionHostSpec;
              this.highRefreshRateEndTimeNano = System.nanoTime() + highRefreshPeriodAfterPanicNano;

              // Stop node threads and let them finish.
              this.nodeThreadsStop.set(true);
              this.closeNodeMonitors();

              // Offer all harvested connections to the handler so it can pick the best one.
              final HostSpec selected = this.getConnectionHandler().acceptConnections(
                  this.nodeThreadsConnections,
                  writerConnectionHostSpec,
                  this.getStoredHosts());

              // Clean up any harvested connections that were not selected.
              for (Map.Entry<HostSpec, AtomicConnection> entry : this.nodeThreadsConnections.entrySet()) {
                if (selected == null || !selected.equals(entry.getKey())) {
                  entry.getValue().clean();
                }
              }
              this.nodeThreadsConnections.clear();

              this.submittedNodes.clear();
              this.stableTopologiesStartNano = 0;
              this.readerTopologiesById.clear();
              this.completedOneCycle.clear();

              continue;

            } else {
              // Update node monitors with the new instances in the topology
              List<HostSpec> hosts = this.nodeThreadsLatestTopology.get();
              if (hosts != null && !this.nodeThreadsStop.get()) {
                final List<HostSpec> monitoredHosts = this.filterHostsForNodeMonitoring(hosts);
                final boolean someRegionsInaccessible = monitoredHosts.size() < hosts.size();
                final HostSpec baselineWriter = this.lastKnownWriterHostSpec;
                for (HostSpec hostSpec : monitoredHosts) {
                  // A list is used to store the exception since lambdas require references to outer variables to be
                  // final. This allows us to identify if an error occurred while creating the node monitoring worker.
                  final List<Exception> exceptionList = new ArrayList<>();
                  this.submittedNodes.computeIfAbsent(hostSpec.getHost(),
                      (key) -> {
                        try {
                          this.nodeExecutorService.submit(
                              this.getNodeMonitoringWorker(
                                  hostSpec, baselineWriter, someRegionsInaccessible));
                        } catch (SQLException e) {
                          exceptionList.add(e);
                          return null;
                        }

                        return true;
                      });

                  if (!exceptionList.isEmpty()) {
                    throw exceptionList.get(0);
                  }
                }
                // We do not call nodeExecutorService.shutdown() here since more node monitors may be submitted later.
              }
            }
          }

          this.checkForStableReaderTopologies();
          this.delay(true);

        } else {
          // We are in regular mode (not panic mode).
          if (!this.submittedNodes.isEmpty()) {
            this.closeNodeMonitors();
            this.nodeThreadConnectionCleanUp();
            this.submittedNodes.clear();
            this.stableTopologiesStartNano = 0;
            this.readerTopologiesById.clear();
            this.completedOneCycle.clear();
          }

          final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
          if (hosts == null) {
            // Attempt to fetch topology failed, so we switch to panic mode.
            this.monitoringConnection.set(null);
            // Clear the live writerHostSpec — it can no longer be considered current. The previously known
            // writer remains in lastKnownWriterHostSpec so that panic-mode node threads can use it as a baseline
            // for writer-change detection.
            this.writerHostSpec.set(null);
            continue;
          }

          // Refresh lastKnownWriterHostSpec from the freshly fetched topology so that, if the monitoring
          // connection later breaks, panic-mode node threads have an accurate baseline for change detection.
          final HostSpec topologyWriter = hosts.stream()
              .filter(h -> h.getRole() == HostRole.WRITER)
              .findFirst()
              .orElse(null);
          if (topologyWriter != null) {
            this.lastKnownWriterHostSpec = topologyWriter;
          }

          // Attempt to upgrade the monitoring connection if not at highest priority. The handler is given the
          // filtered host list so candidates in inaccessible regions (e.g. when gdbAccessibleRegions is set)
          // are not considered for upgrade attempts.
          this.getConnectionHandler().attemptConnectionUpgrade(this.filterHostsForNodeMonitoring(hosts));

          if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() > this.highRefreshRateEndTimeNano) {
            this.highRefreshRateEndTimeNano = 0;
          }

          // We avoid logging the topology while using the high refresh rate because it is too noisy.
          if (this.highRefreshRateEndTimeNano == 0) {
            LOGGER.finest(() -> LogUtils.logTopology(getStoredHosts()));
          }

          this.delay(false);
        }
      }

    } catch (final InterruptedException intEx) {
      Thread.currentThread().interrupt();
    } catch (final Exception ex) {
      // This should not be reached.
      // We want to print the full trace stack of the exception.
      LOGGER.log(Level.FINEST, ex, () -> Messages.get(
              "ClusterTopologyMonitorImpl.exceptionDuringMonitoringStop",
              new Object[] {this.initialHostSpec.getHost()}));

      throw ex;
    } finally {
      this.stop.set(true);
      this.closeNodeMonitors();
      this.monitoringConnection.clean();
      this.nodeThreadConnectionCleanUp();

      this.servicesContainer.getEventPublisher().unsubscribe(
          this, Collections.singleton(MonitorResetEvent.class));

      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.stopMonitoringThread",
          new Object[] {this.initialHostSpec.getHost()}));
    }
  }

  protected void checkForStableReaderTopologies() {
    List<HostSpec> latestHosts = this.getStoredHosts();
    if (Utils.isNullOrEmpty(latestHosts)) {
      this.stableTopologiesStartNano = 0;
      return;
    }

    // Only check hosts we are actually monitoring. Subclasses may filter the topology (e.g., GDB filters out
    // hosts in non-accessible regions). Without this filter, hosts that we never spawned node workers for would
    // perpetually appear "incomplete" and prevent the stable-topology detection from ever succeeding.
    List<String> readerIds = this.filterHostsForNodeMonitoring(latestHosts).stream()
        .map(HostSpec::getHostId).collect(Collectors.toList());
    for (String id : readerIds) {
      Boolean completedOneCycle = this.completedOneCycle.getOrDefault(id, Boolean.FALSE);
      if (!completedOneCycle) {
        // Not all reader monitors have completed a cycle. We shouldn't conclude that reader topologies are stable until
        // each reader monitor has made at least one attempt to fetch topology information, even if unsuccessful.
        this.stableTopologiesStartNano = 0;
        return;
      }
    }

    List<HostSpec> readerTopology = this.readerTopologiesById.values().stream().findFirst().orElse(null);
    if (readerTopology == null) {
      // readerTopologiesById has been cleared since checking its size.
      this.stableTopologiesStartNano = 0;
      return;
    }

    // Check whether the topologies match. HostSpecs are compared using their host, port, role, and availability fields.
    // Note that monitors that encounter exceptions will remove their entry from the map, so only entries from
    // successful monitors are checked.
    if (this.readerTopologiesById.values().stream()
        .map(list -> list.stream().map(hostSpecExtractor).collect(Collectors.toList()))
        .distinct().count() != 1) {
      // The topologies detected by each reader do not match.
      this.stableTopologiesStartNano = 0;
      return;
    }

    // All reader topologies match.
    if (this.stableTopologiesStartNano == 0) {
      this.stableTopologiesStartNano = System.nanoTime();
    }

    if (System.nanoTime() > this.stableTopologiesStartNano + this.getStableTopologiesDurationNano()) {
      // Reader topologies have been consistent for stableTopologiesDurationNano, so the topology should be accurate.
      this.stableTopologiesStartNano = 0;
      this.updateHostsAvailability(readerTopology);
      LOGGER.finest(() -> LogUtils.logTopology(readerTopology, Messages.get(
          "ClusterTopologyMonitorImpl.matchingReaderTopologies",
          new Object[]{TimeUnit.NANOSECONDS.toMillis(this.getStableTopologiesDurationNano())})));
      this.updateTopologyCache(readerTopology);

      // Reader topology is stable. Even though no writer was detected by node threads (e.g., the writer may live
      // in a region we don't monitor), the readers we did probe have established connections that we can use as
      // the monitoring connection. Stop node threads, harvest their connections, and let the handler pick the best
      // one so we can exit panic mode.
      if (this.monitoringConnection.get() == null && !this.nodeThreadsConnections.isEmpty()) {
        this.nodeThreadsStop.set(true);
        this.closeNodeMonitors();

        final HostSpec selected = this.getConnectionHandler().acceptConnections(
            this.nodeThreadsConnections,
            this.writerHostSpec.get(),
            readerTopology);

        for (Map.Entry<HostSpec, AtomicConnection> entry : this.nodeThreadsConnections.entrySet()) {
          if (selected == null || !selected.equals(entry.getKey())) {
            entry.getValue().clean();
          }
        }
        this.nodeThreadsConnections.clear();

        this.submittedNodes.clear();
        this.readerTopologiesById.clear();
        this.completedOneCycle.clear();
      }
    }
  }

  protected void reset() {
    LOGGER.finest(() -> Messages.get("ClusterTopologyMonitorImpl.reset",
            new Object[]{this.clusterId, this.initialHostSpec.getHost()}));

    this.nodeThreadsStop.set(true);
    this.closeNodeMonitors();
    this.nodeThreadConnectionCleanUp();
    this.nodeThreadsStop.set(false);
    this.submittedNodes.clear();
    this.stableTopologiesStartNano = 0;
    this.readerTopologiesById.clear();
    this.completedOneCycle.clear();
    this.createNodeExecutorService();

    this.nodeThreadsWriterHostSpec.set(null);
    this.nodeThreadsLatestTopology.set(null);

    this.monitoringConnection.set(null);
    this.writerHostSpec.set(null);
    this.lastKnownWriterHostSpec = null;
    if (this.connectionHandler != null) {
      this.connectionHandler.close();
    }
    this.highRefreshRateEndTimeNano = 0;
    this.requestToUpdateTopology.set(false);
    this.clearTopologyCache();

    // This breaks any waiting/sleeping cycles in the monitoring thread
    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);
      this.requestToUpdateTopology.notifyAll();
    }
  }

  @Override
  public void processEvent(Event event) {
    if (event instanceof MonitorResetEvent) {
      LOGGER.finest(() -> Messages.get("ClusterTopologyMonitorImpl.resetEventReceived"));
      final MonitorResetEvent resetEvent = (MonitorResetEvent) event;
      if (resetEvent.getClusterId().equals(this.clusterId)) {
        this.reset();
      }
    }
  }

  protected void nodeThreadConnectionCleanUp() {
    // Clean up any connections harvested from node threads that haven't been claimed yet.
    final Connection currentMonitoring = this.monitoringConnection.get();
    for (AtomicConnection atomicConn : this.nodeThreadsConnections.values()) {
      if (atomicConn != null) {
        if (atomicConn.get() == currentMonitoring && currentMonitoring != null) {
          // Don't close the active monitoring connection.
          atomicConn.set(null, false);
        } else {
          atomicConn.clean();
        }
      }
    }
    this.nodeThreadsConnections.clear();
  }

  protected void closeNodeMonitors() {
    if (this.nodeExecutorService != null) {

      try (ResourceLock ignored = this.nodeExecutorLock.obtain()) {

        if (this.nodeExecutorService == null) {
          return;
        }

        if (!this.nodeExecutorService.isShutdown()) {
          this.nodeExecutorService.shutdown();
        }

        try {
          if (!this.nodeExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
            this.nodeExecutorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          // Do nothing.
        }

        this.nodeExecutorService = null;
      }
    }
  }

  protected void createNodeExecutorService() {
    try (ResourceLock ignored = this.nodeExecutorLock.obtain()) {
      this.nodeExecutorService = ExecutorFactory.newCachedThreadPool("node");
    }
  }

  protected MonitoringConnectionHandler createConnectionHandler() {
    return new AuroraMonitoringConnectionHandler(
        this.monitoringConnection,
        this.servicesContainer.getPluginService(),
        this.topologyUtils,
        this.properties,
        this.monitoringProperties,
        this::wakeUpMonitoringLoop);
  }

  /**
   * Wakes up the monitoring loop so it can process events (e.g., a completed connection upgrade)
   * immediately instead of waiting for the next refresh cycle.
   */
  protected void wakeUpMonitoringLoop() {
    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);
      this.requestToUpdateTopology.notifyAll();
    }
  }

  protected boolean isInPanicMode() {
    return this.monitoringConnection.get() == null;
  }

  /**
   * Filters the list of hosts that should be used for node monitoring during panic mode.
   * Subclasses can override this to restrict which hosts are eligible for connection attempts.
   *
   * @param hosts the full list of hosts from the topology
   * @return the filtered list of hosts to use for node monitoring
   */
  protected List<HostSpec> filterHostsForNodeMonitoring(final List<HostSpec> hosts) {
    return hosts;
  }

  protected Runnable getNodeMonitoringWorker(
      final HostSpec hostSpec,
      final @Nullable HostSpec writerHostSpec,
      final boolean someRegionsInaccessible) throws SQLException {
    FullServicesContainer newServiceContainer =
        ServiceUtility.getInstance().createMinimalServiceContainer(this.servicesContainer, this.properties);
    return new NodeMonitoringWorker(
        newServiceContainer,
        this,
        hostSpec,
        writerHostSpec,
        someRegionsInaccessible,
        this.logUnclosedConnections);
  }

  protected List<HostSpec> openAnyConnectionAndUpdateTopology() {
    if (this.monitoringConnection.get() == null) {

      Connection conn;

      // Open a new connection.
      try {
        conn = this.servicesContainer.getPluginService().forceConnect(this.initialHostSpec, this.monitoringProperties);
      } catch (SQLException ex) {
        return null;
      }

      if (this.monitoringConnection.compareAndSet(null, conn)) {
        LOGGER.finest(() -> Messages.get(
            "ClusterTopologyMonitorImpl.openedMonitoringConnection",
            new Object[] {this.initialHostSpec.getHost()}));

        boolean isWriter = false;
        try {
          isWriter = this.topologyUtils.isWriterInstance(this.monitoringConnection.get());
        } catch (SQLException ex) {
          // Do nothing - assume not a writer.
        }

        if (isWriter) {
          try {
            if (rdsHelper.isRdsInstance(this.initialHostSpec.getHost())) {
              this.writerHostSpec.set(this.initialHostSpec);
              this.lastKnownWriterHostSpec = this.initialHostSpec;
              LOGGER.finest(() -> Messages.get(
                  "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                  new Object[] {this.writerHostSpec.get().getHost()}));
            } else {
              final Pair<String, String> pair = this.servicesContainer.getPluginService().getDialect().getHostId(
                  this.monitoringConnection.get());
              if (pair != null) {
                HostSpec instanceTemplate = this.getInstanceTemplate(pair.getValue2(), this.monitoringConnection.get());
                HostSpec writerHost = this.topologyUtils.createHost(
                    pair.getValue1(), pair.getValue2(), true, 0, null, this.initialHostSpec, instanceTemplate);
                this.writerHostSpec.set(writerHost);
                this.lastKnownWriterHostSpec = writerHost;
                LOGGER.finest(() -> Messages.get(
                    "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                    new Object[] {this.writerHostSpec.get().getHost()}));
              }
            }
          } catch (SQLException ex) {
            // Do nothing.
          }
        }

        // Let the connection handler know about this connection.
        this.getConnectionHandler().acceptConnection(conn, isWriter, this.initialHostSpec);

      } else {
        // The monitoring connection has already been detected by another thread. We close the new connection since it
        // is not needed anymore.
        this.closeConnection(conn);
      }
    }

    final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());

    if (hosts == null) {
      // Attempt to fetch topology failed. There might be something wrong with the connection, so we close it here.
      this.monitoringConnection.set(null);
      this.writerHostSpec.set(null);
    }

    return hosts;
  }

  // Note: even though the parameters are not used here, they may be used in subclasses overriding this method.
  protected HostSpec getInstanceTemplate(String nodeId, Connection connection) throws SQLException {
    return this.instanceTemplate;
  }

  protected void closeConnection(final @Nullable Connection connection) {
    try {
      if (connection != null && !connection.isClosed()) {
        try {
          connection.setNetworkTimeout(networkTimeoutExecutor, closeConnectionNetworkTimeoutMs);
        } catch (SQLException ex) {
          // Do nothing.
        }
        connection.close();
      }
    } catch (final SQLException ex) {
      // Do nothing.
    }
  }

  // Sleep method that can be easily interrupted.
  protected void delay(boolean useHighRefreshRate) throws InterruptedException {
    if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() < this.highRefreshRateEndTimeNano) {
      useHighRefreshRate = true;
    }

    if (this.requestToUpdateTopology.get()) {
      useHighRefreshRate = true;
    }

    long start = System.nanoTime();
    long end = start + (useHighRefreshRate ? this.highRefreshRateNano : this.refreshRateNano);
    do {
      synchronized (this.requestToUpdateTopology) {
        this.requestToUpdateTopology.wait(50);
      }
    } while (!this.requestToUpdateTopology.get() && System.nanoTime() < end && !this.stop.get());
  }

  protected @Nullable List<HostSpec> fetchTopologyAndUpdateCache(final Connection connection) {
    if (connection == null) {
      return null;
    }

    try {
      final List<HostSpec> hosts = this.queryForTopology(connection);
      if (!Utils.isNullOrEmpty(hosts)) {
        this.updateTopologyCache(hosts);
      }
      return hosts;
    } catch (SQLException ex) {
      // Ignore exceptions related to network. Log other exceptions.
      if (!this.servicesContainer.getPluginService().isNetworkException(ex,
          this.servicesContainer.getPluginService().getTargetDriverDialect())) {
        LOGGER.log(Level.FINEST, ex,
            () -> Messages.get("ClusterTopologyMonitorImpl.errorFetchingTopology", new Object[]{ex}));
      }
    }

    return null;
  }

  protected List<HostSpec> queryForTopology(Connection connection) throws SQLException {
    return this.topologyUtils.queryForTopology(connection, this.initialHostSpec, this.instanceTemplate);
  }

  protected void updateHostsAvailability(final @NonNull List<HostSpec> hosts) {
    if (Utils.isNullOrEmpty(hosts)) {
      return;
    }
    for (HostSpec host : hosts) {
      host.setAvailability(this.readerTopologiesById.containsKey(host.getHostId())
          ? HostAvailability.AVAILABLE
          : HostAvailability.NOT_AVAILABLE);
    }
  }

  protected void updateTopologyCache(final @NonNull List<HostSpec> hosts) {
    synchronized (this.requestToUpdateTopology) {
      this.servicesContainer.getStorageService().set(this.clusterId, new Topology(hosts));
      synchronized (this.topologyUpdated) {
        this.requestToUpdateTopology.set(false);

        // Notify all threads that are waiting for a topology update.
        this.topologyUpdated.notifyAll();
      }
    }
  }

  protected void clearTopologyCache() {
    synchronized (this.requestToUpdateTopology) {
      this.servicesContainer.getStorageService().remove(Topology.class, this.clusterId);
      synchronized (this.topologyUpdated) {
        this.requestToUpdateTopology.set(false);

        // Notify all threads that are waiting for a topology update.
        this.topologyUpdated.notifyAll();
      }
    }
  }

  private static class NodeMonitoringWorker implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(NodeMonitoringWorker.class.getName());

    protected final FullServicesContainer servicesContainer;
    protected final ClusterTopologyMonitorImpl monitor;
    protected final HostSpec hostSpec;
    protected final @Nullable HostSpec writerHostSpec;
    /**
     * Snapshot of whether some AWS regions are inaccessible at the time this worker was created.
     * When {@code true}, this worker will signal panic-mode exit on observed writer changes
     * (since it may be the only way to detect a writer in an inaccessible region).
     */
    protected final boolean someRegionsInaccessible;
    protected boolean writerChanged = false;
    protected int connectionAttempts = 0;
    protected AtomicConnection connection;

    public NodeMonitoringWorker(
        final FullServicesContainer servicesContainer,
        final ClusterTopologyMonitorImpl monitor,
        final HostSpec hostSpec,
        final @Nullable HostSpec writerHostSpec,
        final boolean someRegionsInaccessible,
        final boolean logUnclosedConnections
    ) {
      this.servicesContainer = servicesContainer;
      this.monitor = monitor;
      this.hostSpec = hostSpec;
      this.writerHostSpec = writerHostSpec;
      this.someRegionsInaccessible = someRegionsInaccessible;
      this.connection = new AtomicConnection(this, logUnclosedConnections);
    }

    @Override
    public void run() {
      final long start = System.nanoTime();

      try {
        while (!this.monitor.nodeThreadsStop.get()) {

          if (this.connection.get() == null) {
            try {
              this.connection.set(
                  this.servicesContainer.getPluginService().forceConnect(
                      hostSpec, this.monitor.monitoringProperties));
              this.connectionAttempts = 0;
            } catch (SQLException ex) {
              // A problem occurred while connecting.
              if (this.servicesContainer.getPluginService().isNetworkException(
                  ex, this.servicesContainer.getPluginService().getTargetDriverDialect())) {
                // It's a network issue that's expected during a cluster failover.
                // We will try again on the next iteration.
                TimeUnit.MILLISECONDS.sleep(100);
                this.monitor.completedOneCycle.put(this.hostSpec.getHostId(), Boolean.TRUE);
                this.monitor.readerTopologiesById.remove(this.hostSpec.getHostId());
                continue;
              } else if (this.servicesContainer.getPluginService().isLoginException(
                    ex, this.servicesContainer.getPluginService().getTargetDriverDialect())) {
                // Something wrong with login credentials. We can't continue.
                throw new RuntimeException(ex);
              } else {
                // It might be some transient error. Let's try again.
                //  If the error repeats, we will try again after a longer delay.
                TimeUnit.MILLISECONDS.sleep(this.calculateBackoffWithJitter(this.connectionAttempts++));
                this.monitor.completedOneCycle.put(this.hostSpec.getHostId(), Boolean.TRUE);
                this.monitor.readerTopologiesById.remove(this.hostSpec.getHostId());
                continue;
              }
            }
          }

          if (this.connection.get() != null) {
            boolean isWriter = false;
            try {
              // Use topology metadata to check on the currently connected instance
              isWriter = this.monitor.topologyUtils.isWriterInstance(this.connection.get());
            } catch (SQLSyntaxErrorException ex) {
              LOGGER.severe(() -> Messages.get(
                  "NodeMonitoringThread.invalidWriterQuery",
                  new Object[] {ex.getMessage()}));
              throw new RuntimeException(ex);
            } catch (SQLException ex) {
              this.connection.set(null);
            }

            if (isWriter) {
              try {
                // Check if connected instance is read_only
                if (this.servicesContainer.getPluginService().getHostRole(this.connection.get()) != HostRole.WRITER) {
                  // The first connection after failover may be stale.
                  isWriter = false;
                }
              } catch (SQLException e) {
                // Invalid connection, retry.
                this.monitor.completedOneCycle.put(this.hostSpec.getHostId(), Boolean.TRUE);
                this.monitor.readerTopologiesById.remove(this.hostSpec.getHostId());
                continue;
              }
            }

            if (isWriter) {
              // Use compareAndSet to claim writer status atomically. Only the first thread to detect a writer succeeds.
              if (!this.monitor.nodeThreadsWriterHostSpec.compareAndSet(null, this.hostSpec)) {
                // The writer host has already been claimed by another node monitor.
                // Our connection will be handed off to the main loop in finally.
              } else {
                // Successfully marked this host as the writer.
                LOGGER.fine(() ->
                    Messages.get("NodeMonitoringThread.detectedWriter", new Object[] {this.hostSpec.getUrl()}));
                this.servicesContainer.getImportantEventService().registerEvent(
                    () -> Messages.get("NodeMonitoringThread.detectedWriter", new Object[] {this.hostSpec.getUrl()}));

                this.monitor.fetchTopologyAndUpdateCache(this.connection.get());
                this.hostSpec.setAvailability(HostAvailability.AVAILABLE);

                this.monitor.nodeThreadsStop.set(true);
                LOGGER.fine(() -> LogUtils.logTopology(this.monitor.getStoredHosts()));
              }
              return;

            } else if (this.connection.get() != null) {
              // This connection is a reader connection.
              if (this.monitor.nodeThreadsWriterHostSpec.get() == null) {
                // We can use this reader connection to update the topology while we wait for the writer connection to
                // be established.
                this.readerThreadFetchTopology(this.connection.get(), this.writerHostSpec);
              }
            }
          }

          this.monitor.completedOneCycle.put(this.hostSpec.getHostId(), Boolean.TRUE);
          TimeUnit.MILLISECONDS.sleep(100);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOGGER.log(Level.SEVERE, ex, () -> Messages.get("NodeMonitoringThread.unhandledException"));
        throw ex;
      } finally {
        this.monitor.completedOneCycle.put(this.hostSpec.getHostId(), Boolean.TRUE);
        this.monitor.readerTopologiesById.remove(this.hostSpec.getHostId());

        // Hand off any live connection to the main loop via a fresh AtomicConnection owned by the monitor.
        // We can't share `this.connection` directly: when this worker is cleaned up, the underlying connection
        // would be closed (or detached, leaking it). Wrapping in a new AtomicConnection separates ownership.
        final Connection liveConn = this.connection.get();
        if (liveConn != null && !this.monitor.stop.get() && !Thread.currentThread().isInterrupted()) {
          final AtomicConnection mapConn =
              new AtomicConnection(this.monitor, this.monitor.logUnclosedConnections);
          mapConn.set(liveConn);
          // Detach from local AtomicConnection without closing — ownership is now in the map.
          this.connection.set(null, false);
          final AtomicConnection previous = this.monitor.nodeThreadsConnections.put(this.hostSpec, mapConn);
          if (previous != null) {
            // Should not normally happen, but clean up any previous entry to avoid leaks.
            previous.clean();
          }
        }
        this.connection.clean();

        final long end = System.nanoTime();
        LOGGER.finest(() -> Messages.get(
            "NodeMonitoringThread.threadCompleted",
            new Object[] {TimeUnit.NANOSECONDS.toMillis(end - start)}));
      }
    }

    private void readerThreadFetchTopology(final Connection connection, final @Nullable HostSpec writerHostSpec) {
      if (connection == null) {
        return;
      }

      List<HostSpec> hosts;
      try {
        hosts = this.monitor.queryForTopology(connection);
        if (hosts == null) {
          return;
        }
      } catch (SQLException ex) {
        return;
      }

      // Share this topology so that the main monitoring thread can adjust the node monitoring threads.
      this.monitor.nodeThreadsLatestTopology.set(hosts);
      this.monitor.readerTopologiesById.put(this.hostSpec.getHostId(), hosts);

      if (this.writerChanged) {
        this.monitor.updateHostsAvailability(hosts);
        this.monitor.updateTopologyCache(hosts);
        LOGGER.finest(() -> LogUtils.logTopology(hosts));
        return;
      }

      final HostSpec latestWriterHostSpec = hosts.stream()
          .filter(x -> x.getRole() == HostRole.WRITER)
          .findFirst()
          .orElse(null);
      if (latestWriterHostSpec != null
          && writerHostSpec != null
          && !latestWriterHostSpec.getHostAndPort().equals(writerHostSpec.getHostAndPort())) {
        this.writerChanged = true;
        LOGGER.fine(() -> Messages.get(
            "NodeMonitoringThread.writerNodeChanged",
            new Object[] {writerHostSpec.getHost(), latestWriterHostSpec.getHost()}));

        // Update the topology cache and notify all waiting threads.
        this.monitor.updateHostsAvailability(hosts);
        this.monitor.updateTopologyCache(hosts);
        LOGGER.fine(() -> LogUtils.logTopology(hosts));

        // Signal the main loop that the writer has changed — but only when some regions are inaccessible
        // (e.g., GDB with a restricted accessibleRegions list). In that case, no node thread may be able to
        // reach the new writer to confirm it via isWriterInstance(), so reader-observed writer changes are the
        // only fast way to exit panic mode.
        // When all regions are accessible, we let the standard exit path run (the node thread connecting to the
        // new writer will call isWriterInstance() and report it directly), which is more reliable since it also
        // verifies a working connection to the writer.
        if (this.someRegionsInaccessible
            && this.monitor.nodeThreadsWriterHostSpec.compareAndSet(null, latestWriterHostSpec)) {
          LOGGER.finest(() -> Messages.get(
              "NodeMonitoringThread.writerChangeExitTriggered",
              new Object[] {latestWriterHostSpec.getHost()}));
          this.monitor.nodeThreadsStop.set(true);
        }
      }
    }

    private long calculateBackoffWithJitter(int attempt) {
      long backoff = INITIAL_BACKOFF_MS * Math.round(Math.pow(2, Math.min(attempt, 6)));
      backoff = Math.min(backoff, MAX_BACKOFF_MS);
      return Math.round(backoff * (0.5 + random.nextDouble() * 0.5));
    }
  }

  protected long getStableTopologiesDurationNano() {
    return STABLE_TOPOLOGIES_DURATION_NANO;
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("monitoringConnection",
        this.monitoringConnection != null ? this.monitoringConnection.get() : null));
    final HostSpec tmpWriterHostSpec = this.writerHostSpec.get();
    state.add(Pair.create("writerHostSpec",
        tmpWriterHostSpec != null ? tmpWriterHostSpec.toString() : null));
    state.add(Pair.create("refreshRateNano", this.refreshRateNano));
    state.add(Pair.create("highRefreshRateNano", this.highRefreshRateNano));
    PropertyUtils.addSnapshotState(state, "monitoringProperties", this.monitoringProperties);
    state.add(Pair.create("initialHostSpec", this.initialHostSpec != null ? this.initialHostSpec.toString() : null));
    state.add(Pair.create("instanceTemplate",
        this.instanceTemplate != null ? this.instanceTemplate.toString() : null));
    state.add(Pair.create("clusterId", this.clusterId));
    state.add(Pair.create("isInPanicMode", this.isInPanicMode()));
    if (this.connectionHandler != null) {
      state.add(Pair.create("connectionHandler", this.connectionHandler.getSnapshotState()));
    }
    return state;
  }
}
