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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.ServiceUtility;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.events.Event;
import software.amazon.jdbc.util.events.MonitorResetEvent;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;

public class ClusterTopologyMonitorImpl extends AbstractMonitor implements ClusterTopologyMonitor {

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
  protected static final long ignoreTopologyRequestNano = TimeUnit.SECONDS.toNanos(10);

  private static final long INITIAL_BACKOFF_MS = 100;
  private static final long MAX_BACKOFF_MS = 10000;
  private static final Random random = new Random();

  protected final AtomicReference<HostSpec> writerHostSpec = new AtomicReference<>(null);
  protected AtomicConnection monitoringConnection;

  protected final Object topologyUpdated = new Object();
  protected final AtomicBoolean requestToUpdateTopology = new AtomicBoolean(false);
  protected final AtomicLong ignoreNewTopologyRequestsEndTimeNano = new AtomicLong(-1);
  protected final ConcurrentHashMap<String, Boolean> submittedNodes = new ConcurrentHashMap<>();

  protected final ResourceLock nodeExecutorLock = new ResourceLock();
  protected final AtomicBoolean nodeThreadsStop = new AtomicBoolean(false);
  protected AtomicConnection nodeThreadsWriterConnection;
  protected final AtomicReference<HostSpec> nodeThreadsWriterHostSpec = new AtomicReference<>(null);
  protected final AtomicConnection nodeThreadsReaderConnection;
  protected final AtomicReference<List<HostSpec>> nodeThreadsLatestTopology = new AtomicReference<>(null);

  protected final Map<String, List<HostSpec>> readerTopologiesById = new ConcurrentHashMap<>();
  protected final long stableTopologiesDurationNano = TimeUnit.SECONDS.toNanos(90);
  protected long stableTopologiesStartNano;

  protected final long refreshRateNano;
  protected final long highRefreshRateNano;
  protected final TopologyUtils topologyUtils;
  protected final FullServicesContainer servicesContainer;
  protected final Properties properties;
  protected final Properties monitoringProperties;
  protected final HostSpec initialHostSpec;
  protected final HostSpec instanceTemplate;

  protected ExecutorService nodeExecutorService = null;
  protected boolean isVerifiedWriterConnection = false;
  protected long highRefreshRateEndTimeNano = 0;
  protected String clusterId;
  protected boolean logUnclosedConnections = false;

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
    this.nodeThreadsWriterConnection = new AtomicConnection(this, this.logUnclosedConnections);
    this.nodeThreadsReaderConnection = new AtomicConnection(this, this.logUnclosedConnections);

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

  @Override
  public boolean canDispose() {
    return true;
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean verifyTopology, final long timeoutMs)
      throws SQLException, TimeoutException {

    if (this.ignoreNewTopologyRequestsEndTimeNano.get() > 0
        && System.nanoTime() < this.ignoreNewTopologyRequestsEndTimeNano.get()) {

      // A previous failover event has completed recently.
      // We can use the results of it without triggering a new topology update.
      List<HostSpec> currentHosts = getStoredHosts();
      LOGGER.finest(() ->
          LogUtils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.ignoringTopologyRequest")));
      if (currentHosts != null) {
        return currentHosts;
      }
    }

    if (verifyTopology) {
      // Enter panic mode, which will verify the topology for us.
      this.monitoringConnection.set(null);
      this.isVerifiedWriterConnection = false;
    }

    return this.waitForTopologyUpdate(timeoutMs);
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

  private List<HostSpec> getStoredHosts() {
    Topology topology = this.servicesContainer.getStorageService().get(Topology.class, this.clusterId);
    return topology == null ? null : topology.getHosts();
  }

  @Override
  public void stop() {
    this.nodeThreadsStop.set(true);

    this.closeNodeMonitors();
    this.nodeThreadsWriterConnection.set(null);
    this.nodeThreadsReaderConnection.set(null);
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
    this.closeNodeMonitors();
    this.monitoringConnection.clean();
    this.nodeThreadsWriterConnection.clean();
    this.nodeThreadsReaderConnection.clean();
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

            if (hosts != null && !this.isVerifiedWriterConnection) {
              for (HostSpec hostSpec : hosts) {
                // A list is used to store the exception since lambdas require references to outer variables to be
                // final. This allows us to identify if an error occurred while creating the node monitoring worker.
                final List<Exception> exceptionList = new ArrayList<>();
                this.submittedNodes.computeIfAbsent(hostSpec.getHost(),
                    (key) -> {
                      final ExecutorService nodeExecutorServiceCopy = this.nodeExecutorService;
                      if (nodeExecutorServiceCopy != null) {
                        try {
                          this.nodeExecutorService.submit(
                              this.getNodeMonitoringWorker(hostSpec, this.writerHostSpec.get()));
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
            final Connection writerConnection = this.nodeThreadsWriterConnection.get();
            final HostSpec writerConnectionHostSpec = this.nodeThreadsWriterHostSpec.get();
            if (writerConnection != null && writerConnectionHostSpec != null) {
              LOGGER.finest(() -> Messages.get(
                      "ClusterTopologyMonitorImpl.writerPickedUpFromNodeMonitors",
                      new Object[] {writerConnectionHostSpec}));

              this.monitoringConnection.set(writerConnection);
              this.writerHostSpec.set(writerConnectionHostSpec);
              this.isVerifiedWriterConnection = true;
              this.highRefreshRateEndTimeNano = System.nanoTime() + highRefreshPeriodAfterPanicNano;

              // We verify the writer on initial connection and on failover, but we only want to ignore new topology
              // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
              // time to 0. Any future writer verifications will set it to a positive value.
              if (!this.ignoreNewTopologyRequestsEndTimeNano.compareAndSet(-1, 0)) {
                this.ignoreNewTopologyRequestsEndTimeNano.set(System.nanoTime() + ignoreTopologyRequestNano);
              }

              this.nodeThreadsStop.set(true);
              this.closeNodeMonitors();
              this.submittedNodes.clear();
              this.stableTopologiesStartNano = 0;
              this.readerTopologiesById.clear();

              continue;

            } else {
              // Update node monitors with the new instances in the topology
              List<HostSpec> hosts = this.nodeThreadsLatestTopology.get();
              if (hosts != null && !this.nodeThreadsStop.get()) {
                for (HostSpec hostSpec : hosts) {
                  // A list is used to store the exception since lambdas require references to outer variables to be
                  // final. This allows us to identify if an error occurred while creating the node monitoring worker.
                  final List<Exception> exceptionList = new ArrayList<>();
                  this.submittedNodes.computeIfAbsent(hostSpec.getHost(),
                      (key) -> {
                        try {
                          this.nodeExecutorService.submit(
                              this.getNodeMonitoringWorker(hostSpec, this.writerHostSpec.get()));
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

          checkForStableReaderTopologies();
          this.delay(true);
        } else {
          // We are in regular mode (not panic mode).
          if (!this.submittedNodes.isEmpty()) {
            this.closeNodeMonitors();
            this.submittedNodes.clear();
            this.stableTopologiesStartNano = 0;
            this.readerTopologiesById.clear();
          }

          final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
          if (hosts == null) {
            // Attempt to fetch topology failed, so we switch to panic mode.
            this.monitoringConnection.set(null);
            this.isVerifiedWriterConnection = false;
            this.writerHostSpec.set(null);
            continue;
          }

          if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() > this.highRefreshRateEndTimeNano) {
            this.highRefreshRateEndTimeNano = 0;
          }

          // We avoid logging the topology while using the high refresh rate because it is too noisy.
          if (this.highRefreshRateEndTimeNano == 0) {
            LOGGER.finest(() -> LogUtils.logTopology(getStoredHosts()));
          }

          this.delay(false);
        }

        if (this.ignoreNewTopologyRequestsEndTimeNano.get() > 0
            && System.nanoTime() > this.ignoreNewTopologyRequestsEndTimeNano.get()) {
          this.ignoreNewTopologyRequestsEndTimeNano.set(0);
        }
      }

    } catch (final InterruptedException intEx) {
      Thread.currentThread().interrupt();
    } catch (final Exception ex) {
      // This should not be reached.
      if (LOGGER.isLoggable(Level.FINEST)) {
        // We want to print the full trace stack of the exception.
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "ClusterTopologyMonitorImpl.exceptionDuringMonitoringStop",
                new Object[] {this.initialHostSpec.getHost()}),
            ex);
      }

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

    long numReaders = latestHosts.stream().filter(h -> h.getRole() == HostRole.READER).count();
    if (numReaders == 0 || numReaders != this.readerTopologiesById.size()) {
      // There are no readers, or we have not received topology information from every reader yet.
      this.stableTopologiesStartNano = 0;
      return;
    }

    List<HostSpec> readerTopology = this.readerTopologiesById.values().stream().findFirst().orElse(null);
    if (readerTopology == null) {
      // readerTopologiesById has been cleared since checking its size.
      this.stableTopologiesStartNano = 0;
      return;
    }

    if (this.readerTopologiesById.values().stream().distinct().count() != 1) {
      // The topologies detected by each reader do not match.
      this.stableTopologiesStartNano = 0;
      return;
    }

    // All reader topologies match.
    if (this.stableTopologiesStartNano == 0) {
      this.stableTopologiesStartNano = System.nanoTime();
    }

    if (System.nanoTime() > this.stableTopologiesStartNano + this.stableTopologiesDurationNano) {
      // Reader topologies have been consistent for stableTopologiesDurationNano, so the topology should be accurate.
      this.stableTopologiesStartNano = 0;
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.matchingReaderTopologies",
          new Object[]{TimeUnit.NANOSECONDS.toMillis(this.stableTopologiesDurationNano)}));
      updateTopologyCache(readerTopology);
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
    this.createNodeExecutorService();

    this.nodeThreadsWriterHostSpec.set(null);
    this.nodeThreadsLatestTopology.set(null);

    this.monitoringConnection.set(null);
    this.isVerifiedWriterConnection = false;
    this.writerHostSpec.set(null);
    this.highRefreshRateEndTimeNano = 0;
    this.requestToUpdateTopology.set(false);
    this.ignoreNewTopologyRequestsEndTimeNano.set(-1);
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
    if (this.monitoringConnection.get() != this.nodeThreadsWriterConnection.get()) {
      this.nodeThreadsWriterConnection.set(null);
    } else {
      // Both variables hold a reference to the same connection.
      // Reset connection reference without closing the current connection.
      this.nodeThreadsWriterConnection.set(null, false);
    }
    if (this.monitoringConnection.get() != this.nodeThreadsReaderConnection.get()) {
      this.nodeThreadsReaderConnection.set(null);
    } else {
      // Both variables hold a reference to the same connection.
      // Reset connection reference without closing the current connection.
      this.nodeThreadsReaderConnection.set(null, false);
    }
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
        this.nodeThreadConnectionCleanUp();
      }
    }
  }

  protected void createNodeExecutorService() {
    try (ResourceLock ignored = this.nodeExecutorLock.obtain()) {
      this.nodeExecutorService = ExecutorFactory.newCachedThreadPool("node");
    }
  }

  protected boolean isInPanicMode() {
    return this.monitoringConnection.get() == null
        || !this.isVerifiedWriterConnection;
  }

  protected Runnable getNodeMonitoringWorker(
      final HostSpec hostSpec, final @Nullable HostSpec writerHostSpec) throws SQLException {
    FullServicesContainer newServiceContainer =
        ServiceUtility.getInstance().createMinimalServiceContainer(this.servicesContainer, this.properties);
    return new NodeMonitoringWorker(
        newServiceContainer, this, hostSpec, writerHostSpec, this.logUnclosedConnections);
  }

  protected List<HostSpec> openAnyConnectionAndUpdateTopology() {
    boolean writerVerifiedByThisThread = false;
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

        try {
          if (this.topologyUtils.isWriterInstance(this.monitoringConnection.get())) {
            this.isVerifiedWriterConnection = true;
            writerVerifiedByThisThread = true;

            if (rdsHelper.isRdsInstance(this.initialHostSpec.getHost())) {
              this.writerHostSpec.set(this.initialHostSpec);
              LOGGER.finest(() -> Messages.get(
                      "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                      new Object[] {this.writerHostSpec.get().getHost()}));
            } else {
              final Pair<String, String> pair = this.topologyUtils.getInstanceId(this.monitoringConnection.get());
              if (pair != null) {
                HostSpec instanceTemplate = this.getInstanceTemplate(pair.getValue2(), this.monitoringConnection.get());
                HostSpec writerHost = this.topologyUtils.createHost(
                    pair.getValue1(), pair.getValue2(), true, 0, null, this.initialHostSpec, instanceTemplate);
                this.writerHostSpec.set(writerHost);
                LOGGER.finest(() -> Messages.get(
                    "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                    new Object[] {this.writerHostSpec.get().getHost()}));
              }
            }
          }
        } catch (SQLException ex) {
          // Do nothing.
        }

      } else {
        // The monitoring connection has already been detected by another thread. We close the new connection since it
        // is not needed anymore.
        this.closeConnection(conn);
      }
    }

    final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
    if (writerVerifiedByThisThread) {
      // We verify the writer on initial connection and on failover, but we only want to ignore new topology
      // requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
      // time to 0. Any future writer verifications will set it to a positive value.
      if (!this.ignoreNewTopologyRequestsEndTimeNano.compareAndSet(-1, 0)) {
        this.ignoreNewTopologyRequestsEndTimeNano.set(System.nanoTime() + ignoreTopologyRequestNano);
      }
    }

    if (hosts == null) {
      // Attempt to fetch topology failed. There might be something wrong with the connection, so we close it here.
      this.monitoringConnection.set(null);
      this.isVerifiedWriterConnection = false;
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
        if (LOGGER.isLoggable(Level.FINEST)) {
          LOGGER.log(Level.FINEST,
              Messages.get("ClusterTopologyMonitorImpl.errorFetchingTopology", new Object[]{ex}),
              ex);
        }
      }
    }

    return null;
  }

  protected List<HostSpec> queryForTopology(Connection connection) throws SQLException {
    return this.topologyUtils.queryForTopology(connection, this.initialHostSpec, this.instanceTemplate);
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
    protected boolean writerChanged = false;
    protected int connectionAttempts = 0;
    protected AtomicConnection connection;

    public NodeMonitoringWorker(
        final FullServicesContainer servicesContainer,
        final ClusterTopologyMonitorImpl monitor,
        final HostSpec hostSpec,
        final @Nullable HostSpec writerHostSpec,
        final boolean logUnclosedConnections
    ) {
      this.servicesContainer = servicesContainer;
      this.monitor = monitor;
      this.hostSpec = hostSpec;
      this.writerHostSpec = writerHostSpec;
      this.connection = new AtomicConnection(this, logUnclosedConnections);
    }

    @Override
    public void run() {
      boolean updateTopology = false;
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
                continue;
              } else if (this.servicesContainer.getPluginService().isLoginException(
                    ex, this.servicesContainer.getPluginService().getTargetDriverDialect())) {
                // Something wrong with login credentials. We can't continue.
                throw new RuntimeException(ex);
              } else {
                // It might be some transient error. Let's try again.
                //  If the error repeats, we will try again after a longer delay.
                TimeUnit.MILLISECONDS.sleep(this.calculateBackoffWithJitter(this.connectionAttempts++));
                continue;
              }
            }
          }

          if (this.connection.get() != null) {
            boolean isWriter = false;
            try {
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
                if (this.servicesContainer.getPluginService().getHostRole(this.connection.get()) != HostRole.WRITER) {
                  // The first connection after failover may be stale.
                  isWriter = false;
                }
              } catch (SQLException e) {
                // Invalid connection, retry.
                continue;
              }
            }

            if (isWriter) {
              // This prevents us from closing the connection in the finally block.
              if (!this.monitor.nodeThreadsWriterConnection.compareAndSet(null, this.connection.get())) {
                // The writer connection is already set up, probably by another node monitor.
                this.connection.set(null);
              } else {
                // Successfully updated the node monitor writer connection.
                LOGGER.fine(() ->
                    Messages.get("NodeMonitoringThread.detectedWriter", new Object[] {hostSpec.getUrl()}));
                // When nodeThreadsWriterConnection and nodeThreadsWriterHostSpec are both set, the topology monitor may
                // set ignoreNewTopologyRequestsEndTimeNano, in which case other threads will use the cached topology
                // for the ignore duration, so we need to update the topology before setting nodeThreadsWriterHostSpec.
                this.monitor.fetchTopologyAndUpdateCache(this.connection.get());
                this.monitor.nodeThreadsWriterHostSpec.set(hostSpec);

                // Connection is already assigned to this.monitor.nodeThreadsWriterConnection
                // so we need just to reset it for this.connection without closing it.
                this.connection.set(null, false);

                this.monitor.nodeThreadsStop.set(true);
                LOGGER.fine(() -> LogUtils.logTopology(this.monitor.getStoredHosts()));
              }
              return;

            } else if (this.connection.get() != null) {
              // This connection is a reader connection.
              if (this.monitor.nodeThreadsWriterConnection.get() == null) {
                // We can use this reader connection to update the topology while we wait for the writer connection to
                // be established.
                this.monitor.nodeThreadsReaderConnection.compareAndSet(null, this.connection.get());
                this.readerThreadFetchTopology(this.connection.get(), this.writerHostSpec);
              }
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        LOGGER.log(Level.SEVERE, ex, () -> Messages.get("NodeMonitoringThread.unhandledException"));
        throw ex;
      } finally {

        // Connection in this.connection may already be assigned to other variables.
        // In this case we just need to reset the JDBC connection without closing it.
        final Connection tempConnection = this.connection.get();
        if (tempConnection != null
            && (tempConnection == this.monitor.nodeThreadsWriterConnection.get()
              || tempConnection == this.monitor.nodeThreadsReaderConnection.get())) {
          this.connection.set(null, false);
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
        this.monitor.updateTopologyCache(hosts);
        LOGGER.fine(() -> LogUtils.logTopology(hosts));
      }
    }

    private long calculateBackoffWithJitter(int attempt) {
      long backoff = INITIAL_BACKOFF_MS * Math.round(Math.pow(2, Math.min(attempt, 6)));
      backoff = Math.min(backoff, MAX_BACKOFF_MS);
      return Math.round(backoff * (0.5 + random.nextDouble() * 0.5));
    }
  }
}
