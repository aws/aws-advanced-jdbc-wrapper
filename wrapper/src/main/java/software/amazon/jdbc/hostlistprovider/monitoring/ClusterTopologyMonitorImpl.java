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

package software.amazon.jdbc.hostlistprovider.monitoring;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.Topology;
import software.amazon.jdbc.hostlistprovider.TopologyUtils;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.ServiceUtility;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;
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

  protected final AtomicReference<HostSpec> writerHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<Connection> monitoringConnection = new AtomicReference<>(null);

  protected final Object topologyUpdated = new Object();
  protected final AtomicBoolean requestToUpdateTopology = new AtomicBoolean(false);
  protected final AtomicLong ignoreNewTopologyRequestsEndTimeNano = new AtomicLong(-1);
  protected final ConcurrentHashMap<String, Boolean> submittedNodes = new ConcurrentHashMap<>();

  protected final ReentrantLock nodeExecutorLock = new ReentrantLock();
  protected final AtomicBoolean nodeThreadsStop = new AtomicBoolean(false);
  protected final AtomicReference<Connection> nodeThreadsWriterConnection = new AtomicReference<>(null);
  protected final AtomicReference<HostSpec> nodeThreadsWriterHostSpec = new AtomicReference<>(null);
  protected final AtomicReference<Connection> nodeThreadsReaderConnection = new AtomicReference<>(null);
  protected final AtomicReference<List<HostSpec>> nodeThreadsLatestTopology = new AtomicReference<>(null);

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
  public List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException {

    if (this.ignoreNewTopologyRequestsEndTimeNano.get() > 0
        && System.nanoTime() < this.ignoreNewTopologyRequestsEndTimeNano.get()) {

      // A previous failover event has completed recently.
      // We can use the results of it without triggering a new topology update.
      List<HostSpec> currentHosts = getStoredHosts();
      LOGGER.finest(
          LogUtils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.ignoringTopologyRequest")));
      if (currentHosts != null) {
        return currentHosts;
      }
    }

    if (shouldVerifyWriter) {
      final Connection monitoringConnection = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.isVerifiedWriterConnection = false;
      this.closeConnection(monitoringConnection);
    }

    return this.waitTillTopologyGetsUpdated(timeoutMs);
  }

  @Override
  public List<HostSpec> forceRefresh(@Nullable Connection connection, final long timeoutMs)
      throws SQLException, TimeoutException {
    if (this.isVerifiedWriterConnection) {
      // Get the monitoring thread to refresh the topology using a verified connection.
      return this.waitTillTopologyGetsUpdated(timeoutMs);
    }

    // Otherwise, use the provided unverified connection to update the topology.
    return this.fetchTopologyAndUpdateCache(connection);
  }

  protected List<HostSpec> waitTillTopologyGetsUpdated(final long timeoutMs) throws TimeoutException {
    List<HostSpec> currentHosts = getStoredHosts();
    List<HostSpec> latestHosts;

    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);

      // Notify the monitoring thread, which may be sleeping, that topology should be refreshed immediately.
      this.requestToUpdateTopology.notifyAll();
    }

    if (timeoutMs == 0) {
      LOGGER.finest(LogUtils.logTopology(currentHosts, Messages.get("ClusterTopologyMonitorImpl.timeoutSetToZero")));
      return currentHosts;
    }

    final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);

    // Note that we are checking reference equality instead of value equality here. We will break out of the loop if
    // there is a new entry in the topology map, even if the value of the hosts in latestHosts is the same as
    // currentHosts.
    while (currentHosts == (latestHosts = getStoredHosts())
        && System.nanoTime() < end) {
      try {
        synchronized (this.topologyUpdated) {
          this.topologyUpdated.wait(1000);
        }
      } catch (InterruptedException ex) {
        LOGGER.fine(Messages.get("ClusterTopologyMonitorImpl.interrupted"));
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
    this.shutdownNodeExecutorService();

    // This code interrupts the waiting/sleeping cycle in the monitoring thread.
    synchronized (this.requestToUpdateTopology) {
      this.requestToUpdateTopology.set(true);
      this.requestToUpdateTopology.notifyAll();
    }

    super.stop();
  }

  @Override
  public void close() {
    this.closeConnection(this.monitoringConnection.get());
    this.closeConnection(this.nodeThreadsWriterConnection.get());
    this.closeConnection(this.nodeThreadsReaderConnection.get());
  }

  @Override
  public void monitor() throws Exception {
    try {
      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.startMonitoringThread",
          new Object[] {this.clusterId, this.initialHostSpec.getHost()}));

      while (!this.stop.get() && !Thread.currentThread().isInterrupted()) {
        this.lastActivityTimestampNanos.set(System.nanoTime());

        if (this.isInPanicMode()) {

          if (this.submittedNodes.isEmpty()) {
            LOGGER.finest(Messages.get("ClusterTopologyMonitorImpl.startingNodeMonitoringThreads"));

            // Start node monitors.
            this.nodeThreadsStop.set(false);
            this.nodeThreadsWriterConnection.set(null);
            this.nodeThreadsReaderConnection.set(null);
            this.nodeThreadsWriterHostSpec.set(null);
            this.nodeThreadsLatestTopology.set(null);

            List<HostSpec> hosts = getStoredHosts();
            if (hosts == null) {
              // Use any available connection to get the topology.
              hosts = this.openAnyConnectionAndUpdateTopology();
            }

            this.shutdownNodeExecutorService();
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
              LOGGER.finest(Messages.get(
                  "ClusterTopologyMonitorImpl.writerPickedUpFromNodeMonitors",
                  new Object[] {writerConnectionHostSpec}));

              this.closeConnection(this.monitoringConnection.get());
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
              this.shutdownNodeExecutorService();
              this.submittedNodes.clear();

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

          this.delay(true);

        } else {
          // We are in regular mode (not panic mode).
          if (!this.submittedNodes.isEmpty()) {
            this.shutdownNodeExecutorService();
            this.submittedNodes.clear();
          }

          final List<HostSpec> hosts = this.fetchTopologyAndUpdateCache(this.monitoringConnection.get());
          if (hosts == null) {
            // Attempt to fetch topology failed, so we switch to panic mode.
            Connection conn = this.monitoringConnection.get();
            this.monitoringConnection.set(null);
            this.isVerifiedWriterConnection = false;
            this.closeConnection(conn);
            continue;
          }

          if (this.highRefreshRateEndTimeNano > 0 && System.nanoTime() > this.highRefreshRateEndTimeNano) {
            this.highRefreshRateEndTimeNano = 0;
          }

          // We avoid logging the topology while using the high refresh rate because it is too noisy.
          if (this.highRefreshRateEndTimeNano == 0) {
            LOGGER.finest(LogUtils.logTopology(getStoredHosts()));
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
      this.shutdownNodeExecutorService();

      final Connection conn = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.closeConnection(conn);

      LOGGER.finest(() -> Messages.get(
          "ClusterTopologyMonitorImpl.stopMonitoringThread",
          new Object[] {this.initialHostSpec.getHost()}));
    }
  }

  protected void shutdownNodeExecutorService() {
    if (this.nodeExecutorService != null) {

      this.nodeExecutorLock.lock();
      try {

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
      } finally {
        this.nodeExecutorLock.unlock();
      }
    }
  }

  protected void createNodeExecutorService() {
    this.nodeExecutorLock.lock();
    try {
      this.nodeExecutorService = ExecutorFactory.newCachedThreadPool("node");
    } finally {
      this.nodeExecutorLock.unlock();
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
    return new NodeMonitoringWorker(newServiceContainer, this, hostSpec, writerHostSpec);
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
              LOGGER.finest(Messages.get(
                  "ClusterTopologyMonitorImpl.writerMonitoringConnection",
                  new Object[] {this.writerHostSpec.get().getHost()}));
            } else {
              final Pair<String, String> pair = this.topologyUtils.getInstanceId(this.monitoringConnection.get());
              if (pair != null) {
                HostSpec instanceTemplate = this.getinstanceTemplate(pair.getValue2(), this.monitoringConnection.get());
                HostSpec writerHost = this.topologyUtils.createHost(
                    pair.getValue1(), pair.getValue2(), true, 0, null, this.initialHostSpec, instanceTemplate);
                this.writerHostSpec.set(writerHost);
                LOGGER.finest(Messages.get(
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
      Connection connToClose = this.monitoringConnection.get();
      this.monitoringConnection.set(null);
      this.closeConnection(connToClose);
      this.isVerifiedWriterConnection = false;
    }

    return hosts;
  }

  protected HostSpec getinstanceTemplate(String nodeId, Connection connection) throws SQLException {
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
      LOGGER.finest(Messages.get("ClusterTopologyMonitorImpl.errorFetchingTopology", new Object[] {ex}));
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

  private static class NodeMonitoringWorker implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(NodeMonitoringWorker.class.getName());

    protected final FullServicesContainer servicesContainer;
    protected final ClusterTopologyMonitorImpl monitor;
    protected final HostSpec hostSpec;
    protected final @Nullable HostSpec writerHostSpec;
    protected boolean writerChanged = false;

    public NodeMonitoringWorker(
        final FullServicesContainer servicesContainer,
        final ClusterTopologyMonitorImpl monitor,
        final HostSpec hostSpec,
        final @Nullable HostSpec writerHostSpec
    ) {
      this.servicesContainer = servicesContainer;
      this.monitor = monitor;
      this.hostSpec = hostSpec;
      this.writerHostSpec = writerHostSpec;
    }

    @Override
    public void run() {
      Connection connection = null;
      boolean updateTopology = false;

      final long start = System.nanoTime();
      try {
        while (!this.monitor.nodeThreadsStop.get()) {

          if (connection == null) {

            try {
              connection = this.servicesContainer.getPluginService().forceConnect(
                  hostSpec, this.monitor.monitoringProperties);
            } catch (SQLException ex) {
              // A problem occurred while connecting. We will try again on the next iteration.
              TimeUnit.MILLISECONDS.sleep(100);
              continue;
            }
          }

          if (connection != null) {

            boolean isWriter = false;
            try {
              isWriter = this.monitor.topologyUtils.isWriterInstance(connection);
            } catch (SQLSyntaxErrorException ex) {
              LOGGER.severe(() -> Messages.get(
                  "NodeMonitoringThread.invalidWriterQuery",
                  new Object[] {ex.getMessage()}));
              throw new RuntimeException(ex);

            } catch (SQLException ex) {
              this.monitor.closeConnection(connection);
              connection = null;
            }

            if (isWriter) {
              try {
                if (this.servicesContainer.getPluginService().getHostRole(connection) != HostRole.WRITER) {
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
              if (!this.monitor.nodeThreadsWriterConnection.compareAndSet(null, connection)) {
                // The writer connection is already set up, probably by another node monitor.
                this.monitor.closeConnection(connection);
              } else {
                // Successfully updated the node monitor writer connection.
                LOGGER.fine(Messages.get("NodeMonitoringThread.detectedWriter", new Object[] {hostSpec.getUrl()}));
                // When nodeThreadsWriterConnection and nodeThreadsWriterHostSpec are both set, the topology monitor may
                // set ignoreNewTopologyRequestsEndTimeNano, in which case other threads will use the cached topology
                // for the ignore duration, so we need to update the topology before setting nodeThreadsWriterHostSpec.
                this.monitor.fetchTopologyAndUpdateCache(connection);
                this.monitor.nodeThreadsWriterHostSpec.set(hostSpec);
                this.monitor.nodeThreadsStop.set(true);
                LOGGER.fine(LogUtils.logTopology(this.monitor.getStoredHosts()));
              }

              // We set the connection to null to prevent the finally block from closing nodeThreadsWriterConnection.
              connection = null;
              return;
            } else if (connection != null) {
              // This connection is a reader connection.
              if (this.monitor.nodeThreadsWriterConnection.get() == null) {
                // We can use this reader connection to update the topology while we wait for the writer connection to
                // be established.
                if (updateTopology) {
                  this.readerThreadFetchTopology(connection, this.writerHostSpec);
                } else if (this.monitor.nodeThreadsReaderConnection.get() == null) {
                  if (this.monitor.nodeThreadsReaderConnection.compareAndSet(null, connection)) {
                    // Use this connection to update the topology.
                    updateTopology = true;
                    this.readerThreadFetchTopology(connection, this.writerHostSpec);
                  }
                }
              }
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } finally {
        this.monitor.closeConnection(connection);
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
        hosts = this.monitor.topologyUtils.queryForTopology(
            connection, this.monitor.initialHostSpec, this.monitor.instanceTemplate);
        if (hosts == null) {
          return;
        }
      } catch (SQLException ex) {
        return;
      }

      // Share this topology so that the main monitoring thread can adjust the node monitoring threads.
      this.monitor.nodeThreadsLatestTopology.set(hosts);

      if (this.writerChanged) {
        this.monitor.updateTopologyCache(hosts);
        LOGGER.finest(LogUtils.logTopology(hosts));
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
        LOGGER.fine(LogUtils.logTopology(hosts));
      }
    }
  }
}
