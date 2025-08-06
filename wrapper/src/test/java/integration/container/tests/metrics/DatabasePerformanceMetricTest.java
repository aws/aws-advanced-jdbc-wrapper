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

package integration.container.tests.metrics;

import static integration.DatabaseEngineDeployment.AURORA;
import static integration.DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER;
import static integration.container.TestDriverProvider.checkClusterHealth;
import static integration.container.TestDriverProvider.rebootAllClusterInstances;
import static integration.container.TestDriverProvider.rebootCluster;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.fail;
import static software.amazon.jdbc.Driver.clearCaches;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.vandermeer.asciitable.AT_Cell;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;
import de.vandermeer.skb.interfaces.document.TableRowType;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverMode;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.Pair;

@TestMethodOrder(MethodOrderer.MethodName.class)
@EnableOnTestFeature(TestEnvironmentFeatures.RUN_DB_METRICS_ONLY)
@EnableOnDatabaseEngineDeployment({RDS_MULTI_AZ_CLUSTER, AURORA})
@EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
@EnableOnNumOfInstances(min = 2)
@Order(21)
public class DatabasePerformanceMetricTest {

  private static final Logger LOGGER = Logger.getLogger(DatabasePerformanceMetricTest.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static final int NUM_OF_ITERATIONS = 10;
  private static final long STOP_IF_NO_CHANGES_FOR_LAST_SECONDS = 180;
  private static final long MULTIAZ_STOP_IF_NO_CHANGES_FOR_LAST_SECONDS = TimeUnit.MINUTES.toSeconds(20);
  private static final String NOT_ACCESSIBLE = "#####";
  private static final String BLANK_TOPOLOGY = "(blank)";
  private static final long DIRECT_CONNECT_TIMEOUT_MS = 1000;
  private static final long DIRECT_SOCKET_TIMEOUT_MS = 1000;
  private static final long WRAPPER_CONNECT_TIMEOUT_MS = 10000;
  private static final long WRAPPER_SOCKET_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long TOPOLOGY_CONNECT_TIMEOUT_MS = 1000;
  private static final long TOPOLOGY_SOCKET_TIMEOUT_MS = 1000;
  private static final long EFM_CONNECT_TIMEOUT_MS = 1000;
  private static final long EFM_SOCKET_TIMEOUT_MS = 1000;
  private static final long EFM_DETECTION_TIME_MS = 0;
  private static final long EFM_DETECTION_INTERVAL_TIME_MS = 1000;
  private static final long EFM_DETECTION_COUNT = 1;

  private final ConcurrentLinkedDeque<Throwable> unhandledExceptions = new ConcurrentLinkedDeque<>();
  private final ConcurrentLinkedDeque<TopologyEventHolder> topologyEvents = new ConcurrentLinkedDeque<>();
  private final ConcurrentLinkedDeque<FailoverResult> failoverResults = new ConcurrentLinkedDeque<>();
  private final AtomicLong failoverTriggerTimeNano = new AtomicLong();
  private final AtomicReference<Instant> failoverTriggerTimestamp = new AtomicReference<>();

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void testCollectMetrics() {
    final Runs runs = new Runs();
    runs.iterations = new ArrayList<>();
    runs.numOfIterations = 0;
    runs.numOfTopologyFalsePositive = 0;
    runs.numOfTopologyOldWriterUnavailable = 0;
    runs.start = Instant.now();
    runs.engineVersion = TestEnvironment.getCurrent().getInfo().getDatabaseEngineVersion();

    final Pair<String, String> awsDriver = auroraUtil.getAwsDriverNameAndVersion();
    runs.awsDriverName = awsDriver.getValue1();
    runs.awsDriverVersion = awsDriver.getValue2();

    final Pair<String, String> targetDriver = auroraUtil.getTargetDriverNameAndVersion();
    runs.targetDriverName = targetDriver.getValue1();
    runs.targetDriverVersion = targetDriver.getValue2();

    Driver.releaseResources();

    try {
      for (int i = 1; i <= NUM_OF_ITERATIONS; i++) {
        final RunData runData = new RunData();
        try {
          this.collectMetrics(i, runData);

        } catch (Exception ex) {
          LOGGER.log(Level.SEVERE, "Unhandled error", ex);
          runData.topologyError = "Unhandled: " + ex;
        }

        runs.iterations.add(runData);

        try {
          this.waitForClusterRecover();
        } catch (Exception ex) {
          LOGGER.finest("Cluster recovery has failed. " + ex);
          break;
        }
      }
    } finally {
      runs.numOfIterations = runs.iterations.size();
      runs.numOfTopologyFalsePositive =
          (int) runs.iterations.stream().filter(x -> x.topologyFalsePositive).count();
      runs.numOfTopologyOldWriterUnavailable =
          (int) runs.iterations.stream().filter(x -> x.topologyOldWriterUnavailable).count();

      this.storeJsonMetrics(runs);
    }
  }

  public void waitForClusterRecover() throws InterruptedException {
    final DatabaseEngineDeployment deployment =
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment();

    clearCaches();

    // Try 3 times to check cluster health status
    int remainingTries = 3;
    boolean success = false;
    while (remainingTries-- > 0 && !success) {
      try {
        checkClusterHealth(true);
        success = true;
      } catch (Exception ex) {
        // Nothing we can do other than to reboot a cluster and hope it gets back in a better shape.
        switch (deployment) {
          case AURORA:
            rebootAllClusterInstances();
            break;
          case RDS_MULTI_AZ_CLUSTER:
            rebootCluster();
            break;
          default:
            throw new RuntimeException("Unsupported deployment " + deployment);
        }
        LOGGER.finest("Remaining attempts: " + remainingTries);
      }
    }

    if (!success) {
      LOGGER.finest("Cluster " + TestEnvironment.getCurrent().getInfo().getRdsDbName() + " is not healthy.");
      throw new RuntimeException("Not healthy.");
    }

    LOGGER.finest("Cluster " + TestEnvironment.getCurrent().getInfo().getRdsDbName() + " is healthy.");
  }


  public void collectMetrics(int iterationNum, final RunData runData) throws InterruptedException {

    runData.topologyError = null;
    runData.topologyRows = new ArrayList<>();
    runData.startTime = Instant.now();
    runData.topologyFalsePositive = false;
    runData.topologyOldWriterUnavailable = false;

    this.topologyEvents.clear();
    this.failoverResults.clear();
    this.unhandledExceptions.clear();

    final ArrayList<Thread> threads = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicReference<CountDownLatch> startLatchAtomic = new AtomicReference<>(null);
    final AtomicReference<CountDownLatch> finishLatchAtomic = new AtomicReference<>(null);
    int threadCount = 0;
    int threadFinishCount = 0;
    int clusterId = 101;

    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    final String dbName = info.getDatabaseInfo().getDefaultDbName();

    for (TestInstanceInfo hostInfo : info.getDatabaseInfo().getInstances()) {
      threads.add(this.getDirectTopologyMonitoringThread(
          info.getRequest().getDatabaseEngine(), info.getRequest().getDatabaseEngineDeployment(),
          hostInfo.getInstanceId(), hostInfo.getHost(), hostInfo.getPort(), dbName,
          startLatchAtomic, stop, finishLatchAtomic, topologyEvents));
      threadCount++;
      threadFinishCount++;

      threads.add(this.getFailoverMonitoringThread(
          info.getRequest().getDatabaseEngine(), info.getRequest().getDatabaseEngineDeployment(),
          hostInfo.getInstanceId(), hostInfo.getHost(), hostInfo.getPort(), dbName,
          FailoverMode.STRICT_WRITER, clusterId++,
          startLatchAtomic, stop, finishLatchAtomic, failoverResults));
      threadCount++;
      threadFinishCount++;

      threads.add(this.getFailoverMonitoringThread(
          info.getRequest().getDatabaseEngine(), info.getRequest().getDatabaseEngineDeployment(),
          hostInfo.getInstanceId(), hostInfo.getHost(), hostInfo.getPort(), dbName,
          FailoverMode.STRICT_READER, clusterId++,
          startLatchAtomic, stop, finishLatchAtomic, failoverResults));
      threadCount++;
      threadFinishCount++;
    }

    threads.add(this.getTriggerFailoverThread(
        info.getRdsDbName(),
        startLatchAtomic,
        finishLatchAtomic,
        this.failoverTriggerTimeNano,
        this.failoverTriggerTimestamp));
    threadCount++;
    threadFinishCount++;

    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final CountDownLatch finishLatch = new CountDownLatch(threadFinishCount);
    startLatchAtomic.set(startLatch);
    finishLatchAtomic.set(finishLatch);

    threads.forEach(Thread::start);
    LOGGER.finest("All threads started.");

    finishLatch.await(10, TimeUnit.MINUTES);
    LOGGER.finest("All threads completed.");

    LOGGER.finest("Stopping all threads...");
    stop.set(true);
    TimeUnit.SECONDS.sleep(5);
    LOGGER.finest("Interrupting all threads...");
    threads.forEach(Thread::interrupt);
    TimeUnit.SECONDS.sleep(5);

    // Report results
    LOGGER.finest("Test is over.");
    this.processMetrics(runData);
    this.printTopologyMetrics(iterationNum, runData);
    this.printFailoverMetrics(iterationNum, runData);

    if (!this.unhandledExceptions.isEmpty()) {
      this.logUnhandledExceptions();
      fail("There are unhandled exceptions.");
    }

    LOGGER.finest("Completed");
  }

  private Thread getDirectTopologyMonitoringThread(
      final DatabaseEngine databaseEngine,
      final DatabaseEngineDeployment deployment,
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final ConcurrentLinkedDeque<TopologyEventHolder> results) {
    return new Thread(() -> {

      Connection conn = null;

      try {

        long threadTimeoutSec = (deployment == RDS_MULTI_AZ_CLUSTER
            ? MULTIAZ_STOP_IF_NO_CHANGES_FOR_LAST_SECONDS
            : STOP_IF_NO_CHANGES_FOR_LAST_SECONDS);

        final Properties props = ConnectionStringHelper.getDefaultProperties();
        PropertyDefinition.CONNECT_TIMEOUT.set(props, String.valueOf(DIRECT_CONNECT_TIMEOUT_MS));
        PropertyDefinition.SOCKET_TIMEOUT.set(props, String.valueOf(DIRECT_SOCKET_TIMEOUT_MS));

        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectTopology @ %s] connection opened", hostId));

        Boolean readOnly = this.getReadOnly(databaseEngine, deployment, conn);
        List<ClusterInstanceInfo> topologyHosts = this.getClusterInstances(databaseEngine, deployment, conn);
        TopologyEventHolder lastEvent = new TopologyEventHolder(
            hostId, Instant.now(), System.nanoTime(), true, topologyHosts, readOnly);
        long lastEventChangeTimeNano = 0;
        results.add(lastEvent);

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        lastEvent = null;

        LOGGER.finest(String.format("[DirectTopology @ %s] Starting topology and connectivity monitoring.", hostId));

        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);

        while (!stop.get() && System.nanoTime() < endTime) {
          TopologyEventHolder currentEvent = null;

          if (conn == null) {
            try {
              conn = DriverManager.getConnection(ConnectionStringHelper.getUrl(host, port, dbName), props);

            } catch (SQLException sqlEx) {
              // ignore, try to connect again
            }

            if (conn != null) {
              LOGGER.finest(String.format("[DirectTopology @ %s] connection re-opened", hostId));
            } else {
              currentEvent = new TopologyEventHolder(hostId, Instant.now(), System.nanoTime(), false, null, null);
            }
          }

          if (conn != null) {
            try {
              readOnly = this.getReadOnly(databaseEngine, deployment, conn);
              topologyHosts = this.getClusterInstances(databaseEngine, deployment, conn);
              currentEvent = new TopologyEventHolder(
                  hostId, Instant.now(), System.nanoTime(), true, topologyHosts, readOnly);

            } catch (SQLException throwable) {
              currentEvent = new TopologyEventHolder(
                  hostId, Instant.now(), System.nanoTime(), false, null, null);
              LOGGER.finest(String.format("[DirectTopology @ %s] thread exception: %s", hostId, throwable));
              this.closeConnection(conn);
              conn = null;
            }
          }

          if (lastEvent == null || !lastEvent.equals(currentEvent)) {
            lastEvent = currentEvent;
            lastEventChangeTimeNano = System.nanoTime();
            results.add(currentEvent);
            TopologyEventHolder finalCurrentEvent = currentEvent;
            LOGGER.finest(() -> String.format(
                "[DirectTopology @ %s] topology or availability changed to: %s", hostId, finalCurrentEvent));
          }

          TimeUnit.MILLISECONDS.sleep(10);

          if (lastEventChangeTimeNano != 0
              && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - lastEventChangeTimeNano)
                >= threadTimeoutSec) {
            LOGGER.finest(() -> String.format(
                "[DirectTopology @ %s] no changes in topology or availability for the last %d sec",
                hostId, threadTimeoutSec));
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[DirectTopology @ %s] thread unhandled exception: ", hostId),
            exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[DirectTopology @ %s] thread is completed.", hostId));
      }
    });
  }

  private Thread getFailoverMonitoringThread(
      final DatabaseEngine databaseEngine,
      final DatabaseEngineDeployment deployment,
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final FailoverMode failoverMode,
      final int clusterId,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final ConcurrentLinkedDeque<FailoverResult> results) {
    return new Thread(() -> {

      Connection conn = null;

      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();

        props.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "false");
        props.setProperty(PropertyDefinition.TELEMETRY_TRACES_BACKEND.name, "none");
        props.setProperty(PropertyDefinition.TELEMETRY_METRICS_BACKEND.name, "none");

        props.setProperty("clusterId", String.valueOf(clusterId));
        //PropertyDefinition.PLUGINS.set(props, "efm2,failover2");
        PropertyDefinition.PLUGINS.set(props, "failover2");
        PropertyDefinition.CONNECT_TIMEOUT.set(props, String.valueOf(WRAPPER_CONNECT_TIMEOUT_MS));

        // Socket timeout also helps to abort a long-running query if failover isn't occurred.
        PropertyDefinition.SOCKET_TIMEOUT.set(props, String.valueOf(WRAPPER_SOCKET_TIMEOUT_MS));

        // Failover2 settings
        props.setProperty("failoverMode",
            failoverMode == FailoverMode.STRICT_WRITER ? "strict-writer" : "strict-reader");
        props.setProperty("topology-monitoring-" + PropertyDefinition.CONNECT_TIMEOUT.name,
            String.valueOf(TOPOLOGY_CONNECT_TIMEOUT_MS));
        props.setProperty("topology-monitoring-" + PropertyDefinition.SOCKET_TIMEOUT.name,
            String.valueOf(TOPOLOGY_SOCKET_TIMEOUT_MS));

        // EFM2 settings
        props.setProperty("monitoring-" + PropertyDefinition.CONNECT_TIMEOUT.name,
            String.valueOf(EFM_CONNECT_TIMEOUT_MS));
        props.setProperty("monitoring-" + PropertyDefinition.SOCKET_TIMEOUT.name,
            String.valueOf(EFM_SOCKET_TIMEOUT_MS));
        props.setProperty("failureDetectionTime", String.valueOf(EFM_DETECTION_TIME_MS));
        props.setProperty("failureDetectionInterval", String.valueOf(EFM_DETECTION_INTERVAL_TIME_MS));
        props.setProperty("failureDetectionCount", String.valueOf(EFM_DETECTION_COUNT));

        conn = openConnectionWithRetry(ConnectionStringHelper.getWrapperUrl(host, port, dbName), props);
        LOGGER.finest(String.format(
            "[FailoverMonitor @ %s %s] connection opened, clusterId: %d",
            hostId, failoverMode, clusterId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[FailoverMonitor @ %s %s] Starting topology and connectivity monitoring.",
            hostId, failoverMode));

        try {
          try (final Statement statement = conn.createStatement();
              final ResultSet rs = statement.executeQuery(this.getLongRunningQuery(databaseEngine, deployment))) {
            // do nothing
            LOGGER.warning(String.format("[FailoverMonitor @ %s %s] Long-running query has completed! rs: %s",
                hostId, failoverMode, rs));
            while (rs.next()) {
              LOGGER.warning(String.format("[FailoverMonitor @ %s %s] Returned data: %d",
                  hostId, failoverMode, rs.getLong(1)));
            }
          }
        } catch (FailoverSuccessSQLException ex) {
          String connectedHost = this.getConnectedHost(conn, databaseEngine, deployment);
          LOGGER.finest(() -> String.format(
              "[FailoverMonitor @ %s %s] Failover successful. Connected to: %s", hostId, failoverMode, connectedHost));
          results.add(new FailoverResult(
              hostId, failoverMode, Instant.now(), System.nanoTime(), true, connectedHost, null));

        } catch (FailoverFailedSQLException ex) {
          LOGGER.finest(() -> String.format(
              "[FailoverMonitor @ %s %s] Failover failed", hostId, failoverMode));
          results.add(new FailoverResult(
              hostId, failoverMode, Instant.now(), System.nanoTime(), false, null, null));

        } catch (SQLException ex) {
          LOGGER.finest(() -> String.format(
              "[FailoverMonitor @ %s %s] Unexpected SQLException", hostId, failoverMode));
          results.add(new FailoverResult(
              hostId, failoverMode, Instant.now(), System.nanoTime(), false, null, ex.toString()));
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
        LOGGER.finest(String.format("[FailoverMonitor @ %s %s] thread in interrupted.", hostId, failoverMode));
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[FailoverMonitor @ %s %s] thread unhandled exception: ", hostId, failoverMode),
            exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[FailoverMonitor @ %s %s] thread is completed.", hostId, failoverMode));
      }
    });
  }

  private String getLongRunningQuery(
      final DatabaseEngine databaseEngine,
      final DatabaseEngineDeployment deployment) {

    switch (databaseEngine) {
      case PG:
        return "SELECT 9999, pg_sleep(600)"; // 10 min
      case MYSQL:
      case MARIADB:
        //return "SELECT sleep(600)"; // 10 min
        return "SELECT 9999 WHERE sleep(600)"; // 10 min
      default:
        throw new UnsupportedOperationException(databaseEngine.toString());
    }
  }

  private String getConnectedHost(
      final Connection conn,
      final DatabaseEngine databaseEngine,
      final DatabaseEngineDeployment deployment) {

    try {
      return auroraUtil.queryInstanceId(databaseEngine, deployment, conn);
    } catch (Exception ex) {
      return null;
    }
  }

  private Thread getTriggerFailoverThread(
      final String clusterId,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicReference<CountDownLatch> finishLatch,
      final AtomicLong failoverTriggerTimeNano,
      final AtomicReference<Instant> failoverTriggerTimestamp) {

    return new Thread(() -> {

      try {
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        TimeUnit.SECONDS.sleep(30);
        auroraUtil.failoverClusterToTarget(clusterId, null);

        LOGGER.finest("[TriggerFailover] failover has triggered.");
        failoverTriggerTimeNano.set(System.nanoTime());
        failoverTriggerTimestamp.set(Instant.now());

      } catch (InterruptedException e) {
        // do nothing
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[TriggerFailover] thread unhandled exception: ", exception);
        this.unhandledExceptions.add(exception);
      } finally {
        finishLatch.get().countDown();
        LOGGER.finest("[TriggerFailover] thread is completed.");
      }
    });
  }

  private Connection openConnectionWithRetry(String url, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = DriverManager.getConnection(url, props);

      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail("Can't connect to " + url);
    }
    return conn;
  }

  private void closeConnection(Connection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (Exception ex) {
      // do nothing
    }
  }

  public List<ClusterInstanceInfo> getClusterInstances(
      DatabaseEngine databaseEngine,
      DatabaseEngineDeployment deployment,
      Connection conn)
      throws SQLException {

    String retrieveTopologySql;
    switch (deployment) {
      case AURORA:
        switch (databaseEngine) {
          case MYSQL:
            retrieveTopologySql =
                "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 1 ELSE 0 END "
                  + "FROM information_schema.replica_host_status "
                  + "ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1), SERVER_ID";
            break;
          case PG:
            retrieveTopologySql =
                "SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 1 ELSE 0 END "
                    + "FROM aurora_replica_status() "
                    + "ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END, SERVER_ID";
            break;
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
        break;
      case RDS_MULTI_AZ_CLUSTER:
        switch (databaseEngine) {
          case MYSQL:

            final String replicaWriterId = getMultiAzMysqlReplicaWriterInstanceId(conn);
            String s = replicaWriterId == null ? "@@server_id" : String.format("'%s'", replicaWriterId);
            retrieveTopologySql =
                "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID, "
                    + "CASE WHEN id = " + s + " THEN 1 ELSE 0 END "
                    + "FROM mysql.rds_topology "
                    + "ORDER BY CASE WHEN id = " + s + " THEN 0 ELSE 1 END, "
                    + "SUBSTRING_INDEX(endpoint, '.', 1)";
            break;
          case PG:
            retrieveTopologySql =
                "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID, "
                    + " CASE WHEN id = (SELECT MAX(multi_az_db_cluster_source_dbi_resource_id) FROM "
                    + " rds_tools.multi_az_db_cluster_source_dbi_resource_id())"
                    + " THEN 1 ELSE 0 END"
                    + " FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')"
                    + " ORDER BY CASE WHEN id ="
                    + " (SELECT MAX(multi_az_db_cluster_source_dbi_resource_id) FROM"
                    + " rds_tools.multi_az_db_cluster_source_dbi_resource_id())"
                    + " THEN 0 ELSE 1 END, endpoint";

            break;
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
        break;
      default:
        return null;
    }

    ArrayList<ClusterInstanceInfo> clusterInstances = new ArrayList<>();

    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(retrieveTopologySql)) {
      while (resultSet.next()) {
        final String host = resultSet.getString(1);
        final boolean isWriter = resultSet.getInt(2) == 1;
        clusterInstances.add(new ClusterInstanceInfo(host, isWriter));
      }
    }
    return clusterInstances;
  }

  public Boolean getReadOnly(
      DatabaseEngine databaseEngine,
      DatabaseEngineDeployment deployment,
      Connection conn) {

    String retrieveReadOnlySql;
    switch (deployment) {
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
        switch (databaseEngine) {
          case MYSQL:
            retrieveReadOnlySql =
                "SELECT @@innodb_read_only";
            break;
          case PG:
            retrieveReadOnlySql =
                "SELECT CASE WHEN pg_is_in_recovery() THEN 1 ELSE 0 END";
            break;
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
        break;
      default:
        return null;
    }

    try {
      try (final Statement stmt = conn.createStatement();
          final ResultSet resultSet = stmt.executeQuery(retrieveReadOnlySql)) {
        if (resultSet.next()) {
          return resultSet.getInt(1) > 0;
        }
      }
    } catch (SQLException ex) {
      // do nothing
      LOGGER.warning("Exception getting readOnly: " + ex);
    }
    return null;
  }

  private String getMultiAzMysqlReplicaWriterInstanceId(Connection conn)
      throws SQLException {

    try (final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery("SHOW REPLICA STATUS")) {
      if (resultSet.next()) {
        return resultSet.getString("Source_Server_id"); // like '1034958454'
      }
      return null;
    }
  }

  private void processMetrics(final RunData runData) {

    LOGGER.finest("Topology event:\n\t"
        + this.topologyEvents.stream().map(TopologyEventHolder::toString).collect(Collectors.joining("\n\t")));

    if (this.failoverTriggerTimestamp.get() == null || this.failoverTriggerTimeNano.get() == 0) {
      runData.topologyError = "Failover trigger time is null.";
      return;
    }

    LOGGER.finest(String.format("Failover triggered: %s", this.failoverTriggerTimestamp.get()));

    // The first event that is occurred after failover is time Zero
    long timeZeroNano = this.topologyEvents.stream()
        .filter(x -> x.timestampNano > this.failoverTriggerTimeNano.get())
        .map(x -> x.timestampNano)
        .sorted()
        .findFirst().orElse(0L);
    if (timeZeroNano == 0) {
      runData.topologyError = "Can't identify time Zero.";
      return;
    }
    LOGGER.finest(String.format("timeZeroNano: %d", timeZeroNano));

    // Get list of all times when the state of topology changed. Calculate offsetTime (ms) against Time Zero.
    final Set<Pair<Long, Instant>> eventOffsetTimes = new HashSet<>();
    for (TopologyEventHolder topologyEventHolder : this.topologyEvents) {
      long offsetTimeMs = TimeUnit.NANOSECONDS.toMillis(topologyEventHolder.timestampNano - timeZeroNano);
      topologyEventHolder.setOffsetTimeMs(offsetTimeMs);
      eventOffsetTimes.add(Pair.create(offsetTimeMs, topologyEventHolder.timestamp));
    }

    // Sort all events by time. The map key is (original) hostId.
    Map<String, ArrayList<TopologyEventHolder>> sortedEventsByOffsetTime = new HashMap<>();
    this.topologyEvents.stream().collect(groupingBy(x -> x.nodeId, toList()))
        .forEach((key, value) -> {
          final ArrayList<TopologyEventHolder> sortedEvents = value.stream()
              .sorted(Comparator.comparingLong(y -> y.getOffsetTimeMs()))
              .collect(Collectors.toCollection(ArrayList::new));
          sortedEventsByOffsetTime.put(key, sortedEvents);
        });

    // Collect a reported writer hostId for each host.
    Set<String> startWriterIds = sortedEventsByOffsetTime
        .values().stream()
        .map(eventHolders -> eventHolders.stream()
            .filter(y -> y.accessible && y.writerHostId != null)
            .sorted(Comparator.comparingLong(y -> y.timestampNano))
            .map(y -> y.writerHostId)
            .findFirst().orElse(null))
        .collect(toSet());

    if (startWriterIds.size() == 0) {
      runData.topologyError = "Can't identify start writer node.";
      return;
    }
    if (startWriterIds.size() > 1) {
      runData.topologyError = "Unstable cluster topology at test start. Possible writers: " + startWriterIds;
      return;
    }

    String startWriterId = startWriterIds.stream().findFirst().orElse(null);
    LOGGER.finest(String.format("startWriterId: %s", startWriterId));

    // Collect the latest writer hostId for each host.
    Set<String> endWriterIds = sortedEventsByOffsetTime
        .values().stream()
        .map(eventHolders -> eventHolders.stream()
            .filter(y -> y.accessible && y.writerHostId != null)
            .sorted(Comparator.comparingLong(y -> -y.timestampNano)) // effectively sort in reverse order
            .map(y -> y.writerHostId)
            .findFirst().orElse(null))
        .collect(toSet());

    if (endWriterIds.size() == 0) {
      runData.topologyError = "Can't identify end writer node.";
      return;
    }

    if (endWriterIds.size() > 1) {
      LOGGER.warning("Unstable cluster topology at test end. Possible writers: " + endWriterIds);
    }

    String endWriterId = endWriterIds.stream().filter(x -> !x.equals(startWriterId)).findFirst().orElse(null);
    LOGGER.info(String.format("endWriterId: %s", endWriterId));

    if (startWriterId.equals(endWriterId)) {
      runData.topologyError = "Start writer node and end writer node are the same.";
      return;
    }

    // Map host names to predefined numbers.
    // "1" - current writer node
    // "2" - new writer node (after failover)
    // "3", "4", ... - other reader nodes if any
    Map<String, Integer> hostMapping = new HashMap<>();
    hostMapping.put(startWriterId, 1); // always "1"
    hostMapping.put(endWriterId, 2); // always "2"

    int mappedReaderHostId = 3;
    Set<String> readers = this.topologyEvents.stream()
        .map(x -> x.nodeId)
        .filter(x -> !startWriterId.equals(x) && !endWriterId.equals(x))
        .collect(toSet());
    for (String readerHostId : readers.stream().sorted().collect(toList())) {
      hostMapping.put(readerHostId, mappedReaderHostId++);
    }

    LOGGER.finest("Host mapping: \n"
        + hostMapping.entrySet().stream()
        .map(x -> String.format("[%s] -> %d", x.getKey(), x.getValue()))
        .collect(Collectors.joining("\n")));

    // Make a reverse host name mapping (host as number to an original host name)
    String[] reverseHostMapping = new String[hostMapping.size() + 1];
    hostMapping.forEach((key, value) -> reverseHostMapping[value] = key);

    LOGGER.finest("Reverse host mapping: \n"
        + IntStream.range(1, reverseHostMapping.length)
        .mapToObj(index -> String.format("[%d] -> %s", index, reverseHostMapping[index]))
        .collect(Collectors.joining("\n")));


    RunDataRow beforeFailover = new RunDataRow();
    beforeFailover.timestamp = this.failoverTriggerTimestamp.get();
    beforeFailover.offsetTimeMs = TimeUnit.NANOSECONDS.toMillis(this.failoverTriggerTimeNano.get() - timeZeroNano);
    beforeFailover.nodes = new HashMap<>();

    // Find a topology state before failover for each mapped host name (as numbers).
    for (int mappedHostId = 1; mappedHostId < reverseHostMapping.length; mappedHostId++) {

      final TopologyEventHolder beforeFailoverTopologyEventHolder =
          sortedEventsByOffsetTime.get(reverseHostMapping[mappedHostId])
            .stream()
            .filter(x -> x.timestampNano <= this.failoverTriggerTimeNano.get())
            .findFirst().orElse(null);

      final RunDataNode runDataNode = new RunDataNode();
      runDataNode.nodeId = mappedHostId;
      if (beforeFailoverTopologyEventHolder != null) {
        runDataNode.accessible = beforeFailoverTopologyEventHolder.accessible;
        runDataNode.readOnly = beforeFailoverTopologyEventHolder.readOnly;
        runDataNode.blankTopology = beforeFailoverTopologyEventHolder.blankTopology;
        runDataNode.writerHostId = beforeFailoverTopologyEventHolder.writerHostId == null
            ? null
            : hostMapping.getOrDefault(beforeFailoverTopologyEventHolder.writerHostId, null);
        runDataNode.readerHostIds = beforeFailoverTopologyEventHolder.readerHostIds == null
              ? null
              : beforeFailoverTopologyEventHolder.readerHostIds.stream()
                    .map(hostMapping::get).sorted().collect(toList());
      } else {
        runDataNode.accessible = false;
        runDataNode.blankTopology = true;
        runDataNode.readOnly = null;
        runDataNode.writerHostId = null;
        runDataNode.readerHostIds = null;
      }
      beforeFailover.nodes.put(mappedHostId, runDataNode);
    }
    runData.topologyRows.add(beforeFailover);

    // Find a first event after failover triggered for each mapped host name (as numbers).
    // Remember the first event index for each node for further use.
    int[] currentIndexByMappedHostId = new int[hostMapping.size() + 1];
    for (int mappedHostId = 1; mappedHostId < reverseHostMapping.length; mappedHostId++) {

      final ArrayList<TopologyEventHolder> events = sortedEventsByOffsetTime.get(reverseHostMapping[mappedHostId]);

      // skip all events that occurred before failover trigger time
      int index = 0;
      while (index < events.size()
          && events.get(index).timestampNano <= this.failoverTriggerTimeNano.get()) {
        index++;
      }

      // index points out to the first event after failover trigger time
      currentIndexByMappedHostId[mappedHostId] = index;
    }

    // Find a node state for each unique event time. Process event times in ascending order.
    for (Pair<Long, Instant> currentRowOffsetTimePair :
        eventOffsetTimes.stream()
            .filter(x -> x.getValue1() >= 0L)
            .sorted(Comparator.comparingLong(Pair::getValue1))
            .collect(Collectors.toList())) {

      final RunDataRow currentRow = new RunDataRow();
      currentRow.timestamp = currentRowOffsetTimePair.getValue2();
      currentRow.offsetTimeMs = currentRowOffsetTimePair.getValue1();
      currentRow.nodes = new HashMap<>();

      // Find a node states for the current event time.
      for (int mappedHostId = 1; mappedHostId < reverseHostMapping.length; mappedHostId++) {

        final ArrayList<TopologyEventHolder> events = sortedEventsByOffsetTime.get(reverseHostMapping[mappedHostId]);

        // Adjust event index to point to the latest events if needed.
        int index = currentIndexByMappedHostId[mappedHostId];

        while (index < events.size()
            && events.get(index).getOffsetTimeMs() <= currentRowOffsetTimePair.getValue1()) {
          index++;
        }
        if (index > 0) {
          index--;
        }

        currentIndexByMappedHostId[mappedHostId] = index;

        final TopologyEventHolder currentTopologyEventHolderForMappedHost = events.get(index);

        final RunDataNode runDataNode = new RunDataNode();
        runDataNode.nodeId = mappedHostId;
        runDataNode.accessible = currentTopologyEventHolderForMappedHost.accessible;
        runDataNode.readOnly = currentTopologyEventHolderForMappedHost.readOnly;
        runDataNode.blankTopology = currentTopologyEventHolderForMappedHost.blankTopology;
        runDataNode.writerHostId = currentTopologyEventHolderForMappedHost.writerHostId == null
            ? null
            : hostMapping.getOrDefault(currentTopologyEventHolderForMappedHost.writerHostId, null);
        runDataNode.readerHostIds = currentTopologyEventHolderForMappedHost.readerHostIds == null
            ? null
            : currentTopologyEventHolderForMappedHost.readerHostIds.stream()
                .map(hostMapping::get).sorted().collect(toList());

        currentRow.nodes.put(mappedHostId, runDataNode);
      }

      runData.topologyRows.add(currentRow);
    }

    if (!runData.topologyRows.isEmpty()) {
      int index = 1;
      // We assume that at this Time Zero moment node "1" identifies itself as a writer or is unavailable.
      // Skip all rows when node "1" ia still accessible and identifies itself as a writer.
      while (index < runData.topologyRows.size()
          && runData.topologyRows.get(index).nodes.get(1).accessible
          && runData.topologyRows.get(index).nodes.get(1).writerHostId == 1) {
        index++;
      }

      // Skip all rows when node "1" ia unavailable.
      while (index < runData.topologyRows.size()
          && !runData.topologyRows.get(index).nodes.get(1).accessible) {
        index++;
      }

      if (index < runData.topologyRows.size()) {
        // Index points out to a row that reports a writer right after node "1" gets available again.
        // We assume that it should report node "2" as a new writer. Otherwise, it's a false positive case.
        runData.topologyFalsePositive = runData.topologyRows.get(index).nodes.get(1).writerHostId != 2;
      } else {
        // Index points out behind the last row.
        // It means that node "1" is still unavailable and can't report a new writer.
        runData.topologyOldWriterUnavailable = true;
      }
    }

    this.processFailoverMetrics(runData, timeZeroNano, hostMapping);
  }

  private void processFailoverMetrics(final RunData runData, long timeZeroNano, Map<String, Integer> hostMapping) {
    this.failoverResults.forEach(x -> {
      x.setMappedHostId(hostMapping.getOrDefault(x.nodeId, 0));
      x.setOffsetTimeMs(TimeUnit.NANOSECONDS.toMillis(x.timestampNano - timeZeroNano));
      if (x.connectedHostId != null) {
        x.setMappedConnectedHostId(hostMapping.getOrDefault(x.connectedHostId, 0));
      }
    });
    runData.failoverResults = new ArrayList<>(this.failoverResults);
    runData.failoverFail = this.failoverResults.stream().anyMatch(x -> !x.success);

    LOGGER.finest("Failover results:\n\t"
        + this.failoverResults.stream().map(FailoverResult::toString).collect(Collectors.joining("\n\t")));
  }

  private void storeJsonMetrics(final Runs runs) {
    try {
      String fileName = "./build/reports/tests/metrics.json";
      File file = new File(fileName);
      ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
      mapper.writeValue(file, runs);
    } catch (Exception ex) {
      LOGGER.log(Level.SEVERE, "Error writing topology metrics json file.", ex);
    }
  }

  private void printTopologyMetrics(int iterationNum, final RunData runData) {

    final String FILE_PREFIX = "topology";

    if (runData.topologyRows.isEmpty()) {
      this.storeTextMetrics(iterationNum, FILE_PREFIX,
          String.format("Iteration %d: no data", iterationNum));
      return;
    }

    if (runData.topologyError != null) {
      this.storeTextMetrics(iterationNum, FILE_PREFIX,
          String.format("Iteration %d: %s", iterationNum, runData.topologyError));
      return;
    }

    final RunDataRow firstRow = runData.topologyRows.get(0);

    if (firstRow.nodes.isEmpty()) {
      this.storeTextMetrics(iterationNum, FILE_PREFIX,
          String.format("Iteration %d: No columns", iterationNum));
      return;
    }

    AsciiTable metricsTable = new AsciiTable();
    metricsTable.addRule();
    ArrayList<String> columns = new ArrayList<>();
    columns.add("Timestamp");
    columns.add("Time (ms)");
    columns.add(null);
    columns.add("1 (writer at start)");
    columns.add(null);
    columns.add("2 (writer at end)");
    for (Entry<Integer, RunDataNode> entry : firstRow.nodes.entrySet().stream()
        .filter(x -> x.getKey() != 1 && x.getKey() != 2)
        .sorted(Comparator.comparingInt(Entry::getKey))
        .collect(toList())) {
      columns.add(null);
      columns.add(String.valueOf(entry.getValue().nodeId));
    }
    metricsTable.addRow(columns);
    metricsTable.addRule();

    columns = new ArrayList<>();
    columns.add("");
    columns.add("");
    columns.add("Writer");
    columns.add("Readers");
    columns.add("Writer");
    columns.add("Readers");
    for (Entry<Integer, RunDataNode> entry : firstRow.nodes.entrySet().stream()
        .filter(x -> x.getKey() != 1 && x.getKey() != 2)
        .sorted(Comparator.comparingInt(Entry::getKey))
        .collect(toList())) {
      columns.add("Writer");
      columns.add("Readers");
    }
    metricsTable.addRow(columns);
    metricsTable.addRule();

    boolean isFirstRow = true;

    for (RunDataRow runDataRow : runData.topologyRows) {
      columns = new ArrayList<>();
      columns.add(runDataRow.timestamp.toString());
      columns.add(String.format("%d", runDataRow.offsetTimeMs));
      for (Entry<Integer, RunDataNode> entry : runDataRow.nodes.entrySet().stream()
          .sorted(Comparator.comparingInt(Entry::getKey))
          .collect(toList())) {
        columns.add(entry.getValue().accessible
            ? (entry.getValue().blankTopology
                || (entry.getValue().writerHostId == null && entry.getValue().readerHostIds == null)
                ? BLANK_TOPOLOGY
                : (entry.getValue().writerHostId == null
                  ? ""
                  : String.valueOf(entry.getValue().writerHostId)))
            : NOT_ACCESSIBLE);
        columns.add(entry.getValue().accessible
            ? (entry.getValue().blankTopology || entry.getValue().readerHostIds == null
              ? BLANK_TOPOLOGY
              : entry.getValue().readerHostIds.stream().map(String::valueOf).collect(joining(",")))
            : NOT_ACCESSIBLE);
      }

      LinkedList<AT_Cell> cells = metricsTable.addRow(columns).getCells();
      cells.get(0).getContext().setTextAlignment(TextAlignment.LEFT);
      cells.get(1).getContext().setTextAlignment(TextAlignment.RIGHT);

      if (isFirstRow) {
        metricsTable.addRule();
        isFirstRow = false;
      }
    }
    metricsTable.addRule();

    String renderedContent = this.renderTable(metricsTable, true);
    LOGGER.finest("\n" + renderedContent);

    this.storeTextMetrics(iterationNum, FILE_PREFIX, renderedContent);
  }

  private void printFailoverMetrics(int iterationNum, final RunData runData) {

    final String FILE_PREFIX = "failover";

    if (runData.failoverResults.isEmpty()) {
      this.storeTextMetrics(iterationNum, FILE_PREFIX,
          String.format("Iteration %d: no data", iterationNum));
      return;
    }

    final RunDataRow firstRow = runData.topologyRows.get(0);

    if (firstRow.nodes.isEmpty()) {
      this.storeTextMetrics(iterationNum, FILE_PREFIX,
          String.format("Iteration %d: No columns", iterationNum));
      return;
    }

    AsciiTable metricsTable = new AsciiTable();
    metricsTable.addRule();
    ArrayList<String> columns = new ArrayList<>();
    columns.add("Timestamp");
    columns.add("Time (ms)");
    columns.add(null);
    columns.add("1 (writer at start)");
    columns.add(null);
    columns.add("2 (writer at end)");
    for (Entry<Integer, RunDataNode> entry : firstRow.nodes.entrySet().stream()
        .filter(x -> x.getKey() != 1 && x.getKey() != 2)
        .sorted(Comparator.comparingInt(Entry::getKey))
        .collect(toList())) {
      columns.add(null);
      columns.add(String.valueOf(entry.getValue().nodeId));
    }
    metricsTable.addRow(columns);
    metricsTable.addRule();

    columns = new ArrayList<>();
    columns.add("");
    columns.add("");
    columns.add("follow writer");
    columns.add("follow reader");
    columns.add("follow writer");
    columns.add("follow reader");
    for (Entry<Integer, RunDataNode> entry : firstRow.nodes.entrySet().stream()
        .filter(x -> x.getKey() != 1 && x.getKey() != 2)
        .sorted(Comparator.comparingInt(Entry::getKey))
        .collect(toList())) {
      columns.add("follow writer");
      columns.add("follow reader");
    }
    metricsTable.addRow(columns);
    metricsTable.addRule();

    RunDataRow runDataRow = runData.topologyRows.get(0);
    columns = new ArrayList<>();
    columns.add(runDataRow.timestamp.toString());
    columns.add(String.format("%d", runDataRow.offsetTimeMs));
    for (Entry<Integer, RunDataNode> entry : runDataRow.nodes.entrySet().stream()
        .sorted(Comparator.comparingInt(Entry::getKey))
        .collect(toList())) {
      columns.add("");
      columns.add("");
    }

    LinkedList<AT_Cell> cells = metricsTable.addRow(columns).getCells();
    cells.get(0).getContext().setTextAlignment(TextAlignment.LEFT);
    cells.get(1).getContext().setTextAlignment(TextAlignment.RIGHT);
    metricsTable.addRule();

    for (long offsetTimeMs : this.failoverResults.stream()
        .map(FailoverResult::getOffsetTimeMs)
        .filter(x -> x >= 0)
        .distinct()
        .sorted()
        .collect(toList())) {

      FailoverResult failoverResult = this.failoverResults.stream()
          .filter(x -> x.getOffsetTimeMs() == offsetTimeMs)
          .findFirst().orElse(null);

      if (failoverResult == null) {
        continue;
      }

      columns = new ArrayList<>();
      columns.add(failoverResult.timestamp.toString());
      columns.add(String.format("%d", failoverResult.getOffsetTimeMs()));
      for (Entry<Integer, RunDataNode> entry : firstRow.nodes.entrySet().stream()
          .sorted(Comparator.comparingInt(Entry::getKey))
          .collect(toList())) {

        FailoverResult failoverResultForNodeFollowWriter = this.failoverResults.stream()
            .filter(x -> x.getOffsetTimeMs() == offsetTimeMs
                && x.mappedHostId == entry.getKey()
                && x.failoverMode.equals(FailoverMode.STRICT_WRITER))
            .findFirst().orElse(null);
        if (failoverResultForNodeFollowWriter != null) {
          columns.add(failoverResultForNodeFollowWriter.success
              ? String.format("success(%d)", failoverResultForNodeFollowWriter.mappedConnectedHostId)
              : "failed");
        } else {
          columns.add("");
        }

        FailoverResult failoverResultForNodeFollowReader = this.failoverResults.stream()
            .filter(x -> x.getOffsetTimeMs() == offsetTimeMs
                && x.mappedHostId == entry.getKey()
                && x.failoverMode.equals(FailoverMode.STRICT_READER))
            .findFirst().orElse(null);
        if (failoverResultForNodeFollowReader != null) {
          columns.add(failoverResultForNodeFollowReader.success
              ? String.format("success(%d)", failoverResultForNodeFollowReader.mappedConnectedHostId)
              : "failed");
        } else {
          columns.add("");
        }
      }

      cells = metricsTable.addRow(columns).getCells();
      cells.get(0).getContext().setTextAlignment(TextAlignment.LEFT);
      cells.get(1).getContext().setTextAlignment(TextAlignment.RIGHT);
    }

    metricsTable.addRule();

    String renderedContent = this.renderTable(metricsTable, true);
    LOGGER.finest("\n" + renderedContent);

    this.storeTextMetrics(iterationNum, FILE_PREFIX, renderedContent);
  }

  private void storeTextMetrics(int iterationNum, String filePrefix, String renderedContent) {
    FileWriter writer = null;
    try {
      String fileName = String.format("./build/reports/tests/%s_metrics_%d.txt", filePrefix, iterationNum);
      writer = new FileWriter(fileName);
      writer.write(renderedContent);
      writer.close();
      writer = null;

    } catch (Exception ex) {
      LOGGER.log(Level.SEVERE, "Error writing " + filePrefix + " metrics text file.", ex);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception ex) {
          // do nothing
        }
      }
    }
  }

  private String renderTable(AsciiTable table, boolean leftAlignForColumn0) {
    table.setTextAlignment(TextAlignment.CENTER);
    table.getRenderer().setCWC(new CWC_LongestLine());
    table.getContext().setGrid(A7_Grids.minusBarPlusEquals());

    for (int rowNum = 0; rowNum < table.getRawContent().size(); rowNum++) {
      AT_Row row = table.getRawContent().get(rowNum);
      row.setPaddingLeft(2);
      row.setPaddingRight(2);
      if (leftAlignForColumn0 && row.getType() == TableRowType.CONTENT) {
        row.getCells().get(0).getContext().setTextAlignment(TextAlignment.LEFT);
      }
    }
    return table.render();
  }

  private void logUnhandledExceptions() {
    for (Throwable throwable : this.unhandledExceptions) {
      LOGGER.log(Level.FINEST, "Unhandled exception", throwable);
    }
  }

  public static class ClusterInstanceInfo {

    public final String hostId;
    public final boolean isWriter;

    public ClusterInstanceInfo(String hostId, boolean isWriter) {
      this.hostId = hostId;
      this.isWriter = isWriter;
    }
  }
}
