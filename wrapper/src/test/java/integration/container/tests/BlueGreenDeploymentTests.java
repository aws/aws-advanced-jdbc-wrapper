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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.DialectCodes;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPlugin;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.plugin.iam.RegularRdsUtility;
import software.amazon.jdbc.util.DriverInfo;
import software.amazon.jdbc.util.RdsUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@EnableOnTestFeature(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
@EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE, DatabaseEngineDeployment.AURORA})
@EnableOnDatabaseEngine({DatabaseEngine.MYSQL, DatabaseEngine.PG})
@Order(16)
public class BlueGreenDeploymentTests {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenDeploymentTests.class.getName());
  protected static final boolean INCLUDE_CLUSTER_ENDPOINTS = false;
  protected static final boolean INCLUDE_WRITER_AND_READER_ONLY = false;
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected static final RdsUtils rdsUtil = new RdsUtils();

  private static final String MYSQL_BG_STATUS_QUERY =
      "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, role, status, version"
      + " FROM mysql.rds_topology";

  private static final String PG_AURORA_BG_STATUS_QUERY =
      "SELECT id, SPLIT_PART(endpoint, '.', 1) as hostId, endpoint, port, role, status, version"
      + " FROM get_blue_green_fast_switchover_metadata('aws_jdbc_driver')";

  private static final String PG_RDS_BG_STATUS_QUERY =
      "SELECT * FROM rds_tools.show_topology('aws_jdbc_driver-" + DriverInfo.DRIVER_VERSION + "')";

  private static final String TEST_CLUSTER_ID = "test-cluster-id";

  public static class TimeHolder {
    public long startTime;
    public long endTime;
    public String error;
    public long holdNano;

    public TimeHolder(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public TimeHolder(long startTime, long endTime, long holdNano) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.holdNano = holdNano;
    }

    public TimeHolder(long startTime, long endTime, String error) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.error = error;
    }

    public TimeHolder(long startTime, long endTime, long holdNano, String error) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.holdNano = holdNano;
      this.error = error;
    }
  }

  public static class BlueGreenResults {
    public final AtomicLong startTime = new AtomicLong();
    public final AtomicLong threadsSyncTime = new AtomicLong();
    public final AtomicLong bgTriggerTime = new AtomicLong();
    public final AtomicLong directBlueLostConnectionTime = new AtomicLong();
    public final AtomicLong directBlueIdleLostConnectionTime = new AtomicLong();
    public final AtomicLong wrapperBlueIdleLostConnectionTime = new AtomicLong();
    public final AtomicLong wrapperGreenLostConnectionTime = new AtomicLong();
    public final AtomicLong dnsBlueChangedTime = new AtomicLong();
    public String dnsBlueError = null;
    public final AtomicLong dnsGreenRemovedTime = new AtomicLong();
    public final AtomicLong greenNodeChangeNameTime = new AtomicLong();
    public final ConcurrentHashMap<String, Long> blueStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, Long> greenStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentLinkedDeque<TimeHolder> blueWrapperConnectTimes = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> blueWrapperExecuteTimes = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> greenWrapperExecuteTimes = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> greenDirectIamIpWithBlueNodeConnectTimes =
        new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> greenDirectIamIpWithGreenNodeConnectTimes =
        new ConcurrentLinkedDeque<>();
  }

  private final ConcurrentHashMap<String, BlueGreenResults> results = new ConcurrentHashMap<>();
  private final ConcurrentLinkedDeque<Throwable> unhandledExceptions = new ConcurrentLinkedDeque<>();

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  //@Disabled
  public void test_Switchover(TestDriver testDriver) throws SQLException, InterruptedException {

    this.results.clear();
    this.unhandledExceptions.clear();

    boolean iamEnabled =
        TestEnvironment.getCurrent().getInfo().getRequest().getFeatures().contains(TestEnvironmentFeatures.IAM);

    final long startTimeNano = System.nanoTime();

    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicReference<CountDownLatch> startLatchAtomic = new AtomicReference<>(null);
    final AtomicReference<CountDownLatch> finishLatchAtomic = new AtomicReference<>(null);
    int threadCount = 0;
    int threadFinishCount = 0;

    final ArrayList<Thread> threads = new ArrayList<>();

    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    final TestInstanceInfo testInstance = info.getDatabaseInfo().getInstances().get(0);
    final String dbName = info.getDatabaseInfo().getDefaultDbName();

    final List<String> topologyInstances = this.getBlueGreenEndpoints(info.getBlueGreenDeploymentId());
    LOGGER.finest("topologyInstances: \n" + String.join("\n", topologyInstances));

    for (String host : topologyInstances) {
      final String hostId = host.substring(0, host.indexOf('.'));
      assertNotNull(hostId);

      this.results.put(hostId, new BlueGreenResults());

      if (rdsUtil.isNoPrefixInstance(host)) {
        threads.add(getDirectBlueConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getDirectBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getWrapperBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getWrapperBlueExecutingConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getWrapperBlueNewConnectionMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;

        threads.add(getBlueDnsMonitoringThread(
            hostId, host, startLatchAtomic, stop, finishLatchAtomic, results.get(hostId)));
        threadCount++;
        threadFinishCount++;
      }

      if (rdsUtil.isGreenInstance(host)) {
        threads.add(getDirectGreenTopologyMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getWrapperGreenConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
            results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        threads.add(getGreenDnsMonitoringThread(
            hostId, host, startLatchAtomic, stop, finishLatchAtomic, results.get(hostId)));
        threadCount++;
        threadFinishCount++;

        if (iamEnabled) {
          threads.add(getGreenIamConnectivityMonitoringThread(
              hostId,
              "BlueHostToken", rdsUtil.removeGreenInstancePrefix(host), host,
              testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
              results.get(hostId),
              results.get(hostId).greenDirectIamIpWithBlueNodeConnectTimes,
              false, true));
          threadCount++;
          threadFinishCount++;

          threads.add(getGreenIamConnectivityMonitoringThread(
              hostId,
              "GreenHostToken", host, host,
              testInstance.getPort(), dbName, startLatchAtomic, stop, finishLatchAtomic,
              results.get(hostId),
              results.get(hostId).greenDirectIamIpWithGreenNodeConnectTimes,
              true, false));
          threadCount++;
          threadFinishCount++;
        }
      }
    }

    threads.add(getBlueGreenSwitchoverTriggerThread(
        info.getBlueGreenDeploymentId(), startLatchAtomic, stop, finishLatchAtomic, results));
    threadCount++;
    threadFinishCount++;

    results.forEach((key, value) -> value.startTime.set(startTimeNano));

    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final CountDownLatch finishLatch = new CountDownLatch(threadFinishCount);
    startLatchAtomic.set(startLatch);
    finishLatchAtomic.set(finishLatch);

    threads.forEach(Thread::start);
    LOGGER.finest("All threads started.");

    finishLatch.await(6, TimeUnit.MINUTES);
    LOGGER.finest("All threads completed.");

    TimeUnit.MINUTES.sleep(3);

    LOGGER.finest("Stopping all threads...");
    stop.set(true);
    TimeUnit.SECONDS.sleep(5);
    LOGGER.finest("Interrupting all threads...");
    threads.forEach(Thread::interrupt);
    TimeUnit.SECONDS.sleep(5);

    assertTrue(results.entrySet().stream().allMatch(x -> x.getValue().bgTriggerTime.get() > 0));

    // Report results
    LOGGER.finest("Test is over.");
    this.printMetrics();

    if (!this.unhandledExceptions.isEmpty()) {
      this.logUnhandledExceptions();
      fail("There are unhandled exceptions.");
    }

    this.assertTest();

    LOGGER.finest("Completed");
  }

  // Blue node
  // Checking: connectivity, isClosed()
  // Can terminate for itself
  private Thread getDirectBlueIdleConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectBlueIdleConnectivity @ %s] connection is open.", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[DirectBlueIdleConnectivity @ %s] Starting connectivity monitoring.", hostId));

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.directBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[DirectBlueIdleConnectivity @ %s] thread exception: %s", hostId, throwable));
            results.directBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[DirectBlueIdleConnectivity @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[DirectBlueIdleConnectivity @ %s] thread is completed.", hostId));
      }
    });
  }

  // Blue node
  // Checking: connectivity, SELECT 1
  // Can terminate for itself
  private Thread getDirectBlueConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectBlueConnectivity @ %s] connection is open.", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[DirectBlueConnectivity @ %s] Starting connectivity monitoring.", hostId));

        while (!stop.get()) {
          try  {
            final Statement statement = conn.createStatement();
            final ResultSet result = statement.executeQuery("SELECT 1");
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[DirectBlueConnectivity @ %s] thread exception: %s", hostId, throwable));
            results.directBlueLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[DirectBlueConnectivity @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[DirectBlueConnectivity @ %s] thread is completed.", hostId));
      }
    });
  }

  // Blue node
  // Check: connectivity, isClosed()
  // Can terminate for itself
  private Thread getWrapperBlueIdleConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = this.getWrapperConnectionProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, this.getWrapperConnectionPlugins()),
            props);
        LOGGER.finest(String.format("[WrapperBlueIdle @ %s] connection is open.", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[WrapperBlueIdle @ %s] Starting connectivity monitoring.", hostId));

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[WrapperBlueIdle @ %s] thread exception: %s", hostId, throwable));
            results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[WrapperBlueIdle @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[WrapperBlueIdle @ %s] thread is completed.", hostId));
      }
    });
  }

  // Blue node
  // Check: connectivity, SELECT sleep(5)
  // Expect: long execution time (longer than 5s) during active phase of switchover
  // Can terminate for itself
  private Thread getWrapperBlueExecutingConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;

      String query;
      switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()) {
        case MYSQL:
          query = "SELECT sleep(5)";
          break;
        case PG:
          query = "SELECT pg_sleep(5)";
          break;
        default:
          throw new UnsupportedOperationException(
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine().toString());
      }

      try {
        final Properties props = this.getWrapperConnectionProperties();
        conn = DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, this.getWrapperConnectionPlugins()),
            props);

        BlueGreenConnectionPlugin bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
        assertNotNull(bgPlugin);

        Statement statement = conn.createStatement();
        LOGGER.finest(String.format("[WrapperBlueExecute @ %s] connection is open.", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[WrapperBlueExecute @ %s] Starting connectivity monitoring.", hostId));

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            ResultSet rs = statement.executeQuery(query);
            endTime = System.nanoTime();
            results.blueWrapperExecuteTimes.add(
                new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano()));
          } catch (SQLException throwable) {
            endTime = System.nanoTime();
            results.blueWrapperExecuteTimes.add(
                new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano(), throwable.getMessage()));
            if (conn.isClosed()) {
              break;
            }
          }

          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[WrapperBlueExecute @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[WrapperBlueExecute @ %s] thread is completed.", hostId));
      }
    });
  }

  // Blue node
  // Check: connectivity, opening a new connection
  // Expect: longer opening connection time during active phase of switchover
  // Need a stop signal to terminate
  private Thread getWrapperBlueNewConnectionMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      BlueGreenConnectionPlugin bgPlugin = null;
      try {
        final Properties props = this.getWrapperConnectionProperties();
        //PropertyDefinition.LOGIN_TIMEOUT.set(props, "10000");
        //PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[WrapperBlueNewConnection @ %s] Starting connectivity monitoring.", hostId));

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            conn = DriverManager.getConnection(
                ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, this.getWrapperConnectionPlugins()),
                props);
            endTime = System.nanoTime();

            bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
            assertNotNull(bgPlugin);

            results.blueWrapperConnectTimes.add(new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano()));

          } catch (SQLTimeoutException sqlTimeoutException) {
            LOGGER.finest(String.format(
                "[WrapperBlueNewConnection @ %s] (SQLTimeoutException) thread exception: %s",
                hostId,
                sqlTimeoutException));
            endTime = System.nanoTime();
            if (conn != null) {
              bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
              assertNotNull(bgPlugin);
              results.blueWrapperConnectTimes.add(
                  new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano(), sqlTimeoutException.getMessage()));
            } else {
              results.blueWrapperConnectTimes.add(
                  new TimeHolder(startTime, endTime, sqlTimeoutException.getMessage()));
            }
          } catch (SQLException throwable) {
            LOGGER.finest(String.format(
                "[WrapperBlueNewConnection @ %s] thread exception: %s", hostId, throwable));
            endTime = System.nanoTime();
            if (conn != null) {
              bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
              assertNotNull(bgPlugin);
              results.blueWrapperConnectTimes.add(
                  new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano(), throwable.getMessage()));
            } else {
              results.blueWrapperConnectTimes.add(
                  new TimeHolder(startTime, endTime, throwable.getMessage()));
            }
          }

          this.closeConnection(conn);
          conn = null;
          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[WrapperBlueNewConnection @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[WrapperBlueNewConnection @ %s] thread is completed.", hostId));
      }
    });
  }

  // Green node
  // Check: DNS record presence
  // Expect: DNS record becomes deleted while/after switchover
  // Can terminate by itself
  private Thread getGreenDnsMonitoringThread(
      final String hostId,
      final String host,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {
      try {
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        final String ip = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[GreenDNS @ %s] %s -> %s", hostId, host, ip));

        while (!stop.get()) {
          TimeUnit.SECONDS.sleep(1);
          try {
            String tmp = InetAddress.getByName(host).getHostAddress();
          } catch (UnknownHostException unknownHostException) {
            results.dnsGreenRemovedTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException e) {
        // do nothing
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, String.format("[GreenDNS @ %s] thread unhandled exception: ", hostId), e);
        this.unhandledExceptions.add(e);
      } finally {
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[GreenDNS @ %s] thread is completed.", hostId));
      }
    });
  }

  // Blue DNS
  // Check IP address change time
  // Can terminate for itself
  public Thread getBlueDnsMonitoringThread(
      final String hostId,
      final String host,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {
      try {
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        final String originalIp = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[BlueDNS @ %s] %s -> %s", hostId, host, originalIp));

        while (!stop.get()) {
          TimeUnit.SECONDS.sleep(1);
          try {
            String currentIp = InetAddress.getByName(host).getHostAddress();
            if (!currentIp.equals(originalIp)) {
              results.dnsBlueChangedTime.set(System.nanoTime());
              LOGGER.finest(() -> String.format("[BlueDNS @ %s] %s -> %s", hostId, host, currentIp));
              break;
            }
          } catch (UnknownHostException unknownHostException) {
            LOGGER.finest(() -> String.format("[BlueDNS @ %s] Error: %s", hostId, unknownHostException));
            results.dnsBlueError = unknownHostException.getMessage();
            results.dnsBlueChangedTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException e) {
        // do nothing
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, String.format("[BlueDNS @ %s] thread unhandled exception: ", hostId), e);
        this.unhandledExceptions.add(e);
      } finally {
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[BlueDNS @ %s] thread is completed.", hostId));
      }
    });
  }

  // Green node
  // Monitor BG status changes
  // Can terminate for itself
  private Thread getDirectGreenTopologyMonitoringThread(
      final String hostId,
      final String url,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;

      String query;
      switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()) {
        case MYSQL:
          query = MYSQL_BG_STATUS_QUERY;
          break;
        case PG:
          switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()) {
            case AURORA:
              query = PG_AURORA_BG_STATUS_QUERY;
              break;
            case RDS_MULTI_AZ_INSTANCE:
              query = PG_RDS_BG_STATUS_QUERY;
              break;
            default:
              throw new UnsupportedOperationException(
                  TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment().toString());
          }
          break;
        default:
          throw new UnsupportedOperationException(
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine().toString());
      }

      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(url, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectGreenTopology @ %s] connection opened", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[DirectGreenTopology @ %s] Starting BG statuses monitoring.", hostId));

        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);

        while (!stop.get() && System.nanoTime() < endTime) {
          if (conn == null) {
            conn = openConnectionWithRetry(
                ConnectionStringHelper.getUrl(url, port, dbName),
                props);
            LOGGER.finest(String.format("[DirectGreenTopology @ %s] connection re-opened", hostId));
          }

          try {
            final Statement statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
              String queryRole = rs.getString("role");
              String queryVersion = rs.getString("version");
              String queryNewStatus = rs.getString("status");
              boolean isGreen = BlueGreenRole.parseRole(queryRole, queryVersion) == BlueGreenRole.TARGET;

              if (isGreen) {
                results.greenStatusTime.computeIfAbsent(queryNewStatus, (key) -> {
                  LOGGER.finest(() -> String.format(
                      "[DirectGreenTopology @ %s] status changed to: %s", hostId, queryNewStatus));
                  return System.nanoTime();
                });
              } else {
                results.blueStatusTime.computeIfAbsent(queryNewStatus, (key) -> {
                  LOGGER.finest(() -> String.format(
                      "[DirectGreenTopology @ %s] status changed to: %s", hostId, queryNewStatus));
                  return System.nanoTime();
                });
              }
            }
            TimeUnit.MILLISECONDS.sleep(100);

          } catch (SQLException throwable) {
            LOGGER.log(
                Level.FINEST,
                String.format("[DirectGreenTopology @ %s] thread exception: %s", hostId, throwable),
                throwable);
            this.closeConnection(conn);
            conn = null;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[DirectGreenTopology @ %s] thread unhandled exception: ", hostId),
            exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[DirectGreenTopology @ %s] thread is completed.", hostId));
      }
    });
  }

  // Green node
  // Check: connectivity, SELECT 1
  // Expect: no interruption, execute takes longer time during BG switchover
  // Can terminate for itself
  private Thread getWrapperGreenConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = this.getWrapperConnectionProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, this.getWrapperConnectionPlugins()),
            props);
        LOGGER.finest(String.format("[WrapperGreenConnectivity @ %s] connection is open.", hostId));

        BlueGreenConnectionPlugin bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
        assertNotNull(bgPlugin);

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format("[WrapperGreenConnectivity @ %s] Starting connectivity monitoring.", hostId));

        long startTime = System.nanoTime();
        while (!stop.get()) {
          try  {
            final Statement statement = conn.createStatement();
            startTime = System.nanoTime();
            final ResultSet result = statement.executeQuery("SELECT 1");
            long endTime = System.nanoTime();
            results.greenWrapperExecuteTimes.add(new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano()));
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLTimeoutException sqlTimeoutException) {
            LOGGER.finest(String.format(
                "[WrapperGreenConnectivity @ %s] (SQLTimeoutException) thread exception: %s",
                hostId,
                sqlTimeoutException));
            results.greenWrapperExecuteTimes.add(
                new TimeHolder(
                    startTime,
                    System.nanoTime(),
                    bgPlugin.getHoldTimeNano(),
                    sqlTimeoutException.getMessage()));
            if (conn.isClosed()) {
              results.wrapperGreenLostConnectionTime.set(System.nanoTime());
              break;
            }
          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[WrapperGreenConnectivity @ %s] thread exception: %s", hostId, throwable));
            results.wrapperGreenLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST,
            String.format("[WrapperGreenConnectivity @ %s] thread unhandled exception: ", hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format("[WrapperGreenConnectivity @ %s] thread is completed.", hostId));
      }
    });
  }

  // Green node
  // Check: connectivity (opening a new connection) with IAM when using node IP address
  // Expect: loose connectivity after green node changes its name (green prefix to no-prefix)
  // Can terminate for itself
  private Thread getGreenIamConnectivityMonitoringThread(
      final String hostId,
      final String threadPrefix,
      final String iamTokenHost,
      final String connectHost,
      final int port,
      final String dbName,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final BlueGreenResults results,
      final ConcurrentLinkedDeque<TimeHolder> resultQueue,
      final boolean notifyOnFirstError,
      final boolean exitOnFirstSuccess) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        RegularRdsUtility regularRdsUtility = new RegularRdsUtility();

        final Properties props = new Properties();
        props.setProperty("user", TestEnvironment.getCurrent().getInfo().getIamUsername());
        props.setProperty("connectTimeout", "10000");
        props.setProperty("socketTimeout", "10000");

        final String greenNodeConnectIp = InetAddress.getByName(connectHost).getHostAddress();

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);

        LOGGER.finest(String.format(
            "[DirectGreenIamIp%s @ %s] Starting connectivity monitoring %s", threadPrefix, hostId, iamTokenHost));

        while (!stop.get()) {

          String token = regularRdsUtility.generateAuthenticationToken(
              DefaultCredentialsProvider.create(),
              Region.of(TestEnvironment.getCurrent().getInfo().getRegion()),
              iamTokenHost,
              getPort(),
              TestEnvironment.getCurrent().getInfo().getIamUsername());
          props.setProperty("password", token);

          long startTime = System.nanoTime();
          long endTime;
          try  {
            conn = DriverManager.getConnection(
                ConnectionStringHelper.getUrl(greenNodeConnectIp, port, dbName),
                props);
            endTime = System.nanoTime();
            resultQueue.add(new TimeHolder(startTime, endTime));

            if (exitOnFirstSuccess) {
              results.greenNodeChangeNameTime.compareAndSet(0, System.nanoTime());
              LOGGER.finest(String.format(
                  "[DirectGreenIamIp%s @ %s] Successfully connected. Exiting thread...", threadPrefix, hostId));
              return;
            }

          } catch (SQLTimeoutException sqlTimeoutException) {
            LOGGER.finest(String.format(
                "[DirectGreenIamIp%s @ %s] (SQLTimeoutException) thread exception: %s",
                threadPrefix, hostId, sqlTimeoutException));
            endTime = System.nanoTime();
            resultQueue.add(new TimeHolder(startTime, endTime, sqlTimeoutException.getMessage()));
          } catch (SQLException throwable) {
            LOGGER.finest(String.format(
                "[DirectGreenIamIp%s @ %s] thread exception: %s", threadPrefix, hostId, throwable.getMessage()));
            endTime = System.nanoTime();
            resultQueue.add(new TimeHolder(startTime, endTime, throwable.getMessage()));
            if (notifyOnFirstError
                && throwable.getMessage() != null
                && throwable.getMessage().contains("Access denied")) {
              results.greenNodeChangeNameTime.compareAndSet(0, System.nanoTime());
              LOGGER.finest(String.format(
                  "[DirectGreenIamIp%s @ %s] The first 'Access denied' exception. Exiting thread...",
                  threadPrefix, hostId));
              return;
            }
          }

          this.closeConnection(conn);
          conn = null;
          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, String.format(
            "[DirectGreenIamIp%s @ %s] thread unhandled exception: ", threadPrefix, hostId), exception);
        this.unhandledExceptions.add(exception);
      } finally {
        this.closeConnection(conn);
        finishLatch.get().countDown();
        LOGGER.finest(String.format(
            "[DirectGreenIamIp%s @ %s] thread is completed.", threadPrefix, hostId));
      }
    });
  }

  // RDS API, trigger BG switchover
  // Can terminate for itself
  private Thread getBlueGreenSwitchoverTriggerThread(
      final String blueGreenId,
      final AtomicReference<CountDownLatch> startLatch,
      final AtomicBoolean stop,
      final AtomicReference<CountDownLatch> finishLatch,
      final Map<String, BlueGreenResults> results) {

    return new Thread(() -> {

      try {
        startLatch.get().countDown();

        // wait for another threads to be ready to start the test
        startLatch.get().await(5, TimeUnit.MINUTES);
        final long nanoTime = System.nanoTime();
        results.forEach((key, value) -> value.threadsSyncTime.set(nanoTime));

        TimeUnit.SECONDS.sleep(30);
        auroraUtil.switchoverBlueGreenDeployment(blueGreenId);
        //LOGGER.finest("Dry run: switchoverBlueGreenDeployment");

        final long nanoTime2 = System.nanoTime();
        results.forEach((key, value) -> value.bgTriggerTime.set(nanoTime2));

      } catch (InterruptedException e) {
        // do nothing
        Thread.currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[Switchover] thread unhandled exception: ", exception);
        this.unhandledExceptions.add(exception);
      } finally {
        finishLatch.get().countDown();
        LOGGER.finest("[Switchover] thread is completed.");
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

  private List<String> getBlueGreenEndpoints(final String blueGreenId) throws SQLException {

    BlueGreenDeployment blueGreenDeployment = auroraUtil.getBlueGreenDeployment(blueGreenId);
    if (blueGreenDeployment == null) {
      throw new RuntimeException("BG not found: " + blueGreenId);
    }

    switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()) {
      case RDS_MULTI_AZ_INSTANCE:
        DBInstance blueInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.source());
        if (blueInstance == null) {
          throw new RuntimeException("Blue instance not found.");
        }
        DBInstance greenInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.target());
        if (greenInstance == null) {
          throw new RuntimeException("Green instance not found.");
        }
        return Arrays.asList(blueInstance.endpoint().address(), greenInstance.endpoint().address());

      case AURORA:
        ArrayList<String> endpoints = new ArrayList<>();
        DBCluster blueCluster = auroraUtil.getClusterByArn(blueGreenDeployment.source());
        if (blueCluster == null) {
          throw new RuntimeException("Blue cluster not found.");
        }
        if (INCLUDE_CLUSTER_ENDPOINTS) {
          endpoints.add(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
        }
        if (INCLUDE_WRITER_AND_READER_ONLY) {
          endpoints.add(
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getHost()); // writer

          if (TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().size() > 1) {
            endpoints.add(
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(1).getHost()); // reader
          }
        } else {
          for (TestInstanceInfo instanceInfo :
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances()) {
            endpoints.add(instanceInfo.getHost());
          }
        }

        DBCluster greenCluster = auroraUtil.getClusterByArn(blueGreenDeployment.target());
        if (greenCluster == null) {
          throw new RuntimeException("Green cluster not found.");
        }

        if (INCLUDE_CLUSTER_ENDPOINTS) {
          endpoints.add(greenCluster.endpoint());
        }

        String instancePattern = rdsUtil.getRdsInstanceHostPattern(greenCluster.endpoint());

        List<String> instanceIds = auroraUtil.getAuroraInstanceIds(
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
            ConnectionStringHelper.getUrl(
                greenCluster.endpoint(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

        if (instanceIds.isEmpty()) {
          throw new RuntimeException("Can't find green cluster instances.");
        }
        if (INCLUDE_WRITER_AND_READER_ONLY) {
          endpoints.add(instancePattern.replace("?", instanceIds.get(0))); // writer
          if (instanceIds.size() > 1) {
            endpoints.add(instancePattern.replace("?", instanceIds.get(1))); // reader
          }
        } else {
          for (String instanceId : instanceIds) {
            endpoints.add(instancePattern.replace("?", instanceId));
          }
        }
        return endpoints;

      default:
        throw new RuntimeException("Unsupported "
            + TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment());
    }
  }

  private Properties getWrapperConnectionProperties() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    RdsHostListProvider.CLUSTER_ID.set(props, TEST_CLUSTER_ID);

    DatabaseEngine databaseEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
        switch (databaseEngine) {
          case MYSQL:
            DialectManager.DIALECT.set(props, DialectCodes.AURORA_MYSQL);
            break;
          case PG:
            DialectManager.DIALECT.set(props, DialectCodes.AURORA_PG);
            break;
          default:
            // do nothing
        }
        break;
      case RDS_MULTI_AZ_INSTANCE:
        switch (databaseEngine) {
          case MYSQL:
            DialectManager.DIALECT.set(props, DialectCodes.RDS_MYSQL);
            break;
          case PG:
            DialectManager.DIALECT.set(props, DialectCodes.RDS_PG);
            break;
          default:
            // do nothing
        }
        break;
      default:
        // do nothing
    }

    if (TestEnvironment.getCurrent().getInfo().getRequest().getFeatures().contains(TestEnvironmentFeatures.IAM)) {
      IamAuthConnectionPlugin.IAM_REGION.set(props, TestEnvironment.getCurrent().getInfo().getRegion());
      PropertyDefinition.USER.set(props, TestEnvironment.getCurrent().getInfo().getIamUsername());
    }
    return props;
  }

  private String getWrapperConnectionPlugins() {
    if (TestEnvironment.getCurrent().getInfo().getRequest().getFeatures().contains(TestEnvironmentFeatures.IAM)) {
      return "bg,iam";
    }
    return "bg";
  }

  private int getPort() {
    switch (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine()) {
      case MYSQL:
      case MARIADB:
        return 3306;
      case PG:
        return 5432;
      default:
        throw new UnsupportedOperationException(
            TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine().toString());
    }
  }

  private void printMetrics() {

    long bgTriggerTime = results.values().stream()
        .map(blueGreenResults -> blueGreenResults.bgTriggerTime.get())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Can't get bgTriggerTime"));

    AsciiTable metricsTable = new AsciiTable();
    metricsTable.addRule();
    metricsTable.addRow(
        "Instance/endpoint",
        "startTime",
        "threadsSync",
        "direct Blue conn dropped (idle)",
        "direct Blue conn dropped (SELECT 1)",
        "wrapper Blue conn dropped (idle)",
        "wrapper Green conn dropped (SELECT 1)",
        "Blue DNS updated",
        "Green DNS removed",
        "Green node certificate change");
    metricsTable.addRule();

    Comparator<Entry<String, BlueGreenResults>> entryGreenComparator =
        Comparator.comparing(x -> rdsUtil.isGreenInstance(x.getKey() + ".") ? 1 : 0);
    Comparator<Entry<String, BlueGreenResults>> entryNameComparator =
        Comparator.comparing(x -> rdsUtil.removeGreenInstancePrefix(x.getKey()).toLowerCase());
    List<Entry<String, BlueGreenResults>> sortedEntries = this.results.entrySet().stream()
        .sorted(entryGreenComparator.thenComparing(entryNameComparator))
        .collect(Collectors.toList());

    if (sortedEntries.isEmpty()) {
      metricsTable.addRow("No entries");
    }

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {

      long startTime = TimeUnit.NANOSECONDS.toMillis(entry.getValue().startTime.get() - bgTriggerTime);
      long threadsSyncTime = TimeUnit.NANOSECONDS.toMillis(entry.getValue().threadsSyncTime.get() - bgTriggerTime);
      String directBlueIdleLostConnectionTime = this.getFormattedNanoTime(
          entry.getValue().directBlueIdleLostConnectionTime, bgTriggerTime);
      String directBlueLostConnectionTime = this.getFormattedNanoTime(
          entry.getValue().directBlueLostConnectionTime, bgTriggerTime);
      String wrapperBlueIdleLostConnectionTime = this.getFormattedNanoTime(
          entry.getValue().wrapperBlueIdleLostConnectionTime, bgTriggerTime);
      String wrapperGreenLostConnectionTime = this.getFormattedNanoTime(
          entry.getValue().wrapperGreenLostConnectionTime, bgTriggerTime);
      String dnsBlueChangedTime = this.getFormattedNanoTime(
          entry.getValue().dnsBlueChangedTime, bgTriggerTime);
      String dnsGreenRemovedTime = this.getFormattedNanoTime(
          entry.getValue().dnsGreenRemovedTime, bgTriggerTime);
      String greenNodeChangeNameTime = this.getFormattedNanoTime(
          entry.getValue().greenNodeChangeNameTime, bgTriggerTime);

      metricsTable.addRow(
          entry.getKey(),
          startTime, threadsSyncTime, directBlueIdleLostConnectionTime,
          directBlueLostConnectionTime, wrapperBlueIdleLostConnectionTime, wrapperGreenLostConnectionTime,
          dnsBlueChangedTime, dnsGreenRemovedTime, greenNodeChangeNameTime)
        .getCells().get(0).getContext().setTextAlignment(TextAlignment.LEFT);
    }

    metricsTable.addRule();
    LOGGER.finest("\n" + this.renderTable(metricsTable, true));

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {
      if (entry.getValue().blueStatusTime.isEmpty() && entry.getValue().greenStatusTime.isEmpty()) {
        continue;
      }
      this.printNodeStatusTimes(entry.getKey(), entry.getValue(), bgTriggerTime);
    }

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {
      if (entry.getValue().blueWrapperConnectTimes.isEmpty()) {
        continue;
      }
      this.printDurationTimes(entry.getKey(), "Wrapper connection time (ms) to Blue",
          entry.getValue().blueWrapperConnectTimes, bgTriggerTime);
    }

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {
      if (entry.getValue().greenDirectIamIpWithGreenNodeConnectTimes.isEmpty()) {
        continue;
      }
      this.printDurationTimes(entry.getKey(), "Wrapper IAM (green token) connection time (ms) to Green",
          entry.getValue().greenDirectIamIpWithGreenNodeConnectTimes, bgTriggerTime);
    }

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {
      if (entry.getValue().blueWrapperExecuteTimes.isEmpty()) {
        continue;
      }
      this.printDurationTimes(entry.getKey(), "Wrapper execution time (ms) to Blue",
          entry.getValue().blueWrapperExecuteTimes, bgTriggerTime);
    }

    for (Entry<String, BlueGreenResults> entry : sortedEntries) {
      if (entry.getValue().greenWrapperExecuteTimes.isEmpty()) {
        continue;
      }
      this.printDurationTimes(entry.getKey(), "Wrapper execution time (ms) to Green",
          entry.getValue().greenWrapperExecuteTimes, bgTriggerTime);
    }
  }

  private String getFormattedNanoTime(AtomicLong timeNanoAtomic, long timeZeroNano) {
    return timeNanoAtomic.get() == 0
        ? "-"
        : String.format("%d ms", TimeUnit.NANOSECONDS.toMillis(timeNanoAtomic.get() - timeZeroNano));
  }

  private void printNodeStatusTimes(String node, BlueGreenResults results, long timeZeroNano) {

    Map<String, Long> statusMap = new HashMap<>();
    statusMap.putAll(results.blueStatusTime);
    statusMap.putAll(results.greenStatusTime);

    AsciiTable metricsTable = new AsciiTable();
    metricsTable.addRule();
    metricsTable.addRow("Status", "SOURCE", "TARGET");
    metricsTable.addRule();

    List<String> sortedStatusNames = statusMap.entrySet().stream()
        .sorted(Entry.comparingByValue())
        .map(Entry::getKey)
        .collect(Collectors.toList());

    for (String status : sortedStatusNames) {
      String sourceTime = results.blueStatusTime.containsKey(status)
          ? String.format("%d ms",
              TimeUnit.NANOSECONDS.toMillis(results.blueStatusTime.get(status) - timeZeroNano))
          : "";
      String targetTime = results.greenStatusTime.containsKey(status)
          ? String.format("%d ms",
              TimeUnit.NANOSECONDS.toMillis(results.greenStatusTime.get(status) - timeZeroNano))
          : "";

      metricsTable.addRow(status, sourceTime, targetTime);
    }

    metricsTable.addRule();
    LOGGER.finest("\n" + node + ":\n" + this.renderTable(metricsTable, true));
  }

  private void printDurationTimes(String node, String title,
      ConcurrentLinkedDeque<TimeHolder> times, long timeZeroNano) {

    AsciiTable metricsTable = new AsciiTable();
    metricsTable.addRule();
    metricsTable.addRow("Connect at (ms)", "Connect time/duration (ms)", "Error");
    metricsTable.addRule();

    long p99nano = this.getPercentile(
        times.stream().map(x -> x.endTime - x.startTime).collect(Collectors.toList()),
        99.0);
    long p99 = TimeUnit.NANOSECONDS.toMillis(p99nano);
    metricsTable.addRow("p99", p99, "");
    metricsTable.addRule();

    TimeHolder firstConnect = times.getFirst();
    metricsTable.addRow(
        TimeUnit.NANOSECONDS.toMillis(firstConnect.startTime - timeZeroNano),
        TimeUnit.NANOSECONDS.toMillis(firstConnect.endTime - firstConnect.startTime),
        firstConnect.error == null ? "" : firstConnect.error.substring(
            0, Math.min(firstConnect.error.length(), 100)).replace("\n", " ") + "...");

    for (TimeHolder timeHolder : times) {
      if (TimeUnit.NANOSECONDS.toMillis(timeHolder.endTime - timeHolder.startTime) > p99) {
        metricsTable.addRow(
            TimeUnit.NANOSECONDS.toMillis(timeHolder.startTime - timeZeroNano),
            TimeUnit.NANOSECONDS.toMillis(timeHolder.endTime - timeHolder.startTime),
            timeHolder.error == null ? "" : timeHolder.error.substring(
                0, Math.min(timeHolder.error.length(), 100)).replace("\n", " ") + "...");
      }
    }

    TimeHolder lastConnect = times.getLast();
    metricsTable.addRow(
        TimeUnit.NANOSECONDS.toMillis(lastConnect.startTime - timeZeroNano),
        TimeUnit.NANOSECONDS.toMillis(lastConnect.endTime - lastConnect.startTime),
        lastConnect.error == null ? "" : lastConnect.error.substring(
            0, Math.min(lastConnect.error.length(), 100)).replace("\n", " ") + "...");

    metricsTable.addRule();
    metricsTable.setTextAlignment(TextAlignment.CENTER);
    LOGGER.finest("\n" + node + ": " + title + "\n" + this.renderTable(metricsTable, false));
  }

  private long getPercentile(List<Long> input, double percentile) {
    if (input == null || input.isEmpty()) {
      return 0;
    }
    List<Long> sortedList = input.stream().sorted().collect(Collectors.toList());
    int rank = percentile == 0 ? 1 : (int) Math.ceil(percentile / 100.0 * input.size());
    return sortedList.get(rank - 1);
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

  private void assertTest() {
    long bgTriggerTime = results.values().stream()
        .map(blueGreenResults -> blueGreenResults.bgTriggerTime.get())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Can't get bgTriggerTime"));

    long maxGreenNodeChangeTime = this.results.values().stream()
        .map(blueGreenResults -> blueGreenResults.greenNodeChangeNameTime.get() == 0
            ? 0
            : TimeUnit.NANOSECONDS.toMillis(blueGreenResults.greenNodeChangeNameTime.get() - bgTriggerTime))
        .max(Comparator.comparingLong(x -> x))
        .orElse(0L);
    LOGGER.finest(() -> String.format("maxGreenNodeChangeTime: %d ms", maxGreenNodeChangeTime));

    long switchoverCompleteTime = this.results.values().stream().filter(x -> !x.greenStatusTime.isEmpty())
        .map(x -> x.greenStatusTime.getOrDefault("SWITCHOVER_COMPLETED", 0L))
        .map(x -> x == 0
            ? 0
            : TimeUnit.NANOSECONDS.toMillis(x - bgTriggerTime))
        .max(Comparator.comparingLong(x -> x))
        .orElse(0L);
    LOGGER.finest(() -> String.format("switchoverCompleteTime: %d ms", switchoverCompleteTime));

    assertNotEquals(0L, switchoverCompleteTime, "BG switchover hasn't completed.");
    assertTrue(switchoverCompleteTime >= maxGreenNodeChangeTime,
        "Green node changed name after SWITCHOVER_COMPLETED.");
  }
}
