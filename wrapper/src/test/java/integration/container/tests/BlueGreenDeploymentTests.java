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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.sql.SQLSyntaxErrorException;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPlugin;
import software.amazon.jdbc.util.RdsUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@EnableOnTestFeature(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE)
@EnableOnDatabaseEngine(DatabaseEngine.MYSQL)
@Order(16)
public class BlueGreenDeploymentTests {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenDeploymentTests.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected static final RdsUtils rdsUtil = new RdsUtils();

  private static final String MYSQL_BG_STATUS_QUERY =
      "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, blue_green_deployment"
      + " FROM mysql.rds_topology";

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
    public final ConcurrentHashMap<String, Long> blueStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, Long> greenStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentLinkedDeque<TimeHolder> blueWrapperConnectTimes = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> blueWrapperExecuteTimes = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<TimeHolder> greenWrapperExecuteTimes = new ConcurrentLinkedDeque<>();
  }

  private final BlueGreenResults results = new BlueGreenResults();

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void test_Switchover(TestDriver testDriver) throws SQLException, InterruptedException {

    this.results.startTime.set(System.nanoTime());

    final AtomicBoolean stop = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(10);
    final CountDownLatch finishLatch = new CountDownLatch(9);

    final ArrayList<Thread> threads = new ArrayList<>();

    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    final TestInstanceInfo testInstance = info.getDatabaseInfo().getInstances().get(0);
    final String dbName = info.getDatabaseInfo().getDefaultDbName();

    final List<String> topologyInstances = this.getBlueGreenEndpoints(info.getBlueGreenDeploymentId());

    for (String host : topologyInstances) {
      final String hostId = host.substring(0, host.indexOf('.'));
      assertNotNull(hostId);

      if (rdsUtil.isNoPrefixInstance(host)) {
        threads.add(getDirectBlueConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getDirectBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueExecutingConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueNewConnectionMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getBlueDnsMonitoringThread(
            hostId, host, startLatch, stop, finishLatch));
      }
      if (rdsUtil.isGreenInstance(host)) {
        threads.add(getDirectGreenTopologyMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperGreenConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getGreenDnsMonitoringThread(
            hostId, host, startLatch, stop, finishLatch));
      }
    }

    threads.add(getBlueGreenSwitchoverTriggerThread(
        info.getBlueGreenDeploymentId(), startLatch, stop, finishLatch, results));

    assertEquals(startLatch.getCount(), threads.size());
    assertEquals(finishLatch.getCount(), threads.size() - 1);

    threads.forEach(Thread::start);
    LOGGER.finest("All threads started.");

    finishLatch.await(5, TimeUnit.MINUTES);
    LOGGER.finest("Stopping all threads...");
    stop.set(true);
    TimeUnit.SECONDS.sleep(5);
    LOGGER.finest("Interrupting all threads...");
    threads.forEach(Thread::interrupt);
    TimeUnit.SECONDS.sleep(5);

    assertTrue(results.bgTriggerTime.get() > 0);

    // Report results

    LOGGER.finest("Test is over.");

    LOGGER.finest(String.format("startTime: %d ms",
        TimeUnit.NANOSECONDS.toMillis(results.startTime.get() - results.bgTriggerTime.get())));
    LOGGER.finest(String.format("threadsSyncTime: %d ms",
        TimeUnit.NANOSECONDS.toMillis(results.threadsSyncTime.get() - results.bgTriggerTime.get())));
    LOGGER.finest("bgTriggerTime: 0 (T0) ms");

    if (results.directBlueIdleLostConnectionTime.get() == 0) {
      LOGGER.finest("directBlueIdleLostConnectionTime: -");
    } else {
      LOGGER.finest(String.format("directBlueIdleLostConnectionTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(
              results.directBlueIdleLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.directBlueLostConnectionTime.get() == 0) {
      LOGGER.finest("directBlueLostConnectionTime (SELECT 1): -");
    } else {
      LOGGER.finest(String.format("directBlueLostConnectionTime (SELECT 1): %d ms",
          TimeUnit.NANOSECONDS.toMillis(
              results.directBlueLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.wrapperBlueIdleLostConnectionTime.get() == 0) {
      LOGGER.finest("wrapperBlueIdleLostConnectionTime: -");
    } else {
      LOGGER.finest(String.format("wrapperBlueIdleLostConnectionTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(
              results.wrapperBlueIdleLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.wrapperGreenLostConnectionTime.get() == 0) {
      LOGGER.finest("wrapperGreenLostConnectionTime (SELECT 1): -");
    } else {
      LOGGER.finest(String.format("wrapperGreenLostConnectionTime (SELECT 1): %d ms",
          TimeUnit.NANOSECONDS.toMillis(
              results.wrapperGreenLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.dnsBlueChangedTime.get() == 0) {
      LOGGER.finest("dnsBlueChangedTime: -");
    } else {
      LOGGER.finest(String.format("dnsBlueChangedTime: %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(results.dnsBlueChangedTime.get() - results.bgTriggerTime.get()),
          (results.dnsBlueError == null ? "" : ", error: " + results.dnsBlueError)));
    }

    if (results.dnsGreenRemovedTime.get() == 0) {
      LOGGER.finest("dnsGreenRemovedTime: -");
    } else {
      LOGGER.finest(String.format("dnsGreenRemovedTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.dnsGreenRemovedTime.get() - results.bgTriggerTime.get())));
    }

    if (results.blueWrapperConnectTimes.isEmpty()) {
      LOGGER.finest("blueWrapperConnectTimes: - empty");
    } else {
      TimeHolder firstConnect = results.blueWrapperConnectTimes.getFirst();
      LOGGER.finest(String.format("blueWrapperConnectTimes [first, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(firstConnect.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(firstConnect.endTime - firstConnect.startTime),
          (firstConnect.error == null ? "" : ", error: " + firstConnect.error)));

      results.blueWrapperConnectTimes.stream()
          .sorted(Comparator.comparingLong(x -> x.startTime))
          .filter(x -> x.holdNano > 0 || x.error != null)
          .forEach(x -> LOGGER.finest(String.format(
              "blueWrapperConnectTimes [starting at %d ms]: duration %d ms, holdTime: %d ms %s",
              TimeUnit.NANOSECONDS.toMillis(x.startTime - results.bgTriggerTime.get()),
              TimeUnit.NANOSECONDS.toMillis(x.endTime - x.startTime),
              TimeUnit.NANOSECONDS.toMillis(x.holdNano),
              (x.error == null ? "" : ", error: " + x.error))));

      TimeHolder lastConnect = results.blueWrapperConnectTimes.getLast();
      LOGGER.finest(String.format("blueWrapperConnectTimes [last, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(lastConnect.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(lastConnect.endTime - lastConnect.startTime),
          (lastConnect.error == null ? "" : ", error: " + lastConnect.error)));
    }

    if (results.blueWrapperExecuteTimes.isEmpty()) {
      LOGGER.finest("blueWrapperExecuteTimes: - empty");
    } else {
      TimeHolder firstExecute = results.blueWrapperExecuteTimes.getFirst();
      LOGGER.finest(String.format("blueWrapperExecuteTimes [first, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(firstExecute.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(firstExecute.endTime - firstExecute.startTime),
          (firstExecute.error == null ? "" : ", error: " + firstExecute.error)));

      results.blueWrapperExecuteTimes.stream()
          .sorted(Comparator.comparingLong(x -> x.startTime))
          .filter(x -> x.holdNano > 0 || x.error != null)
          .forEach(x -> LOGGER.finest(String.format(
              "blueWrapperExecuteTimes [starting at %d ms]: duration %d ms, holdTime: %d ms %s",
              TimeUnit.NANOSECONDS.toMillis(x.startTime - results.bgTriggerTime.get()),
              TimeUnit.NANOSECONDS.toMillis(x.endTime - x.startTime),
              TimeUnit.NANOSECONDS.toMillis(x.holdNano),
              (x.error == null ? "" : ", error: " + x.error))));

      TimeHolder lastExecute = results.blueWrapperExecuteTimes.getLast();
      LOGGER.finest(String.format("blueWrapperExecuteTimes [last, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(lastExecute.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(lastExecute.endTime - lastExecute.startTime),
          (lastExecute.error == null ? "" : ", error: " + lastExecute.error)));
    }

    if (results.greenWrapperExecuteTimes.isEmpty()) {
      LOGGER.finest("greenWrapperExecuteTimes: - empty");
    } else {
      TimeHolder firstExecute = results.greenWrapperExecuteTimes.getFirst();
      LOGGER.finest(String.format("greenWrapperExecuteTimes [first, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(firstExecute.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(firstExecute.endTime - firstExecute.startTime),
          (firstExecute.error == null ? "" : ", error: " + firstExecute.error)));

      results.greenWrapperExecuteTimes.stream()
          .sorted(Comparator.comparingLong(x -> x.startTime))
          .filter(x -> x.holdNano > 0 || x.error != null)
          .forEach(x -> LOGGER.finest(String.format(
              "greenWrapperExecuteTimes [starting at %d ms]: duration %d ms, holdTime: %d ms %s",
              TimeUnit.NANOSECONDS.toMillis(x.startTime - results.bgTriggerTime.get()),
              TimeUnit.NANOSECONDS.toMillis(x.endTime - x.startTime),
              TimeUnit.NANOSECONDS.toMillis(x.holdNano),
              (x.error == null ? "" : ", error: " + x.error))));

      TimeHolder lastExecute = results.greenWrapperExecuteTimes.getLast();
      LOGGER.finest(String.format("greenWrapperExecuteTimes [last, starting at %d ms]: duration %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(lastExecute.startTime - results.bgTriggerTime.get()),
          TimeUnit.NANOSECONDS.toMillis(lastExecute.endTime - lastExecute.startTime),
          (lastExecute.error == null ? "" : ", error: " + lastExecute.error)));
    }

    results.blueStatusTime.entrySet().stream()
        .sorted(Comparator.comparingLong(Entry::getValue))
        .forEach(x -> LOGGER.finest(String.format("[blue] %s: %d ms",
            x.getKey(), TimeUnit.NANOSECONDS.toMillis(x.getValue() - results.bgTriggerTime.get()))));
    if (results.blueStatusTime.isEmpty()) {
      LOGGER.finest("[blue] - empty");
    }

    results.greenStatusTime.entrySet().stream()
        .sorted(Comparator.comparingLong(Entry::getValue))
        .forEach(x -> LOGGER.finest(String.format("[green] %s: %d ms",
            x.getKey(), TimeUnit.NANOSECONDS.toMillis(x.getValue() - results.bgTriggerTime.get()))));
    if (results.greenStatusTime.isEmpty()) {
      LOGGER.finest("[green] - empty");
    }

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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest("[DirectBlueIdleConnectivity] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectBlueIdleConnectivity] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.directBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[DirectBlueIdleConnectivity] thread exception: " + throwable);
            results.directBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectBlueIdleConnectivity] thread unhandled exception: ", exception);
        fail("[DirectBlueConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[DirectBlueIdleConnectivity] thread is completed.");
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest("[DirectBlueConnectivity] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectBlueConnectivity] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            final Statement statement = conn.createStatement();
            final ResultSet result = statement.executeQuery("SELECT 1");
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[DirectBlueConnectivity] thread exception: " + throwable);
            results.directBlueLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectBlueConnectivity] thread unhandled exception: ", exception);
        fail("[DirectBlueConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[DirectBlueConnectivity] thread is completed.");
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        RdsHostListProvider.CLUSTER_ID.set(props, TEST_CLUSTER_ID);

        conn = openConnectionWithRetry(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, "bg"),
            props);
        LOGGER.finest("[WrapperBlueIdle] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueIdle] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[WrapperBlueIdle] thread exception: " + throwable);
            results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueIdle] thread unhandled exception: ", exception);
        fail("[WrapperBlueIdle] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueIdle] thread is completed.");
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        RdsHostListProvider.CLUSTER_ID.set(props, TEST_CLUSTER_ID);

        conn = DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, "bg"),
            props);

        BlueGreenConnectionPlugin bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
        assertNotNull(bgPlugin);

        Statement statement = conn.createStatement();
        LOGGER.finest("[WrapperBlueExecute] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueExecute] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            ResultSet rs = statement.executeQuery("SELECT sleep(5)");
            endTime = System.nanoTime();
            results.blueWrapperExecuteTimes.add(
                new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano()));
          } catch (SQLException throwable) {
            //LOGGER.finest("[WrapperBlueExecute] thread exception: " + throwable);
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
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueExecute] thread unhandled exception: ", exception);
        fail("[WrapperBlueExecute] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueExecute] thread is completed.");
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      BlueGreenConnectionPlugin bgPlugin = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        RdsHostListProvider.CLUSTER_ID.set(props, TEST_CLUSTER_ID);

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueNewConnection] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            conn = DriverManager.getConnection(
                ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, "bg"),
                props);
            endTime = System.nanoTime();

            bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
            assertNotNull(bgPlugin);

            results.blueWrapperConnectTimes.add(new TimeHolder(startTime, endTime, bgPlugin.getHoldTimeNano()));

          } catch (SQLTimeoutException sqlTimeoutException) {
            LOGGER.finest("[WrapperBlueNewConnection] (SQLTimeoutException) thread exception: " + sqlTimeoutException);
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
            LOGGER.log(Level.FINEST, "[WrapperBlueNewConnection] thread exception: ", throwable);
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

          try {
            if (conn != null) {
              conn.close();
              conn = null;
            }
          } catch (SQLException sqlException) {
            // do nothing
          }

          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueNewConnection] thread unhandled exception: ", exception);
        fail("[WrapperBlueNewConnection] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueNewConnection] thread is completed.");
      }
    });
  }

  // Green node
  // Check: DNS record presence
  // Expect: DNS record becomes deleted while/after switchover
  private Thread getGreenDnsMonitoringThread(
      final String hostId,
      final String host,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        final String ip = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[GreenDNS] %s -> %s", host, ip));

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
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, "[GreenDNS] thread unhandled exception: ", e);
        fail(String.format("[GreenDNS] %s: thread unhandled exception: %s", hostId, e));
      } finally {
        finishLatch.countDown();
        LOGGER.finest("[GreenDNS] thread is completed.");
      }
    });
  }

  // Blue DNS
  // Check IP address change time
  // Can terminate for itself
  public Thread getBlueDnsMonitoringThread(
      final String hostId,
      final String host,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        final String originalIp = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[BlueDNS] %s -> %s", host, originalIp));

        while (!stop.get()) {
          TimeUnit.SECONDS.sleep(1);
          try {
            String currentIp = InetAddress.getByName(host).getHostAddress();
            if (!currentIp.equals(originalIp)) {
              results.dnsBlueChangedTime.set(System.nanoTime());
              LOGGER.finest(() -> String.format("[BlueDNS] %s -> %s", host, currentIp));
              break;
            }
          } catch (UnknownHostException unknownHostException) {
            LOGGER.finest(() -> String.format("Error: %s", unknownHostException));
            results.dnsBlueError = unknownHostException.getMessage();
            results.dnsBlueChangedTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException e) {
        // do nothing
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, "[BlueDNS] thread unhandled exception: ", e);
        fail(String.format("[BlueDNS] %s: thread unhandled exception: %s", hostId, e));
      } finally {
        finishLatch.countDown();
        LOGGER.finest("[BlueDNS] thread is completed.");
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      String noPrefixHostId = rdsUtil.removeGreenInstancePrefix(hostId);
      final HashMap<String /* endpoint */, String /* BG status */> statusByHost = new HashMap<>();
      Connection conn = null;
      boolean sqlSyntaxExceptionReported = false; // TODO: debug remove

      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(url, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectGreenTopology] connection open: %s", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectGreenTopology] Starting BG statuses monitoring through " + hostId);

        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);

        while (!stop.get() && System.nanoTime() < endTime) {
          if (conn == null) {
            conn = openConnectionWithRetry(
                ConnectionStringHelper.getUrl(url, port, dbName),
                props);
            LOGGER.finest(String.format("[DirectGreenTopology] connection re-open: %s", hostId));
          }

          try {
            final Statement statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery(MYSQL_BG_STATUS_QUERY);
            while (rs.next()) {
              String queryHostId = rs.getString("hostId");
              String queryNewStatus = rs.getString("blue_green_deployment");
              boolean isGreen = rdsUtil.isGreenInstance(queryHostId);

              String noPrefixQueryHostId = rdsUtil.removeGreenInstancePrefix(queryHostId);
              String oldStatus = statusByHost.get(queryHostId);

              if (noPrefixHostId.equalsIgnoreCase(noPrefixQueryHostId)) {
                if (oldStatus == null) {
                  statusByHost.put(queryHostId, queryNewStatus);
                  LOGGER.finest(
                      String.format("[DirectGreenTopology] status changed for %s: %s", queryHostId, queryNewStatus));
                  if (isGreen) {
                    results.greenStatusTime.put(queryNewStatus, System.nanoTime());
                  } else {
                    results.blueStatusTime.put(queryNewStatus, System.nanoTime());
                  }
                } else if (!oldStatus.equalsIgnoreCase(queryNewStatus)) {
                  if (isGreen) {
                    results.greenStatusTime.put(queryNewStatus, System.nanoTime());
                  } else {
                    results.blueStatusTime.put(queryNewStatus, System.nanoTime());
                  }
                  statusByHost.put(queryHostId, queryNewStatus);
                  LOGGER.finest(
                      String.format("[DirectGreenTopology] status changed for %s: %s", queryHostId, queryNewStatus));
                }
              }
            }
            TimeUnit.MILLISECONDS.sleep(100);

          } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
            if (!sqlSyntaxExceptionReported) {
              for (Map.Entry<String, String> entry : statusByHost.entrySet()) {
                statusByHost.put(entry.getKey(), "<TABLE_DISAPPEARED>");
              }
              results.greenStatusTime.put("<TABLE_DISAPPEARED>", System.nanoTime());
              results.blueStatusTime.put("<TABLE_DISAPPEARED>", System.nanoTime());

              LOGGER.finest(String.format("[DirectGreenTopology] %s: thread SQLSyntaxErrorException: %s", hostId,
                  sqlSyntaxErrorException));

              // Let allow this thread run another 15s.
              // We're not expecting any more changes here since topology table is already deleted.
              endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);

              sqlSyntaxExceptionReported = true;
            }
            // don't close connection here
            // ignore this exception
          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[DirectGreenTopology] %s: thread exception: %s", hostId, throwable));
            try {
              if (conn != null && !conn.isClosed()) {
                conn.close();
              }
            } catch (Exception ex) {
              // do nothing
            }
            conn = null;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectGreenTopology] thread unhandled exception: ", exception);
        fail(String.format("[DirectGreenTopology] %s: thread unhandled exception: %s", hostId, exception));
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest(String.format("[DirectGreenTopology] %s: thread is completed.", hostId));
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
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        RdsHostListProvider.CLUSTER_ID.set(props, TEST_CLUSTER_ID);

        conn = openConnectionWithRetry(
            ConnectionStringHelper.getWrapperUrlWithPlugins(host, port, dbName, "bg"),
            props);
        LOGGER.finest("[WrapperGreenConnectivity] connection is open.");

        BlueGreenConnectionPlugin bgPlugin = conn.unwrap(BlueGreenConnectionPlugin.class);
        assertNotNull(bgPlugin);

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperGreenConnectivity] Starting connectivity monitoring " + hostId);

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
            LOGGER.finest(
                "[WrapperGreenConnectivity] (SQLTimeoutException) thread exception: " + sqlTimeoutException);
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
            LOGGER.finest("[WrapperGreenConnectivity] thread exception: " + throwable);
            results.wrapperGreenLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperGreenConnectivity] thread unhandled exception: ", exception);
        fail("[WrapperGreenConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperGreenConnectivity] thread is completed.");
      }
    });
  }

  // RDS API, trigger BG switchover
  // Can terminate for itself
  private Thread getBlueGreenSwitchoverTriggerThread(
      final String blueGreenId,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);
        results.threadsSyncTime.set(System.nanoTime());

        TimeUnit.SECONDS.sleep(30);
        auroraUtil.switchoverBlueGreenDeployment(blueGreenId);
        //LOGGER.finest("Dry run: switchoverBlueGreenDeployment");
        results.bgTriggerTime.set(System.nanoTime());

      } catch (InterruptedException e) {
        // do nothing
      } finally {
        finishLatch.countDown();
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

  private List<String> getBlueGreenEndpoints(final String blueGreenId) {

    BlueGreenDeployment blueGreenDeployment = auroraUtil.getBlueGreenDeployment(blueGreenId);
    if (blueGreenDeployment == null) {
      throw new RuntimeException("BG not found: " + blueGreenId);
    }
    DBInstance blueInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.source());
    if (blueInstance == null) {
      throw new RuntimeException("Blue instance not found.");
    }
    DBInstance greenInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.target());
    if (greenInstance == null) {
      throw new RuntimeException("Blue instance not found.");
    }
    return Arrays.asList(blueInstance.endpoint().address(), greenInstance.endpoint().address());
  }
}
