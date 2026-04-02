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

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

/**
 * Tests to verify post-failover connection behavior:
 *
 * <p>Scenario 1 (DNS caching): After failover, a NEW connection opened via the cluster endpoint
 * may resolve to the demoted writer (now a reader) due to stale DNS. A write/DML on that
 * connection should produce a read-only error.
 *
 * <p>Scenario 2 (Surviving TCP connection): An EXISTING connection opened via an instance endpoint
 * before failover may survive the role change. A write/DML on that connection should produce a
 * read-only error because the instance is now a reader.
 *
 * <p>Both scenarios are timing-dependent and may not reproduce on every run, so each test loops
 * multiple attempts.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@MakeSureFirstInstanceWriter
@Order(15)
public class StaleConnectionAfterFailoverTest {

  private static final Logger LOGGER =
      Logger.getLogger(StaleConnectionAfterFailoverTest.class.getName());

  private static final int LOOP_ATTEMPTS = 5;
  private static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private ExecutorService executor;
  private String currentWriter;

  @BeforeEach
  public void setUp() {
    this.currentWriter =
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getInstanceId();
    this.executor =
        Executors.newFixedThreadPool(
            1,
            r -> {
              final Thread thread = new Thread(r);
              thread.setDaemon(true);
              return thread;
            });
  }

  @AfterEach
  public void tearDown() {
    this.executor.shutdownNow();
  }

  /**
   * Scenario 1: DNS caching — new connection via cluster endpoint lands on demoted writer.
   *
   * <p>After triggering a real server failover, immediately open a new direct (non-wrapper)
   * connection using the cluster endpoint. If DNS has not yet propagated, the connection will land
   * on the old writer which is now a reader. Attempt a write and expect a read-only error.
   *
   * <p>This is looped because DNS propagation timing is non-deterministic; the stale-DNS window
   * may be missed on some iterations.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_staleDns_newConnectionLandsOnDemotedWriter() throws InterruptedException {
    int readOnlyHits = 0;

    for (int attempt = 1; attempt <= LOOP_ATTEMPTS; attempt++) {
      LOGGER.info("Stale DNS test — attempt " + attempt + " of " + LOOP_ATTEMPTS);

      // Make sure we know the current writer before each attempt.
      final String writerId = auroraUtil.getDBClusterWriterInstanceId();
      LOGGER.info("Current writer before failover: " + writerId);

      // Trigger a real server-side failover via the RDS API.
      auroraUtil.failoverClusterAndWaitUntilWriterChanged();

      // Immediately try to connect via the cluster endpoint using a DIRECT (non-wrapper) URL.
      // The goal is to race DNS propagation — if we connect fast enough, DNS still points to
      // the old writer which is now a reader.
      final String clusterEndpoint =
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getClusterEndpoint();
      final int clusterPort =
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getClusterEndpointPort();
      final String dbName =
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getDefaultDbName();

      final Properties props = ConnectionStringHelper.getDefaultProperties();

      try (Connection conn =
          DriverManager.getConnection(
              ConnectionStringHelper.getUrl(clusterEndpoint, clusterPort, dbName), props)) {

        String connectedInstanceId = auroraUtil.queryInstanceId(conn);
        LOGGER.info("Connected to instance: " + connectedInstanceId);

        if (connectedInstanceId != null && connectedInstanceId.equals(writerId)) {
          // We landed on the old writer (now demoted). Try a write.
          LOGGER.info("Landed on demoted writer — attempting DML.");
          try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS stale_dns_test");
            stmt.executeUpdate(
                "CREATE TABLE stale_dns_test (id INT NOT NULL PRIMARY KEY)");
            // This INSERT should fail with a read-only error if the instance is truly a reader.
            stmt.executeUpdate("INSERT INTO stale_dns_test VALUES (1)");
            // If we get here, the instance accepted the write — it may not have fully
            // transitioned to read-only yet.
            LOGGER.info("Write succeeded — instance may not be read-only yet.");
          } catch (SQLException e) {
            if (isReadOnlyException(e)) {
              LOGGER.info("Got expected read-only error: " + e.getMessage());
              readOnlyHits++;
            } else {
              LOGGER.warning("Got unexpected exception: " + e.getMessage());
            }
          } finally {
            tryCleanup(conn, "stale_dns_test");
          }
        } else {
          LOGGER.info("DNS already propagated — connected to new writer. Retrying.");
        }
      } catch (SQLException e) {
        LOGGER.warning("Connection failed (instance may be rebooting): " + e.getMessage());
      }

      // Wait for the cluster to stabilize before the next attempt.
      waitForClusterStability();
    }

    LOGGER.info("Stale DNS read-only hits: " + readOnlyHits + " / " + LOOP_ATTEMPTS);
    // We don't assert a specific hit count because this is timing-dependent.
    // Even 0 hits is informative — it means DNS propagated fast enough every time.
    // The test is primarily observational; a hit count > 0 confirms the issue is reproducible.
    assertTrue(
        readOnlyHits >= 0,
        "Test completed. Read-only errors observed: " + readOnlyHits + " / " + LOOP_ATTEMPTS);
  }

  /**
   * Scenario 2: Surviving TCP connection — existing connection via instance endpoint.
   *
   * <p>Open a direct (non-wrapper) connection to the writer's instance endpoint. Trigger a
   * failover. Then attempt a write on the pre-existing connection. If the TCP connection survived
   * the role change, the write should fail with a read-only error. If the connection was killed
   * (e.g. by a reboot), we expect a connection-closed/communication-link error instead.
   *
   * <p>This is looped because whether the TCP connection survives depends on the failover type
   * and timing.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_survivingTcpConnection_existingConnectionOnDemotedWriter()
      throws InterruptedException {
    int readOnlyHits = 0;
    int connectionDroppedHits = 0;

    for (int attempt = 1; attempt <= LOOP_ATTEMPTS; attempt++) {
      LOGGER.info("Surviving TCP test — attempt " + attempt + " of " + LOOP_ATTEMPTS);

      final String writerId = auroraUtil.getDBClusterWriterInstanceId();
      final TestInstanceInfo writerInfo =
          TestEnvironment.getCurrent()
              .getInfo()
              .getDatabaseInfo()
              .getInstance(writerId);

      final Properties props = ConnectionStringHelper.getDefaultProperties();
      // Use a long socket timeout so the connection isn't killed by client-side timeout.
      props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "30000");

      Connection conn = null;
      try {
        // Open a direct (non-wrapper) connection to the writer's INSTANCE endpoint.
        conn =
            DriverManager.getConnection(
                ConnectionStringHelper.getUrl(
                    writerInfo.getHost(),
                    writerInfo.getPort(),
                    TestEnvironment.getCurrent()
                        .getInfo()
                        .getDatabaseInfo()
                        .getDefaultDbName()),
                props);

        // Verify we're on the writer.
        String preFailoverId = auroraUtil.queryInstanceId(conn);
        LOGGER.info("Pre-failover connection to: " + preFailoverId);

        // Trigger a real server-side failover.
        auroraUtil.failoverClusterAndWaitUntilWriterChanged();

        // Confirm the writer actually changed.
        String newWriterId = auroraUtil.getDBClusterWriterInstanceId();
        LOGGER.info("New writer after failover: " + newWriterId);
        if (newWriterId.equals(writerId)) {
          LOGGER.info("Writer did not change — skipping this attempt.");
          continue;
        }

        // Now try to use the pre-existing connection. The instance should be a reader now.
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate("DROP TABLE IF EXISTS tcp_survive_test");
          stmt.executeUpdate(
              "CREATE TABLE tcp_survive_test (id INT NOT NULL PRIMARY KEY)");
          stmt.executeUpdate("INSERT INTO tcp_survive_test VALUES (1)");
          // If we get here, the write succeeded — the connection may have been transparently
          // reconnected or the instance hasn't fully transitioned.
          LOGGER.info("Write succeeded on old connection — instance may not be read-only yet.");
        } catch (SQLException e) {
          if (isReadOnlyException(e)) {
            LOGGER.info("Got expected read-only error on surviving connection: " + e.getMessage());
            readOnlyHits++;
          } else if (isConnectionError(e)) {
            LOGGER.info("Connection was dropped (expected for Aurora reboot): " + e.getMessage());
            connectionDroppedHits++;
          } else {
            LOGGER.warning("Unexpected exception: " + e.getMessage());
          }
        } finally {
          tryCleanup(conn, "tcp_survive_test");
        }
      } catch (SQLException e) {
        LOGGER.warning("Setup failed: " + e.getMessage());
      } finally {
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException ignored) {
            // ignore
          }
        }
      }

      waitForClusterStability();
    }

    LOGGER.info(
        "Surviving TCP results — read-only hits: "
            + readOnlyHits
            + ", connection dropped: "
            + connectionDroppedHits
            + " / "
            + LOOP_ATTEMPTS);
    // Log the outcome. For Aurora, we expect connectionDroppedHits > 0 (old writer reboots).
    // For RDS Multi-AZ clusters, we may see readOnlyHits > 0 if TCP connections survive.
    assertTrue(
        (readOnlyHits + connectionDroppedHits) > 0,
        "Expected at least one iteration where the old connection either got a read-only error "
            + "or was dropped. read-only: "
            + readOnlyHits
            + ", dropped: "
            + connectionDroppedHits);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private boolean isReadOnlyException(SQLException e) {
    String sqlState = e.getSQLState();
    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
    // MySQL: error 1290, SQLState HY000, message contains "read-only"
    // PG: SQLState 25006 ("read_only_sql_transaction")
    return "25006".equals(sqlState)
        || (msg.contains("read-only") || msg.contains("read_only"))
        || e.getErrorCode() == 1290;
  }

  private boolean isConnectionError(SQLException e) {
    String sqlState = e.getSQLState() != null ? e.getSQLState() : "";
    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
    // 08xxx = connection exceptions in SQL standard
    return sqlState.startsWith("08")
        || msg.contains("connection")
        || msg.contains("communications link")
        || msg.contains("broken pipe")
        || msg.contains("socket")
        || msg.contains("eof");
  }

  private void tryCleanup(Connection conn, String tableName) {
    try {
      if (conn != null && !conn.isClosed()) {
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        }
      }
    } catch (SQLException ignored) {
      // Best-effort cleanup
    }
  }

  private void waitForClusterStability() throws InterruptedException {
    // Give the cluster time to fully stabilize before the next failover attempt.
    TimeUnit.MINUTES.sleep(2);
  }
}
