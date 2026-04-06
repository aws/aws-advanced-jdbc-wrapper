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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.HIKARI})
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
@Order(16)
public class PooledConnectionAfterFailoverTest {

  private static final Logger LOGGER =
      Logger.getLogger(PooledConnectionAfterFailoverTest.class.getName());

  private static final int LOOP_ATTEMPTS = 5;
  private static final int POOL_SIZE = 5;
  private static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_1_instanceEndpoint_waitForDns() throws InterruptedException {
    runScenario("instance+waitDns", true, true);
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_2_instanceEndpoint_noWaitForDns() throws InterruptedException {
    runScenario("instance+noWaitDns", true, false);
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_3_clusterEndpoint_waitForDns() throws InterruptedException {
    runScenario("cluster+waitDns", false, true);
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_4_clusterEndpoint_noWaitForDns() throws InterruptedException {
    runScenario("cluster+noWaitDns", false, false);
  }

  private void runScenario(String label, boolean useInstanceEndpoint, boolean waitForDns)
      throws InterruptedException {
    int readOnlyHits = 0;
    int connectionErrorHits = 0;
    final String clusterId = TestEnvironment.getCurrent().getInfo().getRdsDbName();

    for (int attempt = 1; attempt <= LOOP_ATTEMPTS; attempt++) {
      LOGGER.info("[" + label + "] attempt " + attempt + " of " + LOOP_ATTEMPTS);

      final String writerId = auroraUtil.getDBClusterWriterInstanceId();
      final String jdbcUrl = buildJdbcUrl(writerId, useInstanceEndpoint);
      LOGGER.info("[" + label + "] writer=" + writerId + " url=" + jdbcUrl);

      HikariDataSource ds = createPool(jdbcUrl);

      try {
        warmPool(ds);

        if (waitForDns) {
          auroraUtil.failoverClusterAndWaitUntilWriterChanged();
        } else {
          auroraUtil.failoverClusterToTarget(clusterId, null);
          auroraUtil.waitUntilClusterHasRightState(clusterId, "failing-over");
          LOGGER.info("[" + label + "] cluster is failing-over — racing DNS.");
          long sleepMs = 1000 + (attempt * 1000L);
          LOGGER.info("[" + label + "] sleeping " + sleepMs + "ms before borrowing.");
          TimeUnit.MILLISECONDS.sleep(sleepMs);
        }

        if (waitForDns) {
          String newWriterId = auroraUtil.getDBClusterWriterInstanceId();
          LOGGER.info("[" + label + "] writer changed: " + writerId + " -> " + newWriterId);
          if (newWriterId.equals(writerId)) {
            LOGGER.info("[" + label + "] writer did not change — skipping.");
            continue;
          }
        }

        int totalBorrows = POOL_SIZE * 2;
        for (int i = 0; i < totalBorrows; i++) {
          try (Connection conn = ds.getConnection()) {
            String instanceId = null;
            try {
              instanceId = auroraUtil.queryInstanceId(conn);
            } catch (SQLException e) {
              LOGGER.info("[" + label + "] conn " + i + ": queryInstanceId failed — "
                  + e.getMessage());
              connectionErrorHits++;
              continue;
            }

            LOGGER.info("[" + label + "] conn " + i + ": connected to " + instanceId);

            try (Statement stmt = conn.createStatement()) {
              stmt.executeUpdate(
                  "CREATE TABLE IF NOT EXISTS pool_fo_test (id INT NOT NULL PRIMARY KEY)");
              stmt.executeUpdate(
                  "INSERT INTO pool_fo_test VALUES (" + (attempt * 100 + i) + ")");
              LOGGER.info("[" + label + "] conn " + i + ": write SUCCEEDED.");
            } catch (SQLException e) {
              if (isReadOnlyException(e)) {
                readOnlyHits++;
                LOGGER.info("[" + label + "] conn " + i + ": *** READ-ONLY *** — "
                    + e.getMessage());
              } else if (isConnectionError(e)) {
                connectionErrorHits++;
                LOGGER.info("[" + label + "] conn " + i + ": connection error — "
                    + e.getMessage());
              } else {
                LOGGER.warning("[" + label + "] conn " + i + ": unexpected — "
                    + e.getMessage());
              }
            } finally {
              tryCleanup(conn, "pool_fo_test");
            }
          } catch (SQLException e) {
            connectionErrorHits++;
            LOGGER.info("[" + label + "] conn " + i + ": borrow failed — " + e.getMessage());
          }
        }
      } finally {
        ds.close();
      }

      if (!waitForDns) {
        auroraUtil.waitUntilClusterHasRightState(clusterId, "available");
      }
      waitForClusterStability();
    }

    LOGGER.info("[" + label + "] FINAL — read-only: " + readOnlyHits
        + ", conn errors: " + connectionErrorHits + " / " + LOOP_ATTEMPTS);
    assertTrue(
        (readOnlyHits + connectionErrorHits) > 0,
        "[" + label + "] Expected at least one read-only or connection error. "
            + "read-only: " + readOnlyHits + ", conn errors: " + connectionErrorHits);
  }

  private String buildJdbcUrl(String writerId, boolean useInstanceEndpoint) {
    if (useInstanceEndpoint) {
      TestInstanceInfo writerInfo =
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(writerId);
      return ConnectionStringHelper.getUrl(
          writerInfo.getHost(),
          writerInfo.getPort(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    } else {
      return ConnectionStringHelper.getUrl(
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpointPort(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    }
  }

  private HikariDataSource createPool(String jdbcUrl) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    config.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    config.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(POOL_SIZE);
    config.setConnectionTestQuery(null);
    config.setMaxLifetime(600000);
    config.setIdleTimeout(600000);
    config.setConnectionTimeout(10000);
    return new HikariDataSource(config);
  }

  private void warmPool(HikariDataSource ds) throws InterruptedException {
    List<Connection> conns = new ArrayList<>();
    try {
      for (int i = 0; i < POOL_SIZE; i++) {
        conns.add(ds.getConnection());
      }
    } catch (SQLException e) {
      LOGGER.warning("Error warming pool: " + e.getMessage());
    } finally {
      for (Connection c : conns) {
        try { c.close(); } catch (SQLException ignored) { }
      }
    }
    Thread.sleep(500);
  }

  private boolean isReadOnlyException(SQLException e) {
    String sqlState = e.getSQLState();
    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
    return "25006".equals(sqlState)
        || msg.contains("read-only")
        || msg.contains("read_only")
        || e.getErrorCode() == 1290
        || e.getErrorCode() == 1836;
  }

  private boolean isConnectionError(SQLException e) {
    String sqlState = e.getSQLState() != null ? e.getSQLState() : "";
    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
    return sqlState.startsWith("08")
        || msg.contains("communications link")
        || msg.contains("connection")
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
    } catch (SQLException ignored) { }
  }

  private void waitForClusterStability() throws InterruptedException {
    TimeUnit.SECONDS.sleep(30);
  }
}
