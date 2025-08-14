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
import static org.junit.jupiter.api.Assertions.fail;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
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
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
    DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE,
})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(17)
public class EFM2Test {
  private static final Logger LOGGER = Logger.getLogger(ReadWriteSplittingTests.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected ExecutorService executor = Executors.newFixedThreadPool(1, r -> {
    final Thread thread = new Thread(r);
    thread.setDaemon(true);
    return thread;
  });

  @BeforeEach
  public void setUpEach() {
    this.executor = Executors.newFixedThreadPool(1, r -> {
      final Thread thread = new Thread(r);
      thread.setDaemon(true);
      return thread;
    });
  }

  @AfterEach
  public void afterEach() {
    this.executor.shutdownNow();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  // TODO: test fails because EFM monitor's isValid call is freezing for MARIADB, investigate why
  @DisableOnTestDriver(TestDriver.MARIADB)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_efmNetworkFailureDetection() throws SQLException {
    int failureDelayMs = 10000;
    int maxDurationMs = 30000;

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, String.valueOf(maxDurationMs));
    props.setProperty(PropertyDefinition.PLUGINS.name, "efm2");
    props.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "5000");
    props.setProperty(HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "1");

    String url = ConnectionStringHelper.getProxyWrapperUrl();
    String instanceId = TestEnvironment.getCurrent()
        .getInfo()
        .getProxyDatabaseInfo()
        .getInstances()
        .get(0)
        .getInstanceId();
    try (final Connection conn = DriverManager.getConnection(url, props)) {
      Statement stmt = conn.createStatement();

      // Simulate network failure in the middle of the query. The simulated failure occurs after a small delay to allow
      // time for the statement to be sent and the monitoring connection to be opened.
      auroraUtil.simulateTemporaryFailure(executor, instanceId, failureDelayMs, maxDurationMs);
      long startNs = System.nanoTime();
      try {
        stmt.executeQuery(getSleepSql(TimeUnit.MILLISECONDS.toSeconds(maxDurationMs)));
        fail("Sleep query should have failed");
      } catch (SQLException e) {
        long endNs = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endNs - startNs);
        // EFM should detect network failure and abort the connection ~5-10 seconds after the query is sent
        assertTrue(durationMs > failureDelayMs && durationMs < maxDurationMs,
            String.format("Time before failure was not between %d and %d seconds, actual duration was %d seconds.",
                failureDelayMs, maxDurationMs, durationMs));
      }
    }
  }

  private String getSleepSql(final long seconds) {
    final DatabaseEngine databaseEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    switch (databaseEngine) {
      case PG:
        return String.format("SELECT pg_sleep(%d)", seconds);
      case MYSQL:
      case MARIADB:
        return String.format("SELECT sleep(%d)", seconds);
      default:
        throw new UnsupportedOperationException(databaseEngine.name());
    }
  }
}
