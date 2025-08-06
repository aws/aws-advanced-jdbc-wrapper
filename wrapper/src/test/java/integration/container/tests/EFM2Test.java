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
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(17)
public class EFM2Test {

  private static final Logger LOGGER = Logger.getLogger(integration.container.tests.EFM2Test.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected ExecutorService executor;

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void test_efmNetworkFailureDetection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "120000");
    props.setProperty(PropertyDefinition.PLUGINS.name, "efm2");

    String url = ConnectionStringHelper.getWrapperUrl();
    try (final Connection conn = DriverManager.getConnection(url, props)) {
      String instanceId = auroraUtil.queryInstanceId(conn);
      Statement stmt = conn.createStatement();

      auroraUtil.simulateTemporaryFailure(executor, instanceId, 10000, 120000);
      long startNs = System.nanoTime();
      try {
        stmt.executeQuery(getSleepSql(120));
        fail("Sleep query should have failed");
      } catch (SQLException e) {
        long endNs = System.nanoTime();
        long durationSec = TimeUnit.NANOSECONDS.toSeconds(endNs - startNs);
        assertTrue(durationSec > 20000 && durationSec < 120000);
      }
    }
  }

  private String getSleepSql(final int seconds) {
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
