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

import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestDriver;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@EnableOnNumOfInstances(min = 2)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
public class ReadOnlyEndpointConnectionPluginTests {
  private static final String MYSQL_SLEEP = "select sleep(60)";
  private static final String PG_SLEEP = "select pg_sleep(60)";
  private static final int REPETITIONS = 50;

  @TestTemplate
  @EnableOnTestDriver(TestDriver.MYSQL)
  public void testMySqlReadOnlyEndpointConnectionPluginQueryTimeout() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    props.setProperty(PropertyDefinition.PLUGINS.name, "readOnlyEndpoint");

    String url = ConnectionStringHelper.getWrapperReaderClusterUrl();

    for (int i = 0; i < REPETITIONS; i++) {
      try (final Connection conn = DriverManager.getConnection(url, props)) {
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(1);
        stmt.executeQuery(MYSQL_SLEEP);
      } catch (MySQLTimeoutException e) {
        // ignore
      }
    }
  }

  @TestTemplate
  @EnableOnTestDriver(TestDriver.PG)
  public void testPostgreSqlReadOnlyEndpointConnectionPluginQueryTimeout() throws SQLException {
    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    props.setProperty(PropertyDefinition.PLUGINS.name, "readOnlyEndpoint");

    String url = ConnectionStringHelper.getWrapperReaderClusterUrl();

    for (int i = 0; i < REPETITIONS; i++) {
      try (final Connection conn = DriverManager.getConnection(url, props)) {
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(1);
        stmt.executeQuery(PG_SLEEP);
      } catch (Exception e) {
        if (!e.getMessage().contains("canceling statement due to user request")) {
          throw e;
        }
      }
    }
  }
}
