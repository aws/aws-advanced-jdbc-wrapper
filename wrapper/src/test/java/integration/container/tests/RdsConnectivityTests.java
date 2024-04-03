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
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnNumOfInstances(min = 2)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(2)
public class RdsConnectivityTests {

  private static final Logger LOGGER = Logger.getLogger(RdsConnectivityTests.class.getName());

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void test_WrapperConnectionReaderClusterWithEfmEnabled(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = new Properties();
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.PLUGINS.name, "efm");

    String url = ConnectionStringHelper.getWrapperReaderClusterUrl();
    LOGGER.finest("Connecting to " + url);

    try (final Connection conn = DriverManager.getConnection(url, props)) {
      assertTrue(conn.isValid(5));

      Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      ResultSet rs = stmt.getResultSet();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }
}
