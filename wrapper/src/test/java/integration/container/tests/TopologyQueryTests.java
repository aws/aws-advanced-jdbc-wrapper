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

import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;

@TestMethodOrder(MethodOrderer.MethodName.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
public class TopologyQueryTests {
  private static final Logger LOGGER = Logger.getLogger(BasicConnectivityTests.class.getName());
  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://atlas-postgres-2-instance-4.czygpppufgy4.us-east-2.rds.amazonaws.com:5432/test";
  protected static final AuroraTestUtility auroraUtil =
      new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void testConnection(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    String url =
        ConnectionStringHelper.getWrapperUrl(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getHost(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getPort(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));
//     List<String> res = auroraUtil.getAuroraInstanceIds();
    Statement stmt = conn.createStatement();
    stmt.executeQuery("select 1");
    ResultSet rs = stmt.getResultSet();
    rs.next();
    assertEquals(1, rs.getInt(1));

    conn.close();
  }
}
