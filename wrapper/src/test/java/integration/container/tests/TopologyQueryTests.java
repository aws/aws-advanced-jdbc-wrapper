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

import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.dialect.AuroraMysqlDialect;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterMysqlDialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterPgDialect;

@TestMethodOrder(MethodName.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
public class TopologyQueryTests {
  private static final Logger LOGGER = Logger.getLogger(TopologyQueryTests.class.getName());

  @TestTemplate
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @ExtendWith(TestDriverProvider.class)
  public void auroraTestTypes(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());
    List<String> expectedTypes;
    // Topology queries fail on docker containers, can't test topology for them
    // Also skip RDS, this is for Aurora
//     if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
//         == DatabaseEngineDeployment.DOCKER
//         || TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
//         == DatabaseEngineDeployment.RDS) {
//       return;
//     }

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

    String query = null;
    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      query = AuroraPgDialect.TOPOLOGY_QUERY;
      expectedTypes = Arrays.asList(
          "text",
          "bool",
          "float4",
          "float4",
          "timestamptz"
      );
    } else {
      query = AuroraMysqlDialect.TOPOLOGY_QUERY;
      expectedTypes = Arrays.asList(
          "VARCHAR",
          "BIGINT",
          "DOUBLE",
          "DOUBLE",
          "DATETIME"
      );
    }

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);
    ResultSet rs = stmt.getResultSet();
    int cols = rs.getMetaData().getColumnCount();
    List<String> columnTypes = new ArrayList<>();
    for (int i = 1; i <= cols; i++) {
      columnTypes.add(rs.getMetaData().getColumnTypeName(i));
    }
    assertEquals(expectedTypes, columnTypes);
    conn.close();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  public void auroraTestTimestamp(TestDriver testDriver) throws SQLException, ParseException {
    LOGGER.info(testDriver.toString());

    // Topology queries fail on docker containers, can't test topology for them
    // Also skip RDS, this is for Aurora
//     if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
//         == DatabaseEngineDeployment.DOCKER
//         || TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
//         == DatabaseEngineDeployment.RDS) {
//       return;
//     }

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

    String query = null;
    SimpleDateFormat format;
    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      query = AuroraPgDialect.TOPOLOGY_QUERY;
      format = new SimpleDateFormat("yyy-MM-dd HH:mm:ssX");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
    } else {
      query = AuroraMysqlDialect.TOPOLOGY_QUERY;
      format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    }

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);
    ResultSet rs = stmt.getResultSet();

    Date date;
    while (rs.next()) {
      date = format.parse(rs.getString(5));
      assertNotNull(date);
    }

    conn.close();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.RDS)
  @Disabled
  // TODO: Disabled due to RDS integration tests not being supported yet
  public void rdsTestTypes(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());

    // Topology queries fail on docker containers, can't test topology for them
    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
        == DatabaseEngineDeployment.DOCKER
        || TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
        == DatabaseEngineDeployment.AURORA) {
      return;
    }

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
    List<String> expectedTypes;
    String query = null;
    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      query = RdsMultiAzDbClusterPgDialect.TOPOLOGY_QUERY;
      expectedTypes = Arrays.asList(
          "text",
          "text",
          "int4"
      );
    } else {
      query = RdsMultiAzDbClusterMysqlDialect.TOPOLOGY_QUERY;
      expectedTypes = Arrays.asList(
          "INT",
          "VARCHAR",
          "INT"
      );
    }

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);
    ResultSet rs = stmt.getResultSet();
    int cols = rs.getMetaData().getColumnCount();
    List<String> columnTypes = new ArrayList<>();

    for (int i = 1; i <= cols; i++) {
      columnTypes.add(rs.getMetaData().getColumnTypeName(i));
    }
    assertEquals(expectedTypes, columnTypes);
    conn.close();
  }
}
