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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.util.AuroraTestUtility;
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
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.dialect.AuroraMysqlDialect;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterMysqlDialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterPgDialect;

@TestMethodOrder(MethodName.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
public class TopologyQueryTests {
  private static final Logger LOGGER = Logger.getLogger(TopologyQueryTests.class.getName());
  private String query = null;

  @TestTemplate
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA)
  @ExtendWith(TestDriverProvider.class)
  public void testAuroraTypes(TestDriver testDriver) throws SQLException {
    LOGGER.info(testDriver.toString());
    List<String> expectedTypes;

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

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.MYSQL) {
      props.setProperty("permitMysqlScheme", "1");
    }

    LOGGER.finest("Connecting to " + url);

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      AuroraPgDialect dialect = new AuroraPgDialect();
      query = dialect.getTopologyQuery();
      expectedTypes = Arrays.asList(
          "text",
          "bool",
          "float4",
          "float4",
          "timestamptz"
      );
    } else {
      AuroraMysqlDialect dialect = new AuroraMysqlDialect();
      query = dialect.getTopologyQuery();
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
  @EnableOnNumOfInstances(min = 2)
  public void testAuroraTimestamp(TestDriver testDriver) throws SQLException, ParseException {
    LOGGER.info(testDriver.toString());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    // Get second instance since first one has null timestamps
    String url =
        ConnectionStringHelper.getWrapperUrl(
            testDriver,
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(1)
                .getHost(),
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(1)
                .getPort(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    LOGGER.finest("Connecting to " + url);

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.MYSQL) {
      props.setProperty("permitMysqlScheme", "1");
    }

    SimpleDateFormat format;
    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      AuroraPgDialect dialect = new AuroraPgDialect();
      query = dialect.getTopologyQuery();
      format = new SimpleDateFormat("yyy-MM-dd HH:mm:ssX");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
    } else {
      AuroraMysqlDialect dialect = new AuroraMysqlDialect();
      query = dialect.getTopologyQuery();
      format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    }

    final Connection conn = DriverManager.getConnection(url, props);
    assertTrue(conn.isValid(5));
    Statement stmt = conn.createStatement();
    stmt.executeQuery(query);
    ResultSet rs = stmt.getResultSet();

    Date date = null;
    while (rs.next()) {
      if (rs.getString(5) != null) {
        date = format.parse(rs.getString(5));
        assertNotNull(date);
      }
    }
    assertNotNull(date);

    conn.close();
  }

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  @EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.RDS)
  @Disabled
  // TODO: Disabled due to RdsMultiAz integration tests not being supported yet
  public void testRdsMultiAzTypes(TestDriver testDriver) throws SQLException {
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

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB
        && TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.MYSQL) {
      props.setProperty("permitMysqlScheme", "1");
    }

    List<String> expectedTypes;
    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.PG) {
      RdsMultiAzDbClusterPgDialect dialect = new RdsMultiAzDbClusterPgDialect();
      query = dialect.getTopologyQuery();
      expectedTypes = Arrays.asList(
          "text",
          "text",
          "int4"
      );
    } else {
      RdsMultiAzDbClusterMysqlDialect dialect = new RdsMultiAzDbClusterMysqlDialect();
      query = dialect.getTopologyQuery();
      expectedTypes = Arrays.asList(
          "VARCHAR",
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
