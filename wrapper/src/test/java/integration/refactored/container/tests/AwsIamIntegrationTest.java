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

package integration.refactored.container.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestDriver;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestDriver;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnTestFeature;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.plugin.IamAuthConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.IAM)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
// MariaDb driver has no configuration parameters to force using 'mysql_clear_password'
// authentication that is essential for IAM. A proper user name and IAM token are passed to MariaDb
// driver however 'mysql_native_password' authentication is chosen by default.
// 'mysql_native_password' authentication sends a password (IAM token in this case) hash,
// rather than a clear password, to a DB server and that leads to 'Access denied' error.
// So taking that into account, all IAM tests are disabled for MariaDb driver.
public class AwsIamIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(AwsIamIntegrationTest.class.getName());

  @BeforeEach
  public void beforeEach() {
    IamAuthConnectionPlugin.clearCache();
  }

  /** Attempt to connect using the wrong database username. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_AwsIam_WrongDatabaseUsername() {
    final Properties props =
        initAwsIamProps(
            "WRONG_"
                + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername()
                + "_USER",
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));
  }

  /** Attempt to connect without specifying a database username. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_AwsIam_NoDatabaseUsername() {
    final Properties props =
        initAwsIamProps("", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));
  }

  /** Attempt to connect using IP address instead of a hostname. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_AwsIam_UsingIPAddress() throws UnknownHostException, SQLException {
    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");

    final String hostIp =
        hostToIP(
            TestEnvironment.getCurrent()
                .getInfo()
                .getDatabaseInfo()
                .getInstances()
                .get(0)
                .getEndpoint());
    props.setProperty(
        "iamHost",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());

    final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                hostIp,
                TestEnvironment.getCurrent()
                    .getInfo()
                    .getDatabaseInfo()
                    .getInstances()
                    .get(0)
                    .getEndpointPort(),
                TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
            props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /** Attempt to connect using valid database username/password & valid Amazon RDS hostname. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_AwsIam_ValidConnectionProperties() throws SQLException {
    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");

    final Connection conn =
        DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    conn.close();
  }

  /**
   * Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
   */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_AwsIam_ValidConnectionPropertiesNoPassword() throws SQLException {
    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "");
    final Connection conn =
        DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    conn.close();
  }

  /**
   * Attempts a valid connection followed by invalid connection without the AWS protocol in
   * Connection URL.
   */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  void test_AwsIam_NoAwsProtocolConnection() throws SQLException {
    final String dbConn = ConnectionStringHelper.getWrapperUrl();
    final Properties validProp =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");

    final Connection conn = DriverManager.getConnection(dbConn, validProp);
    conn.close();

    final Properties invalidProp =
        initAwsIamProps(
            "WRONG_" + TestEnvironment.getCurrent().getInfo().getIamUsername() + "_USER",
            "<anything>");

    Assertions.assertThrows(
        SQLException.class, () -> DriverManager.getConnection(dbConn, invalidProp));
  }

  /**
   * Attempts a valid connection followed by an invalid connection with Username in Connection URL.
   */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  void test_AwsIam_UserInConnStr() throws SQLException {
    final String dbConn = ConnectionStringHelper.getWrapperUrl();
    final Properties awsIamProp =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");
    awsIamProp.remove(PropertyDefinition.USER.name);

    final Connection validConn =
        DriverManager.getConnection(
            dbConn
                + (dbConn.contains("?") ? "&" : "?")
                + PropertyDefinition.USER.name
                + "="
                + TestEnvironment.getCurrent().getInfo().getIamUsername(),
            awsIamProp);
    Assertions.assertNotNull(validConn);
    Assertions.assertThrows(
        SQLException.class,
        () ->
            DriverManager.getConnection(
                dbConn
                    + (dbConn.contains("?") ? "&" : "?")
                    + PropertyDefinition.USER.name
                    + "="
                    + "WRONG_"
                    + TestEnvironment.getCurrent().getInfo().getIamUsername(),
                awsIamProp));
  }

  /**
   * Attempts a valid connection with a datasource and makes sure that the user and password
   * properties persist.
   */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  void test_AwsIam_UserAndPasswordPropertiesArePreserved() throws SQLException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerPropertyName("serverName");
    ds.setDatabasePropertyName("databaseName");

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");
    props.setProperty(
        "serverName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
    props.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceProperties(props);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isValid(10));
    }
  }

  protected Properties initAwsIamProps(String user, String password) {
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    props.setProperty(PropertyDefinition.USER.name, user);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    DriverHelper.setTcpKeepAlive(TestEnvironment.getCurrent().getCurrentDriver(), props, false);
    return props;
  }

  protected String hostToIP(String hostname) throws UnknownHostException {
    final InetAddress inet = InetAddress.getByName(hostname);
    return inet.getHostAddress();
  }
}
