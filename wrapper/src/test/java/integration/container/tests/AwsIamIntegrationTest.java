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

import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.TestDefaultRdsUtilities;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.plugin.iam.LightRdsUtility;
import software.amazon.jdbc.plugin.iam.RegularRdsUtility;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.IAM)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(3)
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
                .getHost());
    props.setProperty(
        "iamHost",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost());

    final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                hostIp,
                TestEnvironment.getCurrent()
                    .getInfo()
                    .getDatabaseInfo()
                    .getInstances()
                    .get(0)
                    .getPort(),
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
    ds.setServerName(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getHost());
    ds.setDatabase(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");
    ds.setTargetDataSourceProperties(props);

    try (final Connection conn = ds.getConnection()) {
      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  void test_TokenGenerators() {

    // This test could be a simple unit test since it requires AWS credentials.

    final String clusterEndpoint = "test-cluster.cluster-xyz.us-west-2.rds.amazonaws.com";
    final int clusterEndpointPort = 5432;
    final String region = "us-west-2";
    final String iamUsername = "jane_doe";

    final HostAvailabilityStrategy mockHostAvailabilityStrategy = Mockito.mock(HostAvailabilityStrategy.class);

    final Properties awsIamProp = new Properties();
    awsIamProp.setProperty(IamAuthConnectionPlugin.IAM_REGION.name, region);
    awsIamProp.setProperty(PropertyDefinition.USER.name, "<anything>");

    final HostSpec hostSpec = new HostSpecBuilder(mockHostAvailabilityStrategy)
        .host(clusterEndpoint)
        .build();

    final AwsCredentialsProvider credentialsProvider = AwsCredentialsManager.getProvider(hostSpec, awsIamProp);

    final Instant fixedInstant = Instant.parse("2024-10-20T00:00:00Z");

    final RdsUtilities rdsUtilities = TestDefaultRdsUtilities.getDefaultRdsUtilities(
        credentialsProvider,
        Region.of(region),
        fixedInstant);
    final RegularRdsUtility regularRdsUtility = new RegularRdsUtility(rdsUtilities);

    final String regularToken = regularRdsUtility.generateAuthenticationToken(
        credentialsProvider,
        Region.of(region),
        clusterEndpoint,
        clusterEndpointPort,
        iamUsername);

    final String lightToken = new LightRdsUtility(fixedInstant).generateAuthenticationToken(
        credentialsProvider,
        Region.of(region),
        clusterEndpoint,
        clusterEndpointPort,
        iamUsername);

    assertEquals(regularToken, lightToken);
  }

  protected Properties initAwsIamProps(String user, String password) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    props.setProperty(
        IamAuthConnectionPlugin.IAM_REGION.name,
        TestEnvironment.getCurrent().getInfo().getRegion());
    props.setProperty(PropertyDefinition.USER.name, user);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    props.setProperty(PropertyDefinition.TCP_KEEP_ALIVE.name, "false");
    return props;
  }

  protected String hostToIP(String hostname) throws UnknownHostException {
    final InetAddress inet = InetAddress.getByName(hostname);
    return inet.getHostAddress();
  }
}
