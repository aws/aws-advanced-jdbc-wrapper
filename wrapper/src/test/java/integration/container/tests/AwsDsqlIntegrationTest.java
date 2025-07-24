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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.arn.Arn;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnTestFeature;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.RUN_DSQL_TESTS_ONLY)
@Order(3)
public class AwsDsqlIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(AwsDsqlIntegrationTest.class.getName());

  @BeforeEach
  public void beforeEach() {
    IamAuthConnectionPlugin.clearCache();
  }

  /**
   * Attempt to connect using the wrong database username.
   */
  @TestTemplate
  public void test_AwsIamDsql_WrongDatabaseUsername() {
    final Properties props =
        initAwsIamProps(
            "WRONG_"
                + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername()
                + "_USER",
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));
  }

  /**
   * Attempt to connect without specifying a database username.
   */
  @TestTemplate
  public void test_AwsIamDsql_NoDatabaseUsername() {
    final Properties props =
        initAwsIamProps("", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));
  }

  /**
   * Attempt to connect using valid database username/password & valid Amazon Aurora DSQL endpoint.
   */
  @TestTemplate
  public void test_AwsIamDsql_ValidConnectionProperties() throws SQLException {
    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");

    final Connection conn =
        DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    assertNotNull(conn);
    conn.close();
  }

  /**
   * Attempt to connect using valid database username, valid Amazon Aurora DSQL endpoint, but no password.
   */
  @TestTemplate
  public void test_AwsIamDsql_ValidConnectionPropertiesNoPassword() throws SQLException {
    final Properties props =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "");
    final Connection conn =
        DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    assertNotNull(conn);
    conn.close();
  }

  /**
   * Attempts a valid connection followed by invalid connection without the AWS protocol in connection URL.
   */
  @TestTemplate
  void test_AwsIamDsql_NoAwsProtocolConnection() throws SQLException {
    final String dbConn = ConnectionStringHelper.getWrapperUrl();
    final Properties validProp =
        initAwsIamProps(TestEnvironment.getCurrent().getInfo().getIamUsername(), "<anything>");

    final Connection conn = DriverManager.getConnection(dbConn, validProp);
    assertNotNull(conn);
    conn.close();

    final Properties invalidProp =
        initAwsIamProps(
            "WRONG_" + TestEnvironment.getCurrent().getInfo().getIamUsername() + "_USER",
            "<anything>");

    assertThrows(
        SQLException.class, () -> DriverManager.getConnection(dbConn, invalidProp));
  }

  /**
   * Attempts a valid connection followed by an invalid connection with username in connection URL.
   */
  @TestTemplate
  void test_AwsIamDsql_UserInConnStr() throws SQLException {
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
    assertNotNull(validConn);
    validConn.close();

    assertThrows(
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
   * Attempts a valid connection with a datasource and makes sure the user and password properties persist.
   */
  @TestTemplate
  void test_AwsIamDsql_UserAndPasswordPropertiesArePreserved() throws SQLException {
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

  /**
   * Tests that a non-admin user can connect when the appropriate IAM role is granted. This test verifies the complete
   * workflow of creating a database role, granting IAM permissions, and connecting as a non-admin user.
   */
  @TestTemplate
  public void test_AwsIamDsql_NonAdminUserCanConnectWhenRoleGranted() throws Exception {
    final String adminUser = "admin";
    final String nonAdminUser = "test_non_admin";

    final Properties nonAdminProps = initAwsIamProps(nonAdminUser, "");
    final Properties adminProps = initAwsIamProps(adminUser, "");

    assertUserCanConnect(adminUser, adminProps);

    final String roleArn = getRoleArnFromCredentials(adminProps);

    Exception storedException = null;
    try {
      createRole(adminProps, nonAdminUser, roleArn);
      assertUserCanConnect(nonAdminUser, nonAdminProps);
    } catch (Exception e) {
      storedException = e;
      throw e;
    } finally {
      dropRole(adminProps, nonAdminUser, roleArn, storedException);
    }
  }

  /**
   * Extract the IAM role ARN from the current AWS credentials.
   *
   * @param adminProps The {@link Properties} to use for admin connections.
   */
  private static String getRoleArnFromCredentials(final Properties adminProps) {
    final String region = adminProps.getProperty(IamAuthConnectionPlugin.IAM_REGION.name);
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("anyHost")
        .build();
    final AwsCredentialsProvider credentialsProvider = AwsCredentialsManager.getProvider(hostSpec, adminProps);

    try (final StsClient client = StsClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.of(region))
        .build()) {
      final GetCallerIdentityResponse response = client.getCallerIdentity();
      final Arn assumedArn = Arn.fromString(response.arn());

      if ("sts".equals(assumedArn.getService()) && assumedArn.getResource().getResourceType().equals("assumed-role")) {
        final String resource = assumedArn.getResource().getResource();
        final String roleName = resource.substring(0, resource.lastIndexOf('/'));

        return assumedArn.toBuilder()
            .withService("iam")
            .withResource("role/" + roleName)
            .build()
            .toString();
      } else {
        return assumedArn.toString();
      }
    }
  }

  /**
   * Verify the provided user can connect to the database.
   *
   * @param user The user to verify.
   * @param props The {@link Properties} to use for the established connection.
   */
  private static void assertUserCanConnect(final String user, final Properties props) throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
      assertTrue(conn.isValid(10));

      try (final Statement stmt = conn.createStatement();
           final ResultSet rs = stmt.executeQuery("SELECT CURRENT_USER")) {
        assertTrue(rs.next());
        assertEquals(user, rs.getString(1));
      }
    }
  }

  /**
   * Create a database role for the non-admin user and grants the necessary IAM permissions.
   *
   * @param adminProps The {@link Properties} to use for admin connections.
   * @param nonAdminUser The non-admin role to create.
   * @param roleArn An ARN for the IAM role to bind to.
   */
  private static void createRole(final Properties adminProps, final String nonAdminUser, final String roleArn)
      throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), adminProps)) {
      try (final Statement statement = conn.createStatement()) {
        statement.execute(String.format("CREATE ROLE %s WITH LOGIN", nonAdminUser));
        statement.execute(String.format("AWS IAM GRANT %s TO '%s'", nonAdminUser, roleArn));
      }
    }
  }

  /**
   * Clean up the created role and IAM permissions.
   *
   * @param adminProps The {@link Properties} to use for admin connections.
   * @param nonAdminUser The non-admin role to drop.
   * @param roleArn The ARN for the IAM role it is bound to.
   * @param storedException The current exception or {@code null} if no error has yet occurred.
   */
  private static void dropRole(
      final Properties adminProps,
      final String nonAdminUser,
      final String roleArn,
      Exception storedException) throws Exception {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), adminProps)) {
      try (final Statement statement = conn.createStatement()) {
        try {
          statement.execute(String.format("AWS IAM REVOKE %s FROM '%s'", nonAdminUser, roleArn));
        } catch (SQLException e) {
          if (storedException != null) {
            storedException.addSuppressed(e);
          } else {
            storedException = e;
          }
          LOGGER.warning(String.format("Failed to revoke %s from %s: %s", nonAdminUser, roleArn, e));
        }

        try {
          statement.execute(String.format("DROP ROLE %s", nonAdminUser));
        } catch (SQLException e) {
          if (storedException != null) {
            storedException.addSuppressed(e);
          } else {
            storedException = e;
          }
          LOGGER.warning(String.format("Failed to drop role %s: %s", nonAdminUser, e));
        }
      }
    }

    if (storedException != null) {
      LOGGER.severe(storedException.getMessage());
      throw storedException;
    }
  }

  private static Properties initAwsIamProps(final String user, final String password) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "iamDsql");
    props.setProperty(
        IamAuthConnectionPlugin.IAM_REGION.name,
        TestEnvironment.getCurrent().getInfo().getRegion());
    props.setProperty(PropertyDefinition.USER.name, user);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    props.setProperty(PropertyDefinition.TCP_KEEP_ALIVE.name, "false");

    return props;
  }
}
