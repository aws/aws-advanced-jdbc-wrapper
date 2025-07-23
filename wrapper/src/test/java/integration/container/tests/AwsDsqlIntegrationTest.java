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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.RUN_DSQL_TESTS_ONLY)
@Order(3)
public class AwsDsqlIntegrationTest {

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
