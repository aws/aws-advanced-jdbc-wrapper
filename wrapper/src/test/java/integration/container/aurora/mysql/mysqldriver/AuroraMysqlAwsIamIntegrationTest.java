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

package integration.container.aurora.mysql.mysqldriver;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;

public class AuroraMysqlAwsIamIntegrationTest extends MysqlAuroraMysqlBaseTest {
  /**
   * Attempt to connect using the wrong database username.
   */
  @Test
  public void test_AwsIam_WrongDatabaseUsername() {
    final Properties props = initAwsIamProps("WRONG_" + AURORA_MYSQL_DB_USER + "_USER", AURORA_MYSQL_PASSWORD);

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props)
    );
  }

  /**
   * Attempt to connect without specifying a database username.
   */
  @Test
  public void test_AwsIam_NoDatabaseUsername() {
    final Properties props = initAwsIamProps("", AURORA_MYSQL_PASSWORD);

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props)
    );
  }

  /**
   * Attempt to connect using IP address instead of a hostname.
   */
  @Test
  public void test_AwsIam_UsingIPAddress() throws UnknownHostException, SQLException {
    final Properties props = initAwsIamProps(AURORA_MYSQL_DB_USER, AURORA_MYSQL_PASSWORD);

    final String hostIp = hostToIP(MYSQL_CLUSTER_URL);
    props.setProperty("iamHost", MYSQL_CLUSTER_URL);

    final Connection conn =  connectToInstance(hostIp, AURORA_MYSQL_PORT, props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /**
   * Attempt to connect using valid database username/password & valid Amazon RDS hostname.
   */
  @Test
  public void test_AwsIam_ValidConnectionProperties() throws SQLException {
    final Properties props = initAwsIamProps(AURORA_MYSQL_DB_USER, AURORA_MYSQL_PASSWORD);

    final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /**
   * Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
   */
  @Test
  public void test_AwsIam_ValidConnectionPropertiesNoPassword() throws SQLException {
    final Properties props = initAwsIamProps(AURORA_MYSQL_DB_USER, "");
    final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /**
   * Attempts a valid connection followed by invalid connection
   * without the AWS protocol in Connection URL.
   */
  @Test
  void test_AwsIam_NoAwsProtocolConnection() throws SQLException {
    final String dbConn = DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL;
    final Properties validProp = initAwsIamProps(AURORA_MYSQL_DB_USER, AURORA_MYSQL_PASSWORD);
    final Properties invalidProp =
        initAwsIamProps("WRONG_" + AURORA_MYSQL_DB_USER + "_USER", AURORA_MYSQL_PASSWORD);

    final Connection conn = DriverManager.getConnection(dbConn, validProp);
    Assertions.assertDoesNotThrow(conn::close);
    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(dbConn, invalidProp)
    );
  }

  /**
   * Attempts a valid connection followed by an invalid connection
   * with Username in Connection URL.
   */
  @Test
  void test_AwsIam_UserInConnStr() throws SQLException {
    final String dbConn = DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL;
    final Properties awsIamProp = initDefaultProps();
    awsIamProp.remove(PropertyDefinition.USER.name);
    awsIamProp.setProperty(PropertyDefinition.PLUGINS.name, "iam");

    final Connection validConn =
        DriverManager.getConnection(
            dbConn + "?" + PropertyDefinition.USER.name + "=" + AURORA_MYSQL_DB_USER,
            awsIamProp);
    Assertions.assertNotNull(validConn);
    Assertions.assertThrows(
        SQLException.class,
        () ->
            DriverManager.getConnection(
                dbConn + "?" + PropertyDefinition.USER.name + "=WRONG_" + AURORA_MYSQL_DB_USER,
                awsIamProp)
    );
  }
}
