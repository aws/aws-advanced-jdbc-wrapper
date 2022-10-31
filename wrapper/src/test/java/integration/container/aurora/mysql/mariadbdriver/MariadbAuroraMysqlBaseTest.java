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

package integration.container.aurora.mysql.mariadbdriver;

import static org.junit.jupiter.api.Assertions.fail;

import integration.container.aurora.mysql.AuroraMysqlBaseTest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class MariadbAuroraMysqlBaseTest extends AuroraMysqlBaseTest {

  @BeforeAll
  public static void setUpMariadb() throws SQLException, IOException {
    setUp();
    try {
      Class.forName("org.mariadb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      fail("MariaDB driver not found");
    }

    try {
      DriverManager.deregisterDriver(DriverManager.getDriver("jdbc:mysql://"));

    } catch (SQLException e) {
      System.out.println("MySQL driver is already deregistered");
    }
  }

  @Override
  protected Connection connectToInstance(String instanceUrl, int port, Properties props)
      throws SQLException {
    final String url = DB_CONN_STR_PREFIX + instanceUrl + ":" + port + "/" + AURORA_MYSQL_DB;
    return DriverManager.getConnection(url + "?permitMysqlScheme", props);
  }

  @Override
  protected Connection connectToInstanceCustomUrl(String url, Properties props)
      throws SQLException {
    return DriverManager.getConnection(url + "?permitMysqlScheme", props);
  }

  @Override
  protected List<String> getTopologyIds() throws SQLException {
    final String url =
        DB_CONN_STR_PREFIX + MYSQL_INSTANCE_1_URL + ":" + AURORA_MYSQL_PORT + "/" + AURORA_MYSQL_DB;
    return this.containerHelper.getAuroraInstanceIds(
        url + "?permitMysqlScheme", AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD, "mysql");
  }
}
