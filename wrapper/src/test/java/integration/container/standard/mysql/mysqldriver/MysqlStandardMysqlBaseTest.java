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

package integration.container.standard.mysql.mysqldriver;

import integration.container.standard.mysql.StandardMysqlBaseTest;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.jdbc.Driver;

public class MysqlStandardMysqlBaseTest extends StandardMysqlBaseTest {

  @BeforeAll
  public static void setUpMysql() throws SQLException, IOException, ClassNotFoundException {
    DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mysql://";
    STANDARD_WRITER = System.getenv("STANDARD_MYSQL_WRITER");
    STANDARD_READER = System.getenv("STANDARD_MYSQL_READER");
    STANDARD_PORT = Integer.parseInt(System.getenv("STANDARD_MYSQL_PORT"));
    STANDARD_DB = System.getenv("STANDARD_MYSQL_DB");
    STANDARD_USERNAME = System.getenv("STANDARD_MYSQL_USERNAME");
    STANDARD_PASSWORD = System.getenv("STANDARD_MYSQL_PASSWORD");
    instanceIDs = new String[] {STANDARD_WRITER, STANDARD_READER};
    setUp();
    DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }
}
