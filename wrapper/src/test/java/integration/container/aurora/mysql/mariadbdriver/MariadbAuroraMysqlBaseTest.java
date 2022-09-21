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
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.jdbc.Driver;

public abstract class MariadbAuroraMysqlBaseTest extends AuroraMysqlBaseTest {

  @BeforeAll
  public static void setUpMariadb() throws SQLException, IOException {
    DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mariadb://";
    setUp();
    try {
      Class.forName("org.mariadb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      fail("MariaDB driver not found");
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  @BeforeEach
  public void setUpEachMysql() throws SQLException, InterruptedException {
    setUpEach();
  }
}
