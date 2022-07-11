/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package integration.container.standard.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

public class StandardPostgresIntegrationTest extends StandardPostgresBaseTest {

  @Test
  public void test_connect() throws SQLException, IOException {
    try (Connection conn = connect()) {
      Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      ResultSet rs = stmt.getResultSet();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }

    try (Connection conn = connectToProxy()) {
      assertTrue(conn.isValid(5));
      containerHelper.disableConnectivity(proxy);
      assertFalse(conn.isValid(5));
      containerHelper.enableConnectivity(proxy);
    }
  }
}
