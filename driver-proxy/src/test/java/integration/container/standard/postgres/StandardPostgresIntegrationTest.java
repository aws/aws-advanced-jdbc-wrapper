/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
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
