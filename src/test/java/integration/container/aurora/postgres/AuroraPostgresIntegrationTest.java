/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.aurora.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

public class AuroraPostgresIntegrationTest extends AuroraPostgresBaseTest {

  @Test
  public void test_connect() throws SQLException, IOException {
    try (final Connection conn = connectToInstance(POSTGRES_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, POSTGRES_PROXY_PORT)) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");
      rs.next();
      assertEquals(1, rs.getInt(1));

      containerHelper.disableConnectivity(proxyInstance_1);
      assertFalse(conn.isValid(5));
      containerHelper.enableConnectivity(proxyInstance_1);
    }
  }
}