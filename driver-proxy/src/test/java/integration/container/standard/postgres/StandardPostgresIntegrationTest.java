/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

public class StandardPostgresIntegrationTest extends StandardPostgresBaseTest {

  protected static String buildConnectionString(
      String connStringPrefix,
      String host,
      String port,
      String databaseName) {
    return connStringPrefix + host + ":" + port + "/" + databaseName;
  }

  private static Stream<Arguments> testConnectionParameters() {
    return Stream.of(
        // missing connection prefix
        Arguments.of(buildConnectionString(
            "",
            STANDARD_POSTGRES_HOST,
            String.valueOf(STANDARD_POSTGRES_PORT),
            STANDARD_POSTGRES_DB)),
        // missing port
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_POSTGRES_HOST,
            "",
            STANDARD_POSTGRES_DB)),
        // incorrect database name
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_POSTGRES_HOST,
            String.valueOf(STANDARD_POSTGRES_PORT),
            "failedDatabaseNameTest")),
        // missing "/" at end of URL
        Arguments.of(DB_CONN_STR_PREFIX
            + STANDARD_POSTGRES_HOST
            + ":"
            + STANDARD_POSTGRES_PORT)
    );
  }

  private static Stream<Arguments> testPropertiesParameters() {
    return Stream.of(
        // missing username
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_POSTGRES_HOST,
            String.valueOf(STANDARD_POSTGRES_PORT),
            STANDARD_POSTGRES_DB),
            "",
            STANDARD_POSTGRES_PASSWORD),
        // missing password
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_POSTGRES_HOST,
            String.valueOf(STANDARD_POSTGRES_PORT),
             STANDARD_POSTGRES_DB),
            STANDARD_POSTGRES_USERNAME,
            "")
        );
  }

  @Test
  public void testBasicConnectionFunctionality() throws SQLException, IOException {
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

  @Test
  public void testSuccessOpenConnection() throws SQLException {
    try (Connection conn = connect()) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

      assertTrue(conn.isValid(10));
    }
  }

  @Test
  public void testSuccessOpenConnectionNoPort() throws SQLException {
    String url =
        DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + "/" + STANDARD_POSTGRES_DB;
    try (Connection conn = connectCustomUrl(url, initDefaultProps())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

      assertTrue(conn.isValid(10));
    }
  }

  @ParameterizedTest
  @MethodSource("testConnectionParameters")
  public void testFailedConnection(String url) {

    assertThrows(SQLException.class, () -> connectCustomUrl(url, initDefaultProps()));
  }


  @ParameterizedTest
  @MethodSource("testPropertiesParameters")
  public void testFailedProperties(String url, String username, String password) {

    final Properties props = new Properties();
    props.setProperty("username", username);
    props.setProperty("password", password);

    assertThrows(SQLException.class, () -> connectCustomUrl(url, props));
  }

  @Test
  public void testFailedHost() {

    String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        "",
        String.valueOf(STANDARD_POSTGRES_PORT),
        STANDARD_POSTGRES_DB);
    assertThrows(RuntimeException.class, () -> connectCustomUrl(url, initDefaultProps()));
  }
}
