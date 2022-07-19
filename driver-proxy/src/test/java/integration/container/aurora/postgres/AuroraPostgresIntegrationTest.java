/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.aurora.postgres;

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

public class AuroraPostgresIntegrationTest extends AuroraPostgresBaseTest {

  protected static String buildConnectionString(
      String connStringPrefix,
      String host,
      String port,
      String databaseName) {
    return connStringPrefix + host + ":" + port + "/" + databaseName;
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        // missing username
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            POSTGRES_INSTANCE_1_URL,
            String.valueOf(AURORA_POSTGRES_PORT),
            AURORA_POSTGRES_DB),
            "",
            AURORA_POSTGRES_PASSWORD),
        // missing password
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            POSTGRES_INSTANCE_1_URL,
            String.valueOf(AURORA_POSTGRES_PORT),
            AURORA_POSTGRES_DB),
            AURORA_POSTGRES_USERNAME,
            ""),
        // missing connection prefix
        Arguments.of(buildConnectionString(
            "",
            POSTGRES_INSTANCE_1_URL,
            String.valueOf(AURORA_POSTGRES_PORT),
            AURORA_POSTGRES_DB),
            AURORA_POSTGRES_USERNAME,
            AURORA_POSTGRES_PASSWORD),
        // missing port
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            POSTGRES_INSTANCE_1_URL,
          "",
            AURORA_POSTGRES_DB),
            AURORA_POSTGRES_USERNAME,
            AURORA_POSTGRES_PASSWORD),
        // incorrect database name
        Arguments.of(buildConnectionString(DB_CONN_STR_PREFIX,
            POSTGRES_INSTANCE_1_URL,
            String.valueOf(AURORA_POSTGRES_PORT),
            "failedDatabaseNameTest"),
            AURORA_POSTGRES_USERNAME,
            AURORA_POSTGRES_PASSWORD)
    );
  }

  @Test
  public void testBasicConnectionFunctionality() throws SQLException, IOException {
    try (final Connection conn = connectToInstance(
        POSTGRES_INSTANCE_1_URL
        + PROXIED_DOMAIN_NAME_SUFFIX,
        POSTGRES_PROXY_PORT)) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");
      rs.next();
      assertEquals(1, rs.getInt(1));

      containerHelper.disableConnectivity(proxyInstance_1);
      assertFalse(conn.isValid(5));
      containerHelper.enableConnectivity(proxyInstance_1);
    }
  }

  @Test
  public void testSuccessOpenConnection() throws SQLException {

    final String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        POSTGRES_INSTANCE_1_URL,
        String.valueOf(AURORA_POSTGRES_PORT),
        AURORA_POSTGRES_DB);

    Properties props = new Properties();
    props.setProperty("user", AURORA_POSTGRES_USERNAME);
    props.setProperty("password", AURORA_POSTGRES_PASSWORD);

    Connection conn = connectToInstanceCustomUrl(url, props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testSuccessOpenConnectionNoPort() throws SQLException {

    final String url = DB_CONN_STR_PREFIX + POSTGRES_INSTANCE_1_URL  + "/" + AURORA_POSTGRES_DB;

    Properties props = new Properties();
    props.setProperty("user", AURORA_POSTGRES_USERNAME);
    props.setProperty("password", AURORA_POSTGRES_PASSWORD);

    Connection conn = connectToInstanceCustomUrl(url, props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @ParameterizedTest
  @MethodSource("testParameters")
  public void testFailedConnection(String url, String user, String password) {

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);

    assertThrows(SQLException.class, () -> connectToInstanceCustomUrl(url, props));
  }

  @Test
  public void testFailedHost() {

    Properties props = new Properties();
    props.setProperty("user", AURORA_POSTGRES_USERNAME);
    props.setProperty("password", AURORA_POSTGRES_PASSWORD);
    String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        "",
        String.valueOf(AURORA_POSTGRES_PORT),
        AURORA_POSTGRES_DB);
    assertThrows(RuntimeException.class, () -> connectToInstanceCustomUrl(
        url, props));
  }
}
