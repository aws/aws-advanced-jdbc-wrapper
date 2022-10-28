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
import software.amazon.jdbc.wrapper.ConnectionWrapper;

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
            STANDARD_WRITER,
            String.valueOf(STANDARD_PORT),
            STANDARD_DB)),
        // missing port
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_WRITER,
            "",
            STANDARD_DB)),
        // incorrect database name
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_WRITER,
            String.valueOf(STANDARD_PORT),
            "failedDatabaseNameTest")),
        // missing "/" at end of URL
        Arguments.of(DB_CONN_STR_PREFIX
            + STANDARD_WRITER
            + ":"
            + STANDARD_PORT)
    );
  }

  private static Stream<Arguments> testPropertiesParameters() {
    return Stream.of(
        // missing username
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_WRITER,
            String.valueOf(STANDARD_PORT),
            STANDARD_DB),
            "",
            STANDARD_PASSWORD),
        // missing password
        Arguments.of(buildConnectionString(
            DB_CONN_STR_PREFIX,
            STANDARD_WRITER,
            String.valueOf(STANDARD_PORT),
             STANDARD_DB),
            STANDARD_USERNAME,
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
      containerHelper.disableConnectivity(proxyWriter);
      assertFalse(conn.isValid(5));
      containerHelper.enableConnectivity(proxyWriter);
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
        DB_CONN_STR_PREFIX + STANDARD_WRITER + "/" + STANDARD_DB;
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
    props.setProperty("user", username);
    props.setProperty("password", password);

    assertThrows(SQLException.class, () -> connectCustomUrl(url, props));
  }

  @Test
  public void testFailedHost() {

    String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        "",
        String.valueOf(STANDARD_PORT),
        STANDARD_DB);
    assertThrows(SQLException.class, () -> connectCustomUrl(url, initDefaultProps()));
  }
}
