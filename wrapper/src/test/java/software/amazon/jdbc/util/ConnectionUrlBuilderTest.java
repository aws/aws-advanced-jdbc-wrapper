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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.HostSpec;

class ConnectionUrlBuilderTest {

  @ParameterizedTest
  @MethodSource("urlArguments")
  void test_buildUrl(
      final String jdbcProtocol,
      final HostSpec host,
      final String server,
      final String port,
      final String db,
      final String expected,
      final Properties props) throws SQLException {
    Properties properties = new Properties(props);
    properties.setProperty(server, server);
    properties.setProperty(port, "1234");
    properties.setProperty(db, db);

    final String actual = ConnectionUrlBuilder.buildUrl(
        jdbcProtocol,
        host,
        server,
        port,
        db,
        properties);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("invalidUrlArguments")
  void test_buildUrl_throwsSQLException(
      final String jdbcProtocol,
      final HostSpec host,
      final String server,
      final String port,
      final String db) {
    final Properties properties = new Properties();
    properties.setProperty(server, server);
    properties.setProperty(port, "1234");
    properties.setProperty(db, db);

    assertThrows(SQLException.class, () -> ConnectionUrlBuilder.buildUrl(
        jdbcProtocol,
        host,
        server,
        port,
        db,
        properties));
  }

  static Stream<Arguments> urlArguments() {
    final HostSpec hostSpec = new HostSpec("fooUrl");
    final Properties properties = new Properties();
    properties.setProperty("bar", "baz");
    return Stream.of(
        Arguments.of(
            "protocol//",
            hostSpec,
            "",
            "port",
            "database",
            "protocol//fooUrl/database",
            properties),
        Arguments.of(
            "protocol//",
            hostSpec,
            " ",
            "port",
            "database",
            "protocol//fooUrl/database",
            properties),
        Arguments.of(
            "protocol",
            hostSpec,
            "server",
            "port",
            "database",
            "protocol//fooUrl/database",
            properties),
        Arguments.of(
            "protocol",
            null,
            "server",
            "port",
            "database",
            "protocol//server:1234/database",
            null),
        Arguments.of(
            "protocol",
            null,
            "server",
            "port",
            "db",
            "protocol//server:1234/",
            null)
    );
  }

  static Stream<Arguments> invalidUrlArguments() {
    final HostSpec hostSpec = new HostSpec("fooUrl");
    return Stream.of(
        Arguments.of("", hostSpec, "server", "1234", "dbName")
    );
  }
}
