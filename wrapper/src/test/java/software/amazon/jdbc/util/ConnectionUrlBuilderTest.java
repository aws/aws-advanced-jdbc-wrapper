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
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

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
    Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty(server, server);
    properties.setProperty(port, "1234");
    properties.setProperty(db, db);

    final String actual = ConnectionUrlBuilder.buildUrl(
        jdbcProtocol,
        host,
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
        properties));
  }

  static Stream<Arguments> urlArguments() {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("fooUrl")
        .build();
    final Properties properties = new Properties();
    properties.setProperty("bar", "baz");
    return Stream.of(
        Arguments.of(
            "protocol//",
            hostSpec,
            "",
            "port",
            "database",
            "protocol//fooUrl/database?port=1234&bar=baz",
            properties),
        Arguments.of(
            "protocol//",
            hostSpec,
            " ",
            "port",
            "database",
            "protocol//fooUrl/database?port=1234&bar=baz",
            properties),
        Arguments.of(
            "protocol",
            hostSpec,
            "server",
            "port",
            "database",
            "protocol//fooUrl/database?port=1234&bar=baz&server=server",
            properties)
    );
  }

  static Stream<Arguments> invalidUrlArguments() {
    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("fooUrl")
        .build();
    return Stream.of(
        Arguments.of("", hostSpec, "server", "1234", "dbName")
    );
  }
}
