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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;

class ConnectionUrlParserTest {
  @ParameterizedTest
  @MethodSource("testGetHostsFromConnectionUrlArguments")
  void testGetHostsFromConnectionUrl_returnCorrectHostList(String testUrl, List<HostSpec> expected) {
    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final List<HostSpec> results = parser.getHostsFromConnectionUrl(testUrl, false);

    assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  @Test
  void testGetHostsFromConnectionUrl_singleWriterConnectionString() {
    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final String testUrl = "jdbc:driver:test://instance-1,instance-2:3303,instance-3/test";
    final List<HostSpec> expected =
        Arrays.asList(new HostSpec("instance-1"), new HostSpec("instance-2", 3303, HostRole.READER),
            new HostSpec("instance-3", HostSpec.NO_PORT, HostRole.READER));
    final List<HostSpec> results = parser.getHostsFromConnectionUrl(testUrl, true);

    assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  @ParameterizedTest
  @MethodSource("userUrls")
  void testParseUserFromUrl(final String url, final String expectedUser) {
    final String actualUser = ConnectionUrlParser.parseUserFromUrl(url);
    assertEquals(expectedUser, actualUser);
  }

  @ParameterizedTest
  @MethodSource("passwordUrls")
  void testParsePasswordFromUrl(final String url, final String expected) {
    final String actual = ConnectionUrlParser.parsePasswordFromUrl(url);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("encodedParams")
  void testEncodedParams(final String url, final String expected) {
    Properties props = new Properties();
    ConnectionUrlParser.parsePropertiesFromUrl(url, props);
    assertEquals(props.getProperty("param"), expected);
  }

  @ParameterizedTest
  @MethodSource("urlWithQuestionMarksParams")
  void testParsingUrlsWithQuestionMarks(final String url, final String expected) {
    Properties props = new Properties();
    ConnectionUrlParser.parsePropertiesFromUrl(url, props);
    assertEquals(expected, props.getProperty("param"));
  }

  private static Stream<Arguments> testGetHostsFromConnectionUrlArguments() {
    return Stream.of(
        Arguments.of("protocol//", new ArrayList<HostSpec>()),
        Arguments.of("bar/", new ArrayList<HostSpec>()),
        Arguments.of("invalid-hosts?", new ArrayList<HostSpec>()),
        Arguments.of("jdbc//host:3303/db?param=1", Collections.singletonList(new HostSpec("host", 3303))),
        Arguments.of("protocol//host2:3303", Collections.singletonList(new HostSpec("host2", 3303))),
        Arguments.of("foo//host:3303/?#", Collections.singletonList(new HostSpec("host", 3303))),
        Arguments.of("jdbc:mysql:replication://host:badInt?param=",
            Collections.singletonList(new HostSpec("host"))),
        Arguments.of("jdbc:driver:test://instance-1,instance-2:3303,instance-3/test",
            Arrays.asList(new HostSpec("instance-1"), new HostSpec("instance-2", 3303),
                new HostSpec("instance-3")))
    );
  }

  private static Stream<Arguments> userUrls() {
    return Stream.of(
        Arguments.of("protocol//url/db?user=foo", "foo"),
        Arguments.of("protocol//url/db?user=foo&pass=bar", "foo"),
        Arguments.of("protocol//url/db?USER=foo", null),
        Arguments.of("protocol//url/db?USER=foo&pass=bar", null),
        Arguments.of("protocol//url/db?username=foo", null)
    );
  }

  private static Stream<Arguments> passwordUrls() {
    return Stream.of(
        Arguments.of("protocol//url/db?password=foo", "foo"),
        Arguments.of("protocol//url/db?password=foo&user=bar", "foo"),
        Arguments.of("protocol//url/db?PASSWORD=foo", null),
        Arguments.of("protocol//url/db?PASSWORD=foo&user=bar", null),
        Arguments.of("protocol//url/db?pass=foo", null)
    );
  }

  private static Stream<Arguments> encodedParams() {
    return Stream.of(
        Arguments.of("protocol//host/db?param=" + StringUtils.encode("value$"), "value$"),
        Arguments.of("protocol//host/db?param=" + StringUtils.encode("value_"), "value_"),
        Arguments.of("protocol//host/db?param=" + StringUtils.encode("value?"), "value?"),
        Arguments.of("protocol//host/db?param=" + StringUtils.encode("value&"), "value&"),
        Arguments.of("protocol//host/db?param=" + StringUtils.encode("value"
            + new String(Character.toChars(0x007E))), "value~")
    );
  }

  private static Stream<Arguments> urlWithQuestionMarksParams() {
    return Stream.of(
        Arguments.of("protocol//host/db?param=foo", "foo"),
        Arguments.of("protocol//host:1234/db?param=?.foo", "?.foo"),
        Arguments.of("protocol//host?param=?", "?")
    );
  }
}
