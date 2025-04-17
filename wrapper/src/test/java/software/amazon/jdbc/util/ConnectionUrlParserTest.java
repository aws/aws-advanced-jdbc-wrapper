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
import static org.junit.jupiter.api.Assertions.assertNull;

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
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class ConnectionUrlParserTest {

  @ParameterizedTest
  @MethodSource("testGetHostsFromConnectionUrlArguments")
  void testGetHostsFromConnectionUrl_returnCorrectHostList(String testUrl, List<HostSpec> expected) {
    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final List<HostSpec> results = parser.getHostsFromConnectionUrl(testUrl, false,
        () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

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
        Arrays.asList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2")
                .port(3303)
                .role(HostRole.READER)
                .build(),
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3")
                .port(HostSpec.NO_PORT)
                .role(HostRole.READER)
                .build());
    final List<HostSpec> results = parser.getHostsFromConnectionUrl(testUrl, true,
        () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

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

  @ParameterizedTest
  @MethodSource("urlWithNestedParams")
  void testParsingUrlWithNestedParams(final String url, final String expected) {
    Properties props = new Properties();
    ConnectionUrlParser.parsePropertiesFromUrl(url, props);
    assertEquals(expected, props.getProperty("param"));
  }

  @Test
  void testParsingCaseSensitiveParam() {
    Properties props = new Properties();

    // proper (camel case) parameter name
    ConnectionUrlParser.parsePropertiesFromUrl("protocol//url/db?wrapperPlugins=somePlugin", props);
    assertEquals("somePlugin", props.getProperty("wrapperPlugins"));
  }

  @Test
  void testParsingParamWrongCase() {
    Properties props = new Properties();

    // lower case parameter
    ConnectionUrlParser.parsePropertiesFromUrl("protocol//url/db?wrapperplugins=somePlugin", props);
    assertNull(props.getProperty("wrapperPlugins"));
  }

  @Test
  void testParsingCaseInsensitiveParam() {
    Properties props = new Properties();

    // proper (camel case) parameter name
    ConnectionUrlParser.parsePropertiesFromUrl(
        "protocol//url/db?wrapperPlugins=somePlugin&wrapperCaseSensitive=false", props);
    assertEquals("somePlugin", props.getProperty("wrapperPlugins"));
  }

  @Test
  void testParsingCaseInsensitiveParam2() {
    Properties props = new Properties();

    // lower case parameter
    ConnectionUrlParser.parsePropertiesFromUrl(
        "protocol//url/db?wrapperplugins=somePlugin&wrapperCaseSensitive=false", props);
    assertEquals("somePlugin", props.getProperty("wrapperPlugins"));
  }

  @Test
  void testParsingCaseInsensitiveParam3() {
    Properties props = new Properties();

    // lower case parameter
    ConnectionUrlParser.parsePropertiesFromUrl(
        "protocol//url/db?wrapperplugins=somePlugin&wrappercasesensitive=false", props);
    assertEquals("somePlugin", props.getProperty("wrapperPlugins"));
  }

  private static Stream<Arguments> testGetHostsFromConnectionUrlArguments() {
    return Stream.of(
        Arguments.of("protocol//", new ArrayList<HostSpec>()),
        Arguments.of("bar/", new ArrayList<HostSpec>()),
        Arguments.of("invalid-hosts?", new ArrayList<HostSpec>()),
        Arguments.of("jdbc//host:3303/db?param=1", Collections.singletonList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").port(3303).build())),
        Arguments.of("protocol//host2:3303", Collections.singletonList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host2").port(3303).build())),
        Arguments.of("foo//host:3303/?#", Collections.singletonList(
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").port(3303).build())),
        Arguments.of("jdbc:mysql:replication://host:badInt?param=",
            Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host")
                .build())),
        Arguments.of("jdbc:driver:test://instance-1,instance-2:3303,instance-3/test",
            Arrays.asList(
                new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").build(),
                new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").port(3303)
                    .build(),
                new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").build()))
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

  private static Stream<Arguments> urlWithNestedParams() {
    return Stream.of(
        Arguments.of("protocol//host/db?param=-c%20foo=1000", "-c foo=1000"),
        Arguments.of("protocol//host/db?param=-c foo=1000", "-c foo=1000"),
        Arguments.of("protocol//host/db?param=-c%20foo=a,b,c%20-c%20bar=10", "-c foo=a,b,c -c bar=10"),
        Arguments.of("protocol//host/db?param=-c foo=a,b,c -c bar=10", "-c foo=a,b,c -c bar=10")
    );
  }
}
