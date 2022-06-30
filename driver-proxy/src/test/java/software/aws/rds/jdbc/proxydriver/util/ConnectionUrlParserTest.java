/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.aws.rds.jdbc.proxydriver.HostSpec;

class ConnectionUrlParserTest {
  @ParameterizedTest
  @MethodSource("testGetHostsFromConnectionUrlArguments")
  void testGetHostsFromConnectionUrl_returnCorrectHostList(String testUrl, List<HostSpec> expected) {
    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final List<HostSpec> results = parser.getHostsFromConnectionUrl(testUrl);

    assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

  private static Stream<Arguments> testGetHostsFromConnectionUrlArguments() {
    return Stream.of(
        Arguments.of("protocol//", new ArrayList<HostSpec>()),
        Arguments.of("bar/", new ArrayList<HostSpec>()),
        Arguments.of("invalid-hosts?", new ArrayList<HostSpec>()),
        Arguments.of("jdbc//host:3303/db?param=1", Collections.singletonList(new HostSpec("integration/host", 3303))),
        Arguments.of("protocol//host2:3303", Collections.singletonList(new HostSpec("host2", 3303))),
        Arguments.of("foo//host:3303/?#", Collections.singletonList(new HostSpec("integration/host", 3303))),
        Arguments.of("jdbc:mysql:replication://host:badInt?param=", Collections.singletonList(new HostSpec("integration/host"))),
        Arguments.of(
            "jdbc:driver:test://source,replica1:3303,host/test",
            Arrays.asList(new HostSpec("source"), new HostSpec("replica1", 3303), new HostSpec("integration/host")))
    );
  }
}
