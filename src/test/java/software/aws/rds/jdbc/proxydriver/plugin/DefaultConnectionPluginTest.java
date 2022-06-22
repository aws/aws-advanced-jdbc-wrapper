/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.aws.rds.jdbc.proxydriver.ConnectionProvider;
import software.aws.rds.jdbc.proxydriver.PluginManagerService;
import software.aws.rds.jdbc.proxydriver.PluginService;

class DefaultConnectionPluginTest {

  private DefaultConnectionPlugin plugin;

  @Mock PluginService pluginService;
  @Mock ConnectionProvider connectionProvider;
  @Mock PluginManagerService pluginManagerService;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    plugin = new DefaultConnectionPlugin(pluginService, connectionProvider, pluginManagerService);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("openTransactionQueries")
  void testOpenTransaction(final String sql, boolean expected) {
    final boolean actual = plugin.doesOpenTransaction(sql);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("closeTransactionQueries")
  void testCloseTransaction(final String sql, boolean expected) {
    final boolean actual = plugin.doesCloseTransaction(sql);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("multiStatementQueries")
  void testParseMultiStatementQueries(final String sql, List<String> expected) {
    final List<String> actual = plugin.parseMultiStatementQueries(sql);
    assertEquals(expected, actual);
  }

  private static Stream<Arguments> openTransactionQueries() {
    return Stream.of(
        Arguments.of("begin;", true),
        Arguments.of("START TRANSACTION;", true),
        Arguments.of("START /* COMMENT */ TRANSACTION;", true),
        Arguments.of("START/* COMMENT */TRANSACTION;", true),
        Arguments.of("START      /* COMMENT */    TRANSACTION;", true),
        Arguments.of("START   /*COMMENT*/TRANSACTION;", true),
        Arguments.of("commit", false)
    );
  }

  private static Stream<Arguments> closeTransactionQueries() {
    return Stream.of(
        Arguments.of("rollback;", true),
        Arguments.of("commit;", true),
        Arguments.of("end", true),
        Arguments.of("abort;", true)
    );
  }

  private static Stream<Arguments> multiStatementQueries() {
    return Stream.of(
        Arguments.of("", new ArrayList<String>()),
        Arguments.of(null, new ArrayList<String>()),
        Arguments.of("  ", new ArrayList<String>()),
        Arguments.of("some  \t  \r  \n   query;", Collections.singletonList("some query")),
        Arguments.of("some\t\t\r\n query;query2", Arrays.asList("some query", "query2"))
    );
  }
}
