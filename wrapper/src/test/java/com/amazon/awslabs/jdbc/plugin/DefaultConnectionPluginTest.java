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

package com.amazon.awslabs.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.awslabs.jdbc.ConnectionProvider;
import com.amazon.awslabs.jdbc.PluginManagerService;
import com.amazon.awslabs.jdbc.PluginService;
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
