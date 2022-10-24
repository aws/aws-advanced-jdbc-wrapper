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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.beans.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class LogQueryConnectionPluginTest {

  private AutoCloseable closeable;
  private static final Properties props = new Properties();

  @Mock Statement mockStatement;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    props.setProperty(LogQueryConnectionPlugin.ENHANCED_LOG_QUERY_ENABLED.name, "true");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("jdbcMethodArgs")
  void test_getQuery_queryInArguments(final List<String> jdbcArgs, final String expected) {
    final LogQueryConnectionPlugin plugin = new LogQueryConnectionPlugin(props);

    final String methodName = "Statement.executeQuery";

    final String query = plugin.getQuery(mockStatement, methodName, jdbcArgs.toArray());

    assertEquals(expected, query);
  }

  static Stream<Arguments> jdbcMethodArgs() {
    return Stream.of(
        Arguments.of(Collections.emptyList(), null),
        Arguments.of(Collections.singletonList(null), null),
        Arguments.of(Collections.singletonList("foo"), "foo"),
        Arguments.of(Arrays.asList("foo", "bar"), "foo")
    );
  }
}
