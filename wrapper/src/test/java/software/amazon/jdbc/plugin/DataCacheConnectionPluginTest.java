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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;

class DataCacheConnectionPluginTest {

  private static final Properties props = new Properties();

  private AutoCloseable closeable;

  @Mock
  ResultSet mockResult1;
  @Mock
  ResultSet mockResult2;
  @Mock
  Statement mockStatement;
  @Mock
  ResultSetMetaData mockMetaData;

  @Mock
  JdbcCallable mockCallable;


  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props.setProperty(DataCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, "foo");
    DataCacheConnectionPlugin.clearCache();

    when(mockResult1.getMetaData()).thenReturn(mockMetaData);
    when(mockResult2.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumnCount()).thenReturn(1);
    when(mockMetaData.getColumnName(1)).thenReturn("fooName");

    // Mock result sets contain 1 row.
    when(mockResult1.next()).thenReturn(true, false);
    when(mockResult2.next()).thenReturn(true, false);
    when(mockResult1.getObject(1)).thenReturn("bar1");
    when(mockResult2.getObject(1)).thenReturn("bar2");
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_execute_withEmptyCache() throws SQLException {
    final String methodName = "Statement.executeQuery";

    final DataCacheConnectionPlugin plugin = new DataCacheConnectionPlugin(props);

    final ResultSet rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        () -> mockResult1,
        new String[]{"foo"}
    );

    compareResults(mockResult1, rs);
  }

  @Test
  void test_execute_withCache() throws Exception {
    final String methodName = "Statement.executeQuery";

    final DataCacheConnectionPlugin plugin = new DataCacheConnectionPlugin(props);

    when(mockCallable.call()).thenReturn(mockResult1, mockResult2);

    ResultSet rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        mockCallable,
        new String[]{"foo"}
    );
    compareResults(mockResult1, rs);

    // Execute the query again with a different result set.
    rs = plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement, methodName,
        mockCallable,
        new String[]{"foo"}
    );

    compareResults(mockResult1, rs);
    verify(mockCallable).call();
  }

  void compareResults(final ResultSet expected, final ResultSet actual) throws SQLException {
    int i = 1;
    while (expected.next() && actual.next()) {
      assertEquals(expected.getObject(i), actual.getObject(i));
      i++;
    }
  }
}
