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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.mock.TestPluginOne;
import software.amazon.jdbc.mock.TestPluginThree;
import software.amazon.jdbc.mock.TestPluginThrowException;
import software.amazon.jdbc.mock.TestPluginTwo;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class ConnectionPluginManagerTests {

  @Mock JdbcCallable<Void, SQLException> mockSqlFunction;

  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testExecuteJdbcCallA() throws Exception {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Object result =
        target.execute(
            String.class,
            Exception.class,
            Connection.class,
            "testJdbcCall_A",
            () -> {
              calls.add("targetCall");
              return "resulTestValue";
            },
            testArgs);

    assertEquals("resulTestValue", result);

    assertEquals(7, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginTwo:before", calls.get(1));
    assertEquals("TestPluginThree:before", calls.get(2));
    assertEquals("targetCall", calls.get(3));
    assertEquals("TestPluginThree:after", calls.get(4));
    assertEquals("TestPluginTwo:after", calls.get(5));
    assertEquals("TestPluginOne:after", calls.get(6));
  }

  @Test
  public void testExecuteJdbcCallB() throws Exception {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Object result =
        target.execute(
            String.class,
            Exception.class,
            Connection.class,
            "testJdbcCall_B",
            () -> {
              calls.add("targetCall");
              return "resulTestValue";
            },
            testArgs);

    assertEquals("resulTestValue", result);

    assertEquals(5, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginTwo:before", calls.get(1));
    assertEquals("targetCall", calls.get(2));
    assertEquals("TestPluginTwo:after", calls.get(3));
    assertEquals("TestPluginOne:after", calls.get(4));
  }

  @Test
  public void testExecuteJdbcCallC() throws Exception {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Object result =
        target.execute(
            String.class,
            Exception.class,
            Connection.class,
            "testJdbcCall_C",
            () -> {
              calls.add("targetCall");
              return "resulTestValue";
            },
            testArgs);

    assertEquals("resulTestValue", result);

    assertEquals(3, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("targetCall", calls.get(1));
    assertEquals("TestPluginOne:after", calls.get(2));
  }

  @Test
  public void testConnect() throws Exception {

    final Connection expectedConnection = mock(Connection.class);

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls, expectedConnection));

    final Properties testProperties = new Properties();
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Connection conn = target.connect("any", new HostSpec("anyHost"), testProperties, true);

    assertEquals(expectedConnection, conn);
    assertEquals(4, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThree:before", calls.get(1));
    assertEquals("TestPluginThree:connection", calls.get(2));
    assertEquals("TestPluginOne:after", calls.get(3));
  }

  @Test
  public void testConnectWithSQLExceptionBefore() {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, SQLException.class, true));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    final Properties testProperties = new Properties();
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    assertThrows(
        SQLException.class,
        () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
  }

  @Test
  public void testConnectWithSQLExceptionAfter() {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, SQLException.class, false));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    final Properties testProperties = new Properties();
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    assertThrows(
        SQLException.class,
        () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(5, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
    assertEquals("TestPluginThree:before", calls.get(2));
    assertEquals("TestPluginThree:connection", calls.get(3));
    assertEquals("TestPluginThrowException:after", calls.get(4));
  }

  @Test
  public void testConnectWithUnexpectedExceptionBefore() {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, IllegalArgumentException.class, true));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    final Properties testProperties = new Properties();
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
  }

  @Test
  public void testConnectWithUnexpectedExceptionAfter() {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, IllegalArgumentException.class, false));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    final Properties testProperties = new Properties();
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper);

    final Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(5, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
    assertEquals("TestPluginThree:before", calls.get(2));
    assertEquals("TestPluginThree:connection", calls.get(3));
    assertEquals("TestPluginThrowException:after", calls.get(4));
  }

  @Test
  public void testExecuteCachedJdbcCallA() throws Exception {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target = Mockito.spy(
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper));

    Object result =
        target.execute(
            String.class,
            Exception.class,
            Connection.class,
            "testJdbcCall_A",
            () -> {
              calls.add("targetCall");
              return "resulTestValue";
            },
            testArgs);

    assertEquals("resulTestValue", result);

    // The method has been called just once to generate a final lambda and cache it.
    verify(target, times(1)).makePluginChainFunc(eq("testJdbcCall_A"));

    assertEquals(7, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginTwo:before", calls.get(1));
    assertEquals("TestPluginThree:before", calls.get(2));
    assertEquals("targetCall", calls.get(3));
    assertEquals("TestPluginThree:after", calls.get(4));
    assertEquals("TestPluginTwo:after", calls.get(5));
    assertEquals("TestPluginOne:after", calls.get(6));

    calls.clear();

    result =
        target.execute(
            String.class,
            Exception.class,
            Connection.class,
            "testJdbcCall_A",
            () -> {
              calls.add("targetCall");
              return "anotherResulTestValue";
            },
            testArgs);

    assertEquals("anotherResulTestValue", result);

    // No additional calls to this method occurred. It's still been called once.
    verify(target, times(1)).makePluginChainFunc(eq("testJdbcCall_A"));

    assertEquals(7, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginTwo:before", calls.get(1));
    assertEquals("TestPluginThree:before", calls.get(2));
    assertEquals("targetCall", calls.get(3));
    assertEquals("TestPluginThree:after", calls.get(4));
    assertEquals("TestPluginTwo:after", calls.get(5));
    assertEquals("TestPluginOne:after", calls.get(6));
  }

  @Test
  public void testExecuteAgainstOldConnection() throws Exception {
    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final PluginService mockPluginService = mock(PluginService.class);
    final ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper = mock(ConnectionWrapper.class);
    final Connection mockOldConnection = mock(Connection.class);
    final Connection mockCurrentConnection = mock(Connection.class);
    final Statement mockOldStatement = mock(Statement.class);
    final ResultSet mockOldResultSet = mock(ResultSet.class);

    when(mockPluginService.getCurrentConnection()).thenReturn(mockCurrentConnection);
    when(mockOldStatement.getConnection()).thenReturn(mockOldConnection);
    when(mockOldResultSet.getStatement()).thenReturn(mockOldStatement);

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins, mockConnectionWrapper,
            mockPluginService);

    assertThrows(SQLException.class,
        () -> target.execute(String.class, Exception.class, mockOldConnection, "testJdbcCall_A", () -> "result", null));
    assertThrows(SQLException.class,
        () -> target.execute(String.class, Exception.class, mockOldStatement, "testJdbcCall_A", () -> "result", null));
    assertThrows(SQLException.class,
        () -> target.execute(String.class, Exception.class, mockOldResultSet, "testJdbcCall_A", () -> "result", null));

    assertDoesNotThrow(
        () -> target.execute(Void.class, SQLException.class, mockOldConnection, "Connection.close", mockSqlFunction,
            null));
    assertDoesNotThrow(
        () -> target.execute(Void.class, SQLException.class, mockOldConnection, "Connection.abort", mockSqlFunction,
            null));
    assertDoesNotThrow(
        () -> target.execute(Void.class, SQLException.class, mockOldStatement, "Statement.close", mockSqlFunction,
            null));
    assertDoesNotThrow(
        () -> target.execute(Void.class, SQLException.class, mockOldResultSet, "ResultSet.close", mockSqlFunction,
            null));
  }
}
