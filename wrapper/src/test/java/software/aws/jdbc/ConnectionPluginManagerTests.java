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

package software.aws.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import software.aws.jdbc.mock.TestPluginOne;
import software.aws.jdbc.mock.TestPluginThree;
import software.aws.jdbc.mock.TestPluginThrowException;
import software.aws.jdbc.mock.TestPluginTwo;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class ConnectionPluginManagerTests {

  @Test
  public void testExecuteJdbcCallA() throws Exception {

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    Properties testProperties = new Properties();

    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    Object[] testArgs = new Object[] {10, "arg2", 3.33};

    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

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

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    Properties testProperties = new Properties();

    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    Object[] testArgs = new Object[] {10, "arg2", 3.33};

    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    Object result =
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

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    Properties testProperties = new Properties();

    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);

    Object[] testArgs = new Object[] {10, "arg2", 3.33};

    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    Object result =
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

    Connection expectedConnection = mock(Connection.class);

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls, expectedConnection));

    Properties testProperties = new Properties();
    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    Connection conn = target.connect("any", new HostSpec("anyHost"), testProperties, true);

    assertEquals(expectedConnection, conn);
    assertEquals(4, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThree:before", calls.get(1));
    assertEquals("TestPluginThree:connection", calls.get(2));
    assertEquals("TestPluginOne:after", calls.get(3));
  }

  @Test
  public void testConnectWithSQLExceptionBefore() {

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, SQLException.class, true));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    Properties testProperties = new Properties();
    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    assertThrows(
        SQLException.class,
        () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
  }

  @Test
  public void testConnectWithSQLExceptionAfter() {

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, SQLException.class, false));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    Properties testProperties = new Properties();
    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

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

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, IllegalArgumentException.class, true));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    Properties testProperties = new Properties();
    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> target.connect("any", new HostSpec("anyHost"), testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
  }

  @Test
  public void testConnectWithUnexpectedExceptionAfter() {

    ArrayList<String> calls = new ArrayList<>();

    ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThrowException(calls, IllegalArgumentException.class, false));
    testPlugins.add(new TestPluginThree(calls, mock(Connection.class)));

    Properties testProperties = new Properties();
    ConnectionProvider mockConnectionProvider = mock(ConnectionProvider.class);
    ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider, testProperties, testPlugins);

    Exception ex =
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
}
