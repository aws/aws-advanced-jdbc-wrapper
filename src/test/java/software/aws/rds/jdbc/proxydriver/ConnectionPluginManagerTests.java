package software.aws.rds.jdbc.proxydriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.mock.TestPluginOne;
import software.aws.rds.jdbc.proxydriver.mock.TestPluginThree;
import software.aws.rds.jdbc.proxydriver.mock.TestPluginThrowException;
import software.aws.rds.jdbc.proxydriver.mock.TestPluginTwo;

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
