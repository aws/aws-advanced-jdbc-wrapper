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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.mock.TestPluginOne;
import software.amazon.jdbc.mock.TestPluginThree;
import software.amazon.jdbc.mock.TestPluginThrowException;
import software.amazon.jdbc.mock.TestPluginTwo;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPlugin;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.LogQueryConnectionPlugin;
import software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class ConnectionPluginManagerTests {

  private static final Logger LOGGER = Logger.getLogger(ConnectionPluginManagerTests.class.getName());

  @Mock JdbcCallable<Void, SQLException> mockSqlFunction;
  @Mock ConnectionProvider mockConnectionProvider;
  @Mock ConnectionWrapper mockConnectionWrapper;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock PluginService mockPluginService;
  @Mock PluginManagerService mockPluginManagerService;
  ConfigurationProfile configurationProfile = ConfigurationProfileBuilder.get().withName("test").build();

  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
  }

  @Test
  public void testExecuteJdbcCallA() throws Exception {

    final ArrayList<String> calls = new ArrayList<>();

    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(calls));
    testPlugins.add(new TestPluginTwo(calls));
    testPlugins.add(new TestPluginThree(calls));

    final Properties testProperties = new Properties();

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

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

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

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

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

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
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

    final Connection conn = target.connect("any",
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("anyHost").build(), testProperties,
        true);

    assertEquals(expectedConnection, conn);
    assertEquals(4, calls.size());
    assertEquals("TestPluginOne:before connect", calls.get(0));
    assertEquals("TestPluginThree:before connect", calls.get(1));
    assertEquals("TestPluginThree:connection", calls.get(2));
    assertEquals("TestPluginOne:after connect", calls.get(3));
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
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

    assertThrows(
        SQLException.class,
        () -> target.connect("any", new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("anyHost").build(),
            testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before connect", calls.get(0));
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
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

    assertThrows(
        SQLException.class,
        () -> target.connect("any", new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("anyHost").build(),
            testProperties, true));

    assertEquals(5, calls.size());
    assertEquals("TestPluginOne:before connect", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
    assertEquals("TestPluginThree:before connect", calls.get(2));
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
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

    final Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> target.connect("any",
                new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("anyHost").build(),
                testProperties, true));

    assertEquals(2, calls.size());
    assertEquals("TestPluginOne:before connect", calls.get(0));
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
    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory);

    final Exception ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> target.connect("any",
                new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("anyHost").build(),
                testProperties, true));

    assertEquals(5, calls.size());
    assertEquals("TestPluginOne:before connect", calls.get(0));
    assertEquals("TestPluginThrowException:before", calls.get(1));
    assertEquals("TestPluginThree:before connect", calls.get(2));
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

    final Object[] testArgs = new Object[] {10, "arg2", 3.33};

    final ConnectionPluginManager target = Mockito.spy(
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper, mockTelemetryFactory));

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

    final Connection mockOldConnection = mock(Connection.class);
    final Connection mockCurrentConnection = mock(Connection.class);
    final Statement mockOldStatement = mock(Statement.class);
    final ResultSet mockOldResultSet = mock(ResultSet.class);

    when(mockPluginService.getCurrentConnection()).thenReturn(mockCurrentConnection);
    when(mockOldStatement.getConnection()).thenReturn(mockOldConnection);
    when(mockOldResultSet.getStatement()).thenReturn(mockOldStatement);

    final ConnectionPluginManager target =
        new ConnectionPluginManager(mockConnectionProvider,
            null, testProperties, testPlugins, mockConnectionWrapper,
            mockPluginService, mockTelemetryFactory);

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

  @Test
  public void testDefaultPlugins() throws SQLException {
    final Properties testProperties = new Properties();

    final ConnectionPluginManager target = Mockito.spy(new ConnectionPluginManager(
        mockConnectionProvider,
        null,
        mockConnectionWrapper,
        mockTelemetryFactory));
    target.init(mockPluginService, testProperties, mockPluginManagerService, configurationProfile);

    assertEquals(4, target.plugins.size());
    assertEquals(AuroraConnectionTrackerPlugin.class, target.plugins.get(0).getClass());
    assertEquals(FailoverConnectionPlugin.class, target.plugins.get(1).getClass());
    assertEquals(HostMonitoringConnectionPlugin.class, target.plugins.get(2).getClass());
    assertEquals(DefaultConnectionPlugin.class, target.plugins.get(3).getClass());
  }

  @Test
  public void testNoWrapperPlugins() throws SQLException {
    final Properties testProperties = new Properties();
    testProperties.setProperty(PropertyDefinition.PLUGINS.name, "");

    final ConnectionPluginManager target = Mockito.spy(new ConnectionPluginManager(
        mockConnectionProvider,
        null,
        mockConnectionWrapper,
        mockTelemetryFactory));
    target.init(mockPluginService, testProperties, mockPluginManagerService, configurationProfile);

    assertEquals(1, target.plugins.size());
  }

  @Test
  public void testOverridingDefaultPluginsWithPluginCodes() throws SQLException {
    final Properties testProperties = new Properties();
    testProperties.setProperty("wrapperPlugins", "logQuery");

    final ConnectionPluginManager target = Mockito.spy(new ConnectionPluginManager(
        mockConnectionProvider,
        null,
        mockConnectionWrapper,
        mockTelemetryFactory));
    target.init(mockPluginService, testProperties, mockPluginManagerService, configurationProfile);

    assertEquals(2, target.plugins.size());
    assertEquals(LogQueryConnectionPlugin.class, target.plugins.get(0).getClass());
    assertEquals(DefaultConnectionPlugin.class, target.plugins.get(1).getClass());
  }

  @Test
  public void testTwoConnectionsDoNotBlockOneAnother() throws Exception {

    final Properties testProperties = new Properties();
    final ArrayList<ConnectionPlugin> testPlugins = new ArrayList<>();
    testPlugins.add(new TestPluginOne(new ArrayList<>()));

    final ConnectionProvider mockConnectionProvider1 = Mockito.mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper1 = Mockito.mock(ConnectionWrapper.class);
    final PluginService mockPluginService1 = Mockito.mock(PluginService.class);
    final TelemetryFactory mockTelemetryFactory1 = Mockito.mock(TelemetryFactory.class);
    final Object object1 = new Object();
    when(mockPluginService1.getTelemetryFactory()).thenReturn(mockTelemetryFactory1);
    when(mockTelemetryFactory1.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory1.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);

    final ConnectionPluginManager pluginManager1 =
        new ConnectionPluginManager(mockConnectionProvider1,
            null, testProperties, testPlugins, mockConnectionWrapper1,
            mockPluginService1, mockTelemetryFactory1);

    final ConnectionProvider mockConnectionProvider2 = Mockito.mock(ConnectionProvider.class);
    final ConnectionWrapper mockConnectionWrapper2 = Mockito.mock(ConnectionWrapper.class);
    final PluginService mockPluginService2 = Mockito.mock(PluginService.class);
    final TelemetryFactory mockTelemetryFactory2 = Mockito.mock(TelemetryFactory.class);
    final Object object2 = new Object();
    when(mockPluginService2.getTelemetryFactory()).thenReturn(mockTelemetryFactory2);
    when(mockTelemetryFactory2.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory2.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);

    final ConnectionPluginManager pluginManager2 =
        new ConnectionPluginManager(mockConnectionProvider2,
            null, testProperties, testPlugins, mockConnectionWrapper2,
            mockPluginService2, mockTelemetryFactory2);

    // Imaginary database resource is considered "locked" when latch is 0
    final CountDownLatch waitForDbResourceLocked = new CountDownLatch(1);
    final ReentrantLock dbResourceLock = new ReentrantLock();
    final CountDownLatch waitForReleaseDbResourceToProceed = new CountDownLatch(1);
    final AtomicBoolean dbResourceReleased = new AtomicBoolean(false);
    final AtomicBoolean acquireDbResourceLockSuccessful = new AtomicBoolean(false);

    CompletableFuture.allOf(

        // Thread 1
        CompletableFuture.runAsync(() -> {

          LOGGER.info("thread-1: started");

          WrapperUtils.executeWithPlugins(
              Integer.class,
              pluginManager1,
              object1,
              "lock-db-resource-from-thread-1",
              () -> {
                dbResourceLock.lock();
                waitForDbResourceLocked.countDown();
                LOGGER.info("thread-1: locked");
                return 1;
              });

          LOGGER.info("thread-1: waiting for thread-2");
          try {
            waitForReleaseDbResourceToProceed.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          LOGGER.info("thread-1: continue");

          WrapperUtils.executeWithPlugins(
              Integer.class,
              pluginManager1,
              object1,
              "release-db-resource-from-thread-1",
              () -> {
                dbResourceLock.unlock();
                dbResourceReleased.set(true);
                LOGGER.info("thread-1: unlocked");
                return 1;
              });
          LOGGER.info("thread-1: completed");
        }),

        // Thread 2
        CompletableFuture.runAsync(() -> {

          LOGGER.info("thread-2: started");
          LOGGER.info("thread-2: waiting for thread-1");
          try {
            waitForDbResourceLocked.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          LOGGER.info("thread-2: continue");

          WrapperUtils.executeWithPlugins(
              Integer.class,
              pluginManager2,
              object2,
              "lock-db-resource-from-thread-2",
              () -> {
                waitForReleaseDbResourceToProceed.countDown();
                LOGGER.info("thread-2: try to acquire a lock");
                try {
                  acquireDbResourceLockSuccessful.set(dbResourceLock.tryLock(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                return 1;
              });
          LOGGER.info("thread-2: completed");
        })
    ).join();

    assertTrue(dbResourceReleased.get());
    assertTrue(acquireDbResourceLockSuccessful.get());
  }
}
