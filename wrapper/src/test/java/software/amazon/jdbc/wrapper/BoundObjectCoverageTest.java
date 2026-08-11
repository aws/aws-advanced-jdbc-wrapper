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

package software.amazon.jdbc.wrapper;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionBoundObject;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * Enforces the pairing between {@link JdbcMethod#checkBoundedConnection} and the call sites in the
 * wrapper classes: a method declared with that flag must be routed through
 * {@code WrapperUtils.executeWithPluginsWithBoundObject} (or the rebind-handle variant) so the
 * stale-object check can run. A call site that uses a plain {@code executeWithPlugins} variant
 * instead would silently lose the check.
 *
 * <p>The check is behavioral rather than static: every guarded method (and every overload of it) is
 * invoked on a wrapper whose connection has been swapped out, and must be rejected. This also catches
 * a call site that supplies the wrong bound object.
 */
public class BoundObjectCoverageTest {

  /** Wrapper class to the {@link JdbcMethod} name prefix its call sites use. */
  private static final Map<Class<?>, String> WRAPPERS = new LinkedHashMap<>();

  static {
    WRAPPERS.put(StatementWrapper.class, "Statement");
    WRAPPERS.put(PreparedStatementWrapper.class, "PreparedStatement");
    WRAPPERS.put(CallableStatementWrapper.class, "CallableStatement");
    WRAPPERS.put(ResultSetWrapper.class, "ResultSet");
    WRAPPERS.put(DatabaseMetaDataWrapper.class, "DatabaseMetaData");
  }

  @Mock private ConnectionWrapper connectionWrapper;
  @Mock private FullServicesContainer servicesContainer;
  @Mock private ConnectionPluginManager pluginManager;
  @Mock private PluginService pluginService;
  @Mock private PluginManagerService pluginManagerService;
  @Mock private TelemetryFactory telemetryFactory;

  private final AtomicReference<Connection> currentConnection = new AtomicReference<>();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    currentConnection.set(mock(Connection.class));

    when(connectionWrapper.getCurrentConnection()).thenAnswer(i -> currentConnection.get());
    when(connectionWrapper.getServicesContainer()).thenReturn(servicesContainer);
    when(servicesContainer.getPluginService()).thenReturn(pluginService);
    when(servicesContainer.getPluginManagerService()).thenReturn(pluginManagerService);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    when(pluginService.getCallContext()).thenReturn(new PluginCallContext());
    when(pluginManager.getTelemetryFactory()).thenReturn(telemetryFactory);
    when(pluginManager.mustUsePipeline(any())).thenReturn(true);
    when(pluginManager.execute(any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> ((JdbcCallable<?, ?>) i.getArgument(4)).call());
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void everyGuardedMethodRejectsAStaleWrapper() throws Exception {
    final List<String> notChecked = new ArrayList<>();
    final Map<String, Integer> exercised = new LinkedHashMap<>();
    int verified = 0;

    for (final Map.Entry<Class<?>, String> entry : WRAPPERS.entrySet()) {
      final Class<?> wrapperClass = entry.getKey();
      final String prefix = entry.getValue();
      exercised.put(wrapperClass.getSimpleName(), 0);

      for (final JdbcMethod jdbcMethod : JdbcMethod.values()) {
        // The stale-object check is effective only for methods that also lock the connection.
        if (!jdbcMethod.shouldLockConnection || !jdbcMethod.checkBoundedConnection) {
          continue;
        }
        if (!jdbcMethod.methodName.startsWith(prefix + ".")) {
          continue;
        }
        final String simpleName = jdbcMethod.methodName.substring(prefix.length() + 1);

        // Only methods the wrapper actually implements. Overloads it leaves to the JDBC interface
        // default (for example Statement.executeLargeUpdate(String), which throws
        // UnsupportedOperationException) never reach the pipeline, so staleness cannot apply.
        for (final Method method : wrapperClass.getDeclaredMethods()) {
          if (!method.getName().equals(simpleName)
              || !Modifier.isPublic(method.getModifiers())
              || method.isSynthetic()
              || method.isBridge()
              || Modifier.isStatic(method.getModifiers())) {
            continue;
          }

          // A fresh wrapper created on one connection, then the connection is swapped out.
          final Object wrapper = newStaleWrapper(wrapperClass);
          verified++;
          exercised.merge(wrapperClass.getSimpleName(), 1, Integer::sum);

          if (!rejectsCall(wrapper, method)) {
            notChecked.add(String.format("%s.%s(%s) [%s]",
                wrapperClass.getSimpleName(),
                method.getName(),
                describe(method.getParameterTypes()),
                jdbcMethod.name()));
          }
        }
      }
    }

    // Fails if the audit ever stops finding methods to exercise, for example after a JdbcMethod or
    // wrapper rename leaves a prefix matching nothing.
    assertTrue(verified > 0, "no guarded methods were exercised; the audit is not doing anything");
    for (final Map.Entry<String, Integer> e : exercised.entrySet()) {
      assertTrue(e.getValue() > 0,
          "no guarded method was exercised for " + e.getKey() + "; check its JdbcMethod name prefix");
    }

    assertTrue(notChecked.isEmpty(),
        "these methods are declared with checkBoundedConnection but did not reject a stale wrapper,"
            + " so their call site is not passing a bound object: " + notChecked);
  }

  /** Builds a wrapper on one connection and then swaps the current connection out. */
  private Object newStaleWrapper(final Class<?> wrapperClass) throws Exception {
    final Connection createdOn = mock(Connection.class);
    currentConnection.set(createdOn);

    final Object wrapper;
    if (wrapperClass == StatementWrapper.class) {
      wrapper = new StatementWrapper(mock(Statement.class), connectionWrapper, pluginManager);
    } else if (wrapperClass == PreparedStatementWrapper.class) {
      wrapper = new PreparedStatementWrapper(
          mock(PreparedStatement.class), connectionWrapper, pluginManager);
    } else if (wrapperClass == CallableStatementWrapper.class) {
      wrapper = new CallableStatementWrapper(
          mock(CallableStatement.class), connectionWrapper, pluginManager);
    } else if (wrapperClass == ResultSetWrapper.class) {
      wrapper = new ResultSetWrapper(mock(ResultSet.class), connectionWrapper, pluginManager);
    } else if (wrapperClass == DatabaseMetaDataWrapper.class) {
      wrapper = new DatabaseMetaDataWrapper(
          mock(DatabaseMetaData.class), connectionWrapper, pluginManager);
    } else {
      throw new IllegalArgumentException("unhandled wrapper class " + wrapperClass);
    }

    // Sanity: the wrapper recorded the connection it was created on, and it is no longer current.
    final ConnectionBoundObject bound = (ConnectionBoundObject) wrapper;
    assertTrue(bound.getCreatedOnConnection() == createdOn,
        wrapperClass.getSimpleName() + " did not record the connection it was created on");
    currentConnection.set(mock(Connection.class));
    return wrapper;
  }

  /** True when invoking the method reported the stale-object failure. */
  private boolean rejectsCall(final Object wrapper, final Method method) {
    final Object[] args = new Object[method.getParameterTypes().length];
    for (int i = 0; i < args.length; i++) {
      args[i] = defaultValue(method.getParameterTypes()[i]);
    }

    try {
      method.invoke(wrapper, args);
      return false;
    } catch (final InvocationTargetException e) {
      Throwable cause = e.getCause();
      while (cause != null) {
        if (cause instanceof SQLException
            && cause.getMessage() != null
            && cause.getMessage().contains("internal connection has changed")) {
          return true;
        }
        cause = cause.getCause();
      }
      return false;
    } catch (final IllegalAccessException | IllegalArgumentException e) {
      return false;
    }
  }

  private static Object defaultValue(final Class<?> type) {
    if (type == String.class) {
      return "SELECT 1";
    } else if (type == int.class) {
      return 0;
    } else if (type == long.class) {
      return 0L;
    } else if (type == boolean.class) {
      return false;
    } else if (type == int[].class) {
      return new int[] {1};
    } else if (type == String[].class) {
      return new String[] {"a"};
    }
    return null;
  }

  private static String describe(final Class<?>[] types) {
    final List<String> names = new ArrayList<>();
    for (final Class<?> type : types) {
      names.add(type.getSimpleName());
    }
    return String.join(", ", names);
  }

  @Test
  void guardedMethodsExistForEveryStatementLikeWrapper() {
    // Guards against the audit silently covering nothing if a prefix stops matching, e.g. after a
    // JdbcMethod rename.
    for (final Map.Entry<Class<?>, String> entry : WRAPPERS.entrySet()) {
      final String prefix = entry.getValue();
      final boolean any = Arrays.stream(JdbcMethod.values())
          .anyMatch(m -> m.shouldLockConnection
              && m.checkBoundedConnection
              && m.methodName.startsWith(prefix + "."));
      assertTrue(any, "no guarded JdbcMethod found for prefix " + prefix);
    }
  }
}
