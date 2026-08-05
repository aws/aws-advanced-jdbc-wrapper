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

package software.amazon.jdbc.benchmarks.support;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal in-memory stand-ins for the target driver's JDBC objects.
 *
 * <p>Why this exists: the pre-existing benchmarks mock the target driver with Mockito, whose
 * dynamic-proxy dispatch and invocation recording cost far more than the wrapper code being
 * measured, so the scores say more about Mockito than about the driver. These fakes are plain
 * {@link Proxy} instances backed by a switch on the method name. They allocate nothing per call on
 * the hot methods and record nothing, which keeps the measured delta attributable to the wrapper.
 *
 * <p>They are deliberately not a correct JDBC implementation. Unhandled methods return the
 * zero/null value for their return type rather than throwing, because benchmark code paths probe a
 * long tail of getters and a thrown exception would abort the iteration instead of just being
 * ignored. Do not use these to assert behaviour - they exist to be cheap and predictable.
 *
 * <p>A {@link Proxy} still costs an interface dispatch plus an argument array. That cost sits
 * underneath both sides of every raw-versus-wrapped comparison in this module, so it cancels out of
 * the difference; it does not cancel out of the absolute numbers.
 */
public final class FakeJdbc {

  /** Fixed column value, pre-allocated so row reads do not measure String construction. */
  public static final String STRING_VALUE = "benchmark-value";

  private FakeJdbc() {
  }

  /**
   * Returns a connection that reports the given autocommit state and hands out statements which
   * produce result sets of {@code rowCount} rows.
   */
  public static Connection connection(final boolean autoCommit, final int rowCount) {
    final AtomicBoolean autoCommitState = new AtomicBoolean(autoCommit);
    final Connection[] self = new Connection[1];
    self[0] = proxy(Connection.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getAutoCommit":
          return autoCommitState.get();
        case "setAutoCommit":
          autoCommitState.set((Boolean) args[0]);
          return null;
        case "createStatement":
          return statement(self[0], rowCount);
        case "prepareStatement":
          return preparedStatement(self[0], rowCount);
        case "isClosed":
        case "isReadOnly":
          return false;
        case "getCatalog":
        case "getSchema":
          return "benchmark";
        case "getTransactionIsolation":
          return Connection.TRANSACTION_READ_COMMITTED;
        case "getNetworkTimeout":
          return 0;
        case "getHoldability":
          return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        default:
          return fallback(proxy, method, args);
      }
    });
    return self[0];
  }

  /** Returns a statement bound to {@code connection} that produces {@code rowCount}-row results. */
  public static Statement statement(final Connection connection, final int rowCount) {
    final Statement[] self = new Statement[1];
    self[0] = proxy(Statement.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getConnection":
          return connection;
        case "executeQuery":
        case "getResultSet":
          return resultSet(self[0], rowCount);
        case "execute":
          return true;
        case "executeUpdate":
        case "getUpdateCount":
          return 1;
        case "isClosed":
          return false;
        default:
          return fallback(proxy, method, args);
      }
    });
    return self[0];
  }

  /** Returns a prepared statement bound to {@code connection}; parameter setters are no-ops. */
  public static PreparedStatement preparedStatement(final Connection connection, final int rowCount) {
    final PreparedStatement[] self = new PreparedStatement[1];
    self[0] = proxy(PreparedStatement.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getConnection":
          return connection;
        case "executeQuery":
        case "getResultSet":
          return resultSet(self[0], rowCount);
        case "execute":
          return true;
        case "executeUpdate":
        case "getUpdateCount":
          return 1;
        case "isClosed":
          return false;
        default:
          return fallback(proxy, method, args);
      }
    });
    return self[0];
  }

  /**
   * Returns a forward-only result set over {@code rowCount} synthetic rows. Column values are
   * constants so that per-row benchmarks measure traversal and wrapping rather than value decoding.
   */
  public static ResultSet resultSet(final Statement statement, final int rowCount) {
    final int[] cursor = {0};
    return proxy(ResultSet.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "next":
          if (cursor[0] >= rowCount) {
            return false;
          }
          cursor[0]++;
          return true;
        case "getInt":
          return cursor[0];
        case "getLong":
          return (long) cursor[0];
        case "getDouble":
          return (double) cursor[0];
        case "getString":
          return STRING_VALUE;
        case "getObject":
          return STRING_VALUE;
        case "wasNull":
        case "isClosed":
          return false;
        case "getRow":
          return cursor[0];
        case "getStatement":
          return statement;
        case "getMetaData":
          return metaData();
        default:
          return fallback(proxy, method, args);
      }
    });
  }

  private static ResultSetMetaData metaData() {
    return proxy(ResultSetMetaData.class, (proxy, method, args) -> {
      switch (method.getName()) {
        case "getColumnCount":
          return 4;
        case "getColumnName":
        case "getColumnLabel":
          return "col";
        case "getColumnType":
          return java.sql.Types.VARCHAR;
        default:
          return fallback(proxy, method, args);
      }
    });
  }

  /**
   * Handles the {@link Object} methods a proxy also receives, the {@code Wrapper} contract, and
   * anything else with a type-appropriate default.
   */
  private static Object fallback(final Object proxy, final Method method, final Object[] args) {
    switch (method.getName()) {
      case "hashCode":
        return System.identityHashCode(proxy);
      case "equals":
        return proxy == (args == null ? null : args[0]);
      case "toString":
        return "FakeJdbc:" + method.getDeclaringClass().getSimpleName() + "@"
            + Integer.toHexString(System.identityHashCode(proxy));
      case "unwrap":
        return proxy;
      case "isWrapperFor":
        return false;
      default:
        return defaultValue(method.getReturnType());
    }
  }

  private static Object defaultValue(final Class<?> type) {
    if (!type.isPrimitive()) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == char.class) {
      return (char) 0;
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == float.class) {
      return 0f;
    }
    if (type == double.class) {
      return 0d;
    }
    // void
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(final Class<T> iface, final InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(), new Class<?>[] {iface}, handler);
  }
}
