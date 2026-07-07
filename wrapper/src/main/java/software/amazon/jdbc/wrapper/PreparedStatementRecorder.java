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

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Records the parameter and configuration setters applied to a {@link PreparedStatement} (or
 * {@link java.sql.CallableStatement}) so they can be replayed onto a freshly-prepared statement
 * when a read/write splitting plugin rebinds the statement to a different connection for query-level
 * load balancing.
 *
 * <p>The recorder is installed as a {@link Proxy} between the wrapper and the real driver statement,
 * so every setter call is captured without modifying the wrapper's ~45 setter methods. Recording is
 * only installed when rebinding is enabled (see {@code ConnectionWrapper}), so it adds no overhead
 * to the default configuration.
 *
 * <p>A statement becomes non-rebindable if a one-shot argument (an {@link InputStream} or
 * {@link Reader}) is bound (it cannot be replayed), or once a batch is started (a pending batch must
 * not be rerouted).
 */
public class PreparedStatementRecorder implements InvocationHandler {

  /** Statement-level settings (not parameters); replayed before parameters and never cleared by clearParameters. */
  private static final Set<String> STATEMENT_SETTING_METHODS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "setFetchSize", "setFetchDirection", "setMaxRows", "setMaxFieldSize", "setLargeMaxRows",
          "setQueryTimeout", "setPoolable", "setEscapeProcessing", "setCursorName")));

  // The live target is swappable: on rebind, the wrapper points the recorder at the freshly-prepared
  // statement while keeping the same proxy, so subsequent setters continue to be recorded and the
  // wrapper's stable proxy reference delegates to the current target.
  private PreparedStatement target;
  private final List<Recorded> statementSettings = new ArrayList<>();
  private final List<Recorded> parameterOps = new ArrayList<>();
  private boolean rebindable = true;

  private PreparedStatementRecorder(final PreparedStatement target) {
    this.target = target;
  }

  PreparedStatement getTarget() {
    return this.target;
  }

  void setTarget(final PreparedStatement target) {
    this.target = target;
  }

  /**
   * Wraps {@code target} in a recording proxy of the given statement interface (PreparedStatement or
   * CallableStatement) and returns both the proxy (to be used as the wrapper's target) and the
   * recorder (to drive replay on rebind).
   */
  static <T extends PreparedStatement> Installed<T> install(final T target, final Class<T> iface) {
    final PreparedStatementRecorder recorder = new PreparedStatementRecorder(target);
    @SuppressWarnings("unchecked")
    final T proxy = (T) Proxy.newProxyInstance(
        target.getClass().getClassLoader(), new Class<?>[] {iface}, recorder);
    return new Installed<>(proxy, recorder);
  }

  boolean isRebindable() {
    return this.rebindable;
  }

  /** Replays the recorded configuration and parameters onto a freshly-prepared statement. */
  void replay(final PreparedStatement fresh) throws SQLException {
    invokeAll(this.statementSettings, fresh);
    invokeAll(this.parameterOps, fresh);
  }

  private static void invokeAll(final List<Recorded> ops, final PreparedStatement fresh) throws SQLException {
    for (final Recorded op : ops) {
      try {
        op.method.invoke(fresh, op.args);
      } catch (final InvocationTargetException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof SQLException) {
          throw (SQLException) cause;
        }
        throw new SQLException(cause);
      } catch (final IllegalAccessException e) {
        throw new SQLException(e);
      }
    }
  }

  @Override
  public @Nullable Object invoke(final Object proxy, final Method method, final Object @Nullable [] args)
      throws Throwable {
    final String name = method.getName();

    // Object methods are routed to the handler by the JDK proxy; delegate them to the target.
    switch (name) {
      case "toString":
        return "PreparedStatementRecorder[" + this.target + "]";
      case "hashCode":
        return System.identityHashCode(this.target);
      case "equals":
        return proxy == (args == null ? null : args[0]);
      default:
        break;
    }

    final Object result;
    try {
      result = method.invoke(this.target, args);
    } catch (final InvocationTargetException e) {
      throw e.getCause();
    }

    if (STATEMENT_SETTING_METHODS.contains(name)) {
      this.statementSettings.add(new Recorded(method, args));
    } else if ("clearParameters".equals(name)) {
      this.parameterOps.clear();
    } else if ("addBatch".equals(name)) {
      // A pending batch cannot be rerouted to another connection.
      this.rebindable = false;
    } else if ("registerOutParameter".equals(name)
        || (name.startsWith("set") && args != null && args.length >= 1)) {
      // A parameter setter (or CallableStatement OUT-parameter registration).
      if (hasOneShotArg(args)) {
        // Streams/readers are consumed on first execution and cannot be replayed.
        this.rebindable = false;
      } else {
        this.parameterOps.add(new Recorded(method, args));
      }
    }
    return result;
  }

  private static boolean hasOneShotArg(final Object @Nullable [] args) {
    if (args == null) {
      return false;
    }
    for (final Object arg : args) {
      if (arg instanceof InputStream || arg instanceof Reader) {
        return true;
      }
    }
    return false;
  }

  /** A recorded setter invocation: the interface method and the arguments passed. */
  private static final class Recorded {
    private final Method method;
    private final Object @Nullable [] args;

    private Recorded(final Method method, final Object @Nullable [] args) {
      this.method = method;
      this.args = args;
    }
  }

  /** The installed recording proxy paired with its recorder. */
  static final class Installed<T extends PreparedStatement> {
    final T proxy;
    final PreparedStatementRecorder recorder;

    private Installed(final T proxy, final PreparedStatementRecorder recorder) {
      this.proxy = proxy;
      this.recorder = recorder;
    }
  }
}
