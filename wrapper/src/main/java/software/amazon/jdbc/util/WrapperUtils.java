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

package software.amazon.jdbc.util;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.JdbcRunnable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;
import software.amazon.jdbc.wrapper.ArrayWrapper;
import software.amazon.jdbc.wrapper.ArrayWrapperFactory;
import software.amazon.jdbc.wrapper.BlobWrapper;
import software.amazon.jdbc.wrapper.BlobWrapperFactory;
import software.amazon.jdbc.wrapper.CallableStatementWrapper;
import software.amazon.jdbc.wrapper.CallableStatementWrapperFactory;
import software.amazon.jdbc.wrapper.ClobWrapper;
import software.amazon.jdbc.wrapper.ClobWrapperFactory;
import software.amazon.jdbc.wrapper.ConnectionWrapper;
import software.amazon.jdbc.wrapper.DatabaseMetaDataWrapper;
import software.amazon.jdbc.wrapper.DatabaseMetaDataWrapperFactory;
import software.amazon.jdbc.wrapper.NClobWrapper;
import software.amazon.jdbc.wrapper.NClobWrapperFactory;
import software.amazon.jdbc.wrapper.ParameterMetaDataWrapper;
import software.amazon.jdbc.wrapper.ParameterMetaDataWrapperFactory;
import software.amazon.jdbc.wrapper.PreparedStatementWrapper;
import software.amazon.jdbc.wrapper.PreparedStatementWrapperFactory;
import software.amazon.jdbc.wrapper.RefWrapper;
import software.amazon.jdbc.wrapper.RefWrapperFactory;
import software.amazon.jdbc.wrapper.ResultSetMetaDataWrapper;
import software.amazon.jdbc.wrapper.ResultSetMetaDataWrapperFactory;
import software.amazon.jdbc.wrapper.ResultSetWrapper;
import software.amazon.jdbc.wrapper.ResultSetWrapperFactory;
import software.amazon.jdbc.wrapper.SQLDataWrapper;
import software.amazon.jdbc.wrapper.SQLDataWrapperFactory;
import software.amazon.jdbc.wrapper.SQLInputWrapper;
import software.amazon.jdbc.wrapper.SQLInputWrapperFactory;
import software.amazon.jdbc.wrapper.SQLOutputWrapper;
import software.amazon.jdbc.wrapper.SQLOutputWrapperFactory;
import software.amazon.jdbc.wrapper.SQLTypeWrapper;
import software.amazon.jdbc.wrapper.SQLTypeWrapperFactory;
import software.amazon.jdbc.wrapper.SavepointWrapper;
import software.amazon.jdbc.wrapper.SavepointWrapperFactory;
import software.amazon.jdbc.wrapper.StatementWrapper;
import software.amazon.jdbc.wrapper.StatementWrapperFactory;
import software.amazon.jdbc.wrapper.StructWrapper;
import software.amazon.jdbc.wrapper.StructWrapperFactory;
import software.amazon.jdbc.wrapper.WrapperFactory;

public class WrapperUtils {

  private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache =
      new ConcurrentHashMap<>();

  private static final Map<Class<?>, WrapperFactory> availableWrappers =
      new HashMap<Class<?>, WrapperFactory>() {
        {
          put(CallableStatement.class, new CallableStatementWrapperFactory());
          put(PreparedStatement.class, new PreparedStatementWrapperFactory());
          put(Statement.class, new StatementWrapperFactory());
          put(ResultSet.class, new ResultSetWrapperFactory());
          put(Array.class, new ArrayWrapperFactory());
          put(Blob.class, new BlobWrapperFactory());
          put(NClob.class, new NClobWrapperFactory());
          put(Clob.class, new ClobWrapperFactory());
          put(Ref.class, new RefWrapperFactory());
          put(Struct.class, new StructWrapperFactory());
          put(Savepoint.class, new SavepointWrapperFactory());
          put(DatabaseMetaData.class, new DatabaseMetaDataWrapperFactory());
          put(ParameterMetaData.class, new ParameterMetaDataWrapperFactory());
          put(ResultSetMetaData.class, new ResultSetMetaDataWrapperFactory());
          put(SQLData.class, new SQLDataWrapperFactory());
          put(SQLInput.class, new SQLInputWrapperFactory());
          put(SQLOutput.class, new SQLOutputWrapperFactory());
          put(SQLType.class, new SQLTypeWrapperFactory());
        }
      };

  private static final Set<Class<?>> allWrapperClasses = new HashSet<Class<?>>() {
    {
      add(ArrayWrapper.class);
      add(BlobWrapper.class);
      add(CallableStatementWrapper.class);
      add(ClobWrapper.class);
      add(ConnectionWrapper.class); // additional
      add(DatabaseMetaDataWrapper.class);
      add(NClobWrapper.class);
      add(ParameterMetaDataWrapper.class);
      add(PreparedStatementWrapper.class);
      add(RefWrapper.class);
      add(ResultSetMetaDataWrapper.class);
      add(ResultSetWrapper.class);
      add(SavepointWrapper.class);
      add(SQLDataWrapper.class);
      add(SQLInputWrapper.class);
      add(SQLOutputWrapper.class);
      add(SQLTypeWrapper.class);
      add(StatementWrapper.class);
      add(StructWrapper.class);
    }
  };

  public static final Set<Class<?>> skipWrappingForClasses = new HashSet<Class<?>>() {
    {
      add(Boolean.class);
      add(String.class);
      add(Float.class);
      add(Integer.class);
      add(BigDecimal.class);
      add(Double.class);
      add(Date.class);
      add(Long.class);
      add(Object.class);
      add(Short.class);
      add(Timer.class);
      add(URL.class);
    }
  };

  public static final Set<String> skipWrappingForPackages = new HashSet<>();

  public static <E extends Exception> void runWithPlugins(
      final Class<E> exceptionClass,
      final ConnectionWrapper connectionWrapper,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final JdbcMethod jdbcMethod,
      final JdbcRunnable<E> jdbcMethodFunc,
      final Object... jdbcMethodArgs)
      throws E {

    executeWithPlugins(
        Void.TYPE,
        exceptionClass,
        connectionWrapper,
        pluginManager,
        methodInvokeOn,
        jdbcMethod,
        () -> {
          jdbcMethodFunc.call();
          return null;
        },
        jdbcMethodArgs);
  }

  public static <T> T executeWithPlugins(
      final Class<T> resultClass,
      final ConnectionWrapper connectionWrapper,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final JdbcMethod jdbcMethod,
      final JdbcCallable<T, RuntimeException> jdbcMethodFunc,
      final Object... jdbcMethodArgs) {

    if (jdbcMethod.shouldLockConnection) {
      pluginManager.lock();
    }
    TelemetryFactory telemetryFactory = pluginManager.getTelemetryFactory();
    TelemetryContext context =
        telemetryFactory.openTelemetryContext(jdbcMethod.methodName, TelemetryTraceLevel.TOP_LEVEL);

    try {
      if (context != null) {
        context.setAttribute("jdbcCall", jdbcMethod.methodName);
      }

      // The target driver may block on Statement.getConnection().
      if (jdbcMethod.shouldLockConnection && jdbcMethod.checkBoundedConnection) {
        final Connection conn = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
        if (conn != null && conn != connectionWrapper.getCurrentConnection()) {
          throw WrapperUtils.wrapExceptionIfNeeded(
              RuntimeException.class,
              new SQLException(
                  Messages.get("ConnectionPluginManager.invokedAgainstOldConnection", new Object[]{methodInvokeOn})));
        }
      }

      final T result =
          pluginManager.execute(
              resultClass,
              RuntimeException.class,
              methodInvokeOn,
              jdbcMethod,
              jdbcMethodFunc,
              jdbcMethodArgs);

      if (context != null) {
        context.setSuccess(true);
      }

      try {
        if (jdbcMethod.wrapResults) {
          return wrapWithProxyIfNeeded(resultClass, result, connectionWrapper, pluginManager);
        } else {
          return result;
        }
      } catch (final InstantiationException e) {
        if (context != null) {
          context.setSuccess(false);
        }
        throw new RuntimeException(e);
      }
    } finally {
      if (jdbcMethod.shouldLockConnection) {
        pluginManager.unlock();
      }
      if (context != null) {
        context.closeContext();
      }
    }
  }

  public static <T, E extends Exception> T executeWithPlugins(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final ConnectionWrapper connectionWrapper,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final JdbcMethod jdbcMethod,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object... jdbcMethodArgs)
      throws E {

    if (jdbcMethod.shouldLockConnection) {
      pluginManager.lock();
    }
    TelemetryFactory telemetryFactory = pluginManager.getTelemetryFactory();
    TelemetryContext context =
        telemetryFactory.openTelemetryContext(jdbcMethod.methodName, TelemetryTraceLevel.TOP_LEVEL);

    try {
      if (context != null) {
        context.setAttribute("jdbcCall", jdbcMethod.methodName);
      }

      // The target driver may block on Statement.getConnection().
      if (jdbcMethod.shouldLockConnection && jdbcMethod.checkBoundedConnection) {
        final Connection conn = WrapperUtils.getConnectionFromSqlObject(methodInvokeOn);
        if (conn != null && conn != connectionWrapper.getCurrentConnection()) {
          throw WrapperUtils.wrapExceptionIfNeeded(
              exceptionClass,
              new SQLException(
                  Messages.get("ConnectionPluginManager.invokedAgainstOldConnection", new Object[]{methodInvokeOn})));
        }
      }

      final T result =
          pluginManager.execute(resultClass,
              exceptionClass,
              methodInvokeOn,
              jdbcMethod,
              jdbcMethodFunc,
              jdbcMethodArgs);

      if (context != null) {
        context.setSuccess(true);
      }

      if (jdbcMethod.wrapResults) {
        try {
          return wrapWithProxyIfNeeded(resultClass, result, connectionWrapper, pluginManager);
        } catch (final InstantiationException e) {
          if (context != null) {
            context.setSuccess(false);
          }
          throw new RuntimeException(e);
        }
      } else {
        return result;
      }

    } finally {
      if (jdbcMethod.shouldLockConnection) {
        pluginManager.unlock();
      }
      if (context != null) {
        context.closeContext();
      }
    }
  }

  public static @Nullable <T> T wrapWithProxyIfNeeded(
      final Class<T> resultClass,
      @Nullable final T toProxy,
      final ConnectionWrapper connectionWrapper,
      final ConnectionPluginManager pluginManager) throws InstantiationException {
    if (toProxy == null) {
      return null;
    }

    final Class<?> toProxyClass = toProxy.getClass();

    // Exceptional case
    if (skipWrappingForClasses.contains(toProxyClass)
        || toProxy instanceof RowId
        || toProxy instanceof SQLXML
        || toProxy instanceof InputStream
        || toProxy instanceof Reader) {
      return toProxy;
    }

    if (allWrapperClasses.contains(toProxy.getClass())) {
      return toProxy;
    }

    Class<?> effectiveResultClass = resultClass;

    if (resultClass == Statement.class) {
      // Statement class is a special case since it has subclasses like PreparedStatement and CallableStatement.
      // We need to choose the best result class based on actual toProxy object.

      // Order of the following if-statements is important!
      if (toProxy instanceof CallableStatement) {
        effectiveResultClass = CallableStatement.class;
      } else if (toProxy instanceof PreparedStatement) {
        effectiveResultClass = PreparedStatement.class;
      }
    }

    WrapperFactory wrapperFactory = availableWrappers.get(effectiveResultClass);

    if (wrapperFactory != null) {
      return resultClass.cast(wrapperFactory.getInstance(toProxy, connectionWrapper, pluginManager));
    }

    for (final Class<?> iface : toProxy.getClass().getInterfaces()) {
      if (isJdbcInterface(iface)) {
        wrapperFactory = availableWrappers.get(iface);
        if (wrapperFactory != null) {
          return resultClass.cast(wrapperFactory.getInstance(toProxy, connectionWrapper, pluginManager));
        }
      }
    }

    if (skipWrappingForPackages.contains(toProxyClass.getPackage().getName())) {
      return toProxy;
    }

    if (isJdbcInterface(toProxy.getClass())) {
      throw new RuntimeException(
          Messages.get(
              "WrapperUtils.noWrapperClassExists",
              new Object[] {toProxy.getClass().getName()}));
    }

    return toProxy;
  }

  /**
   * Check whether the given package is a JDBC package.
   *
   * @param packageName the name of the package to analyze
   * @return true if the given package is a JDBC package
   */
  public static boolean isJdbcPackage(@Nullable final String packageName) {
    return packageName != null
        && (packageName.startsWith("java.sql")
        || packageName.startsWith("javax.sql")
        || packageName.startsWith("org.postgresql"));
  }

  /**
   * Check whether the given class implements a JDBC interface defined in a JDBC package. See {@link
   * #isJdbcPackage(String)} Calls to this function are cached for improved efficiency.
   *
   * @param clazz the class to analyze
   * @return true if the given class implements a JDBC interface
   */
  public static boolean isJdbcInterface(final Class<?> clazz) {
    if (isJdbcInterfaceCache.containsKey(clazz)) {
      return (isJdbcInterfaceCache.get(clazz));
    }

    if (clazz.isInterface()) {
      try {
        final Package classPackage = clazz.getPackage();
        if (classPackage != null && isJdbcPackage(classPackage.getName())) {
          isJdbcInterfaceCache.putIfAbsent(clazz, true);
          return true;
        }
      } catch (final Exception ex) {
        // Ignore any exceptions since they're caused by runtime-generated classes, or due to class
        // load issues.
      }
    }

    for (final Class<?> iface : clazz.getInterfaces()) {
      if (isJdbcInterface(iface)) {
        isJdbcInterfaceCache.putIfAbsent(clazz, true);
        return true;
      }
    }

    if (clazz.getSuperclass() != null && isJdbcInterface(clazz.getSuperclass())) {
      isJdbcInterfaceCache.putIfAbsent(clazz, true);
      return true;
    }

    isJdbcInterfaceCache.putIfAbsent(clazz, false);
    return false;
  }

  public static <T> T createInstance(
      final Class<?> classToInstantiate,
      final Class<T> resultClass,
      final Class<?>[] constructorArgClasses,
      final Object... constructorArgs)
      throws InstantiationException {

    if (classToInstantiate == null) {
      throw new IllegalArgumentException("classToInstantiate");
    }

    if (resultClass == null) {
      throw new IllegalArgumentException("resultClass");
    }

    try {
      if (constructorArgClasses == null
          || constructorArgClasses.length == 0
          || constructorArgs == null
          || constructorArgs.length == 0) {
        return resultClass.cast(classToInstantiate.newInstance());
      }

      final Constructor<?> constructor = classToInstantiate.getConstructor(constructorArgClasses);
      return resultClass.cast(constructor.newInstance(constructorArgs));
    } catch (final Exception e) {
      throw new InstantiationException(
          Messages.get(
              "WrapperUtils.failedToInitializeClass",
              new Object[] {classToInstantiate.getName()}));
    }
  }

  public static <T> T createInstance(
      final String className, final Class<T> resultClass, final Object... constructorArgs)
      throws InstantiationException {

    if (StringUtils.isNullOrEmpty(className)) {
      throw new IllegalArgumentException("className");
    }

    if (resultClass == null) {
      throw new IllegalArgumentException("resultClass");
    }

    final Class<?> loaded;
    try {
      loaded = Class.forName(className);
    } catch (final Exception e) {
      throw new InstantiationException(
          Messages.get(
              "WrapperUtils.failedToInitializeClass",
              new Object[] {className}));
    }

    return createInstance(loaded, resultClass, null, constructorArgs);
  }

  public static Object getFieldValue(Object target, final String accessor) {
    if (target == null) {
      return null;
    }

    final List<String> fieldNames = StringUtils.split(accessor, "\\.", true);
    Class<?> targetClass = target.getClass();

    for (final String fieldName : fieldNames) {
      Field field = null;
      while (targetClass != null && field == null) {
        try {
          field = targetClass.getDeclaredField(fieldName);
        } catch (final Exception ex) {
          // try parent class
          targetClass = targetClass.getSuperclass();
        }
      }

      if (field == null) {
        return null; // field not found
      }

      if (!field.isAccessible()) {
        field.setAccessible(true);
      }

      final Object fieldValue;
      try {
        fieldValue = field.get(target);
      } catch (final Exception ex) {
        return null;
      }

      if (fieldValue == null) {
        return null;
      }

      target = fieldValue;
      targetClass = target.getClass();
    }

    return target;
  }

  public static Connection getConnectionFromSqlObject(final Object obj) {
    if (obj == null) {
      return null;
    }
    try {
      if (obj instanceof Connection) {
        return (Connection) obj;
      } else if (obj instanceof Statement) {
        final Statement stmt = (Statement) obj;
        return !stmt.isClosed() ? stmt.getConnection() : null;
      } else if (obj instanceof ResultSet) {
        final ResultSet rs = (ResultSet) obj;
        final Statement stmt = !rs.isClosed() ? rs.getStatement() : null;
        return stmt != null && !stmt.isClosed() ? stmt.getConnection() : null;
      }
    } catch (final SQLException | UnsupportedOperationException e) {
      // Do nothing. The UnsupportedOperationException comes from ResultSets returned by
      // DataLocalCacheConnectionPlugin and will be triggered when getStatement is called.
    }

    return null;
  }

  /**
   * Check if the throwable is an instance of the given exception and throw it as the required
   * exception class, otherwise throw it as a runtime exception.
   *
   * @param exceptionClass The exception class the exception is expected to be
   * @param exception      The exception that occurred while invoking the given method
   * @param <E>            The exception class the exception is expected to be
   * @return an exception indicating the failure that occurred while invoking the given method
   */
  public static <E extends Exception> E wrapExceptionIfNeeded(final Class<E> exceptionClass,
                                                              final Throwable exception) {
    if (exceptionClass.isAssignableFrom(exception.getClass())) {
      return exceptionClass.cast(exception);
    }

    // wrap in an expected exception type
    E result;
    try {
      result = createInstance(
          exceptionClass,
          exceptionClass,
          new Class<?>[] {Throwable.class},
          exception);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }

    return exceptionClass.cast(result);
  }
}
