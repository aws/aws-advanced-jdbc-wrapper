/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import java.lang.reflect.Constructor;
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
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;
import software.aws.rds.jdbc.proxydriver.JdbcRunnable;
import software.aws.rds.jdbc.proxydriver.wrapper.ArrayWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.BlobWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.CallableStatementWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ClobWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.DatabaseMetaDataWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.NClobWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ParameterMetaDataWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.PreparedStatementWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.RefWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ResultSetMetaDataWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ResultSetWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.SQLDataWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.SQLInputWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.SQLOutputWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.SQLTypeWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.SavepointWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.StatementWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.StructWrapper;

public class WrapperUtils {

  private static final ConcurrentMap<Class<?>, Class<?>[]> getImplementedInterfacesCache =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache =
      new ConcurrentHashMap<>();

  private static final Map<Class<?>, Class<?>> availableWrappers =
      new HashMap<Class<?>, Class<?>>() {
        {
          put(Connection.class, ConnectionWrapper.class);
          put(CallableStatement.class, CallableStatementWrapper.class);
          put(PreparedStatement.class, PreparedStatementWrapper.class);
          put(Statement.class, StatementWrapper.class);
          put(ResultSet.class, ResultSetWrapper.class);
          put(Array.class, ArrayWrapper.class);
          put(Blob.class, BlobWrapper.class);
          put(NClob.class, NClobWrapper.class);
          put(Clob.class, ClobWrapper.class);
          put(Ref.class, RefWrapper.class);
          put(Struct.class, StructWrapper.class);
          put(Savepoint.class, SavepointWrapper.class);
          put(DatabaseMetaData.class, DatabaseMetaDataWrapper.class);
          put(ParameterMetaData.class, ParameterMetaDataWrapper.class);
          put(ResultSetMetaData.class, ResultSetMetaDataWrapper.class);
          put(SQLData.class, SQLDataWrapper.class);
          put(SQLInput.class, SQLInputWrapper.class);
          put(SQLOutput.class, SQLOutputWrapper.class);
          put(SQLType.class, SQLTypeWrapper.class);
        }
      };

  // TODO: choose a better name to distinguish runWithPlugins and executeWithPlugins
  public static synchronized void runWithPlugins(
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcRunnable<RuntimeException> jdbcMethodFunc,
      Object... jdbcMethodArgs) {

    executeWithPlugins(
        Void.TYPE,
        RuntimeException.class,
        pluginManager,
        methodInvokeOn,
        methodName,
        () -> {
          jdbcMethodFunc.call();
          return null;
        },
        jdbcMethodArgs);
  }

  // TODO: think about synchronized in this class; they might need to be moved to wrapper classes
  public static synchronized <E extends Exception> void runWithPlugins(
      final Class<E> exceptionClass,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcRunnable<E> jdbcMethodFunc,
      Object... jdbcMethodArgs)
      throws E {

    executeWithPlugins(
        Void.TYPE,
        exceptionClass,
        pluginManager,
        methodInvokeOn,
        methodName,
        () -> {
          jdbcMethodFunc.call();
          return null;
        },
        jdbcMethodArgs);
  }

  public static synchronized <T> T executeWithPlugins(
      final Class<T> resultClass,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, RuntimeException> jdbcMethodFunc,
      Object... jdbcMethodArgs) {

    Object[] argsCopy =
        jdbcMethodArgs == null ? null : Arrays.copyOf(jdbcMethodArgs, jdbcMethodArgs.length);

    T result =
        pluginManager.execute(
            resultClass,
            RuntimeException.class,
            methodInvokeOn,
            methodName,
            jdbcMethodFunc,
            argsCopy);

    try {
      return wrapWithProxyIfNeeded(resultClass, result, pluginManager);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public static synchronized <T, E extends Exception> T executeWithPlugins(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final ConnectionPluginManager pluginManager,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      Object... jdbcMethodArgs)
      throws E {

    Object[] argsCopy =
        jdbcMethodArgs == null ? null : Arrays.copyOf(jdbcMethodArgs, jdbcMethodArgs.length);

    T result =
        pluginManager.execute(
            resultClass, exceptionClass, methodInvokeOn, methodName, jdbcMethodFunc, argsCopy);

    try {
      return wrapWithProxyIfNeeded(resultClass, result, pluginManager);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  protected static @Nullable <T> T wrapWithProxyIfNeeded(
      final Class<T> resultClass, @Nullable T toProxy, final ConnectionPluginManager pluginManager)
      throws InstantiationException {

    if (toProxy == null) {
      return null;
    }

    // Exceptional case
    if (toProxy instanceof RowId || toProxy instanceof SQLXML) {
      return toProxy;
    }

    Class<?> wrapperClass = availableWrappers.get(resultClass);

    if (wrapperClass != null) {
      return createInstance(
          wrapperClass,
          resultClass,
          new Class<?>[] {resultClass, ConnectionPluginManager.class},
          toProxy,
          pluginManager);
    }

    if (isJdbcInterface(toProxy.getClass())) {
      throw new RuntimeException("No wrapper class exists for " + toProxy.getClass().getName());
    }

    return toProxy;
  }

  /**
   * Check whether the given package is a JDBC package.
   *
   * @param packageName the name of the package to analyze
   * @return true if the given package is a JDBC package
   */
  public static boolean isJdbcPackage(@Nullable String packageName) {
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
  public static boolean isJdbcInterface(Class<?> clazz) {
    if (isJdbcInterfaceCache.containsKey(clazz)) {
      return (isJdbcInterfaceCache.get(clazz));
    }

    if (clazz.isInterface()) {
      try {
        Package classPackage = clazz.getPackage();
        if (classPackage != null && isJdbcPackage(classPackage.getName())) {
          isJdbcInterfaceCache.putIfAbsent(clazz, true);
          return true;
        }
      } catch (Exception ex) {
        // Ignore any exceptions since they're caused by runtime-generated classes, or due to class
        // load issues.
      }
    }

    for (Class<?> iface : clazz.getInterfaces()) {
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

  /**
   * Get the {@link Class} objects corresponding to the interfaces implemented by the given class.
   * Calls to this function are cached for improved efficiency.
   *
   * @param clazz the class to analyze
   * @return the interfaces implemented by the given class
   */
  public static Class<?>[] getImplementedInterfaces(Class<?> clazz) {
    Class<?>[] implementedInterfaces = getImplementedInterfacesCache.get(clazz);
    if (implementedInterfaces != null) {
      return implementedInterfaces;
    }

    Set<Class<?>> interfaces = new LinkedHashSet<>();
    Class<?> superClass = clazz;
    do {
      Collections.addAll(interfaces, superClass.getInterfaces());
    } while ((superClass = superClass.getSuperclass()) != null);

    implementedInterfaces = interfaces.toArray(new Class<?>[0]);
    Class<?>[] oldValue = getImplementedInterfacesCache.putIfAbsent(clazz, implementedInterfaces);
    if (oldValue != null) {
      implementedInterfaces = oldValue;
    }

    return implementedInterfaces;
  }

  public static <T> List<T> loadClasses(
      final String extensionClassNames, final Class<T> clazz, final String errorMessage)
      throws InstantiationException {

    List<T> instances = new LinkedList<>();
    List<String> interceptorsToCreate = StringUtils.split(extensionClassNames, ",", true);
    String className = null;

    try {
      for (String value : interceptorsToCreate) {
        className = value;
        T instance = createInstance(className, clazz);
        instances.add(instance);
      }

    } catch (Throwable t) {
      throw new InstantiationException(String.format(errorMessage, className));
    }

    return instances;
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
      if (constructorArgs.length == 0) {
        return resultClass.cast(classToInstantiate.newInstance());
      }

      Class<?>[] argClasses = constructorArgClasses;
      if (argClasses == null) {
        argClasses = new Class<?>[constructorArgs.length];
        for (int i = 0; i < constructorArgs.length; i++) {
          argClasses[i] = constructorArgs[i].getClass();
        }
      }
      Constructor<?> constructor = classToInstantiate.getConstructor(argClasses);
      return resultClass.cast(constructor.newInstance(constructorArgs));
    } catch (Exception e) {
      throw new InstantiationException("Can't initialize class " + classToInstantiate.getName());
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

    Class<?> loaded;
    try {
      loaded = Class.forName(className);
    } catch (Exception e) {
      throw new InstantiationException("Can't initialize class " + className);
    }

    return createInstance(loaded, resultClass, null, constructorArgs);
  }
}
