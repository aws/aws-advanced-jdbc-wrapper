/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.wrapper.*;

import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WrapperUtils {

    private static final ConcurrentMap<Class<?>, Class<?>[]> getImplementedInterfacesCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Class<?>> availableWrappers = new HashMap<Class<?>, Class<?>>() {{
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
    }};

    //TODO: choose a better name to distinguish runWithPlugins and executeWithPlugins
    public static synchronized void runWithPlugins(
            final ConnectionPluginManager pluginManager,
            final Class<?> methodInvokeOn,
            final String methodName,
            final Callable<Void> executeSqlFunc,
            Object... args) {

        executeWithPlugins(Void.TYPE, pluginManager, methodInvokeOn, methodName, executeSqlFunc, args);
    }

    public static synchronized <E extends Exception> void runWithPlugins(
            final Class<E> exceptionClass,
            final ConnectionPluginManager pluginManager,
            final Class<?> methodInvokeOn,
            final String methodName,
            final Callable<Void> executeSqlFunc,
            Object... args)
            throws E {

        executeWithPlugins(Void.TYPE, exceptionClass, pluginManager, methodInvokeOn, methodName, executeSqlFunc, args);
    }

    public static synchronized <T> T executeWithPlugins(
            final Class<T> resultClass,
            final ConnectionPluginManager pluginManager,
            final Class<?> methodInvokeOn,
            final String methodName,
            final Callable<T> executeSqlFunc,
            Object... args) {

        Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

        try {
            T result = pluginManager.execute(
                    resultClass,
                    methodInvokeOn,
                    methodName,
                    executeSqlFunc,
                    argsCopy);
            return wrapWithProxyIfNeeded(resultClass, result, pluginManager);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized <T, E extends Exception> T executeWithPlugins(
            final Class<T> resultClass,
            final Class<E> exceptionClass,
            final ConnectionPluginManager pluginManager,
            final Class<?> methodInvokeOn,
            final String methodName,
            final Callable<T> executeSqlFunc,
            Object... args) throws E {

        Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

        try {
            T result = pluginManager.execute(
                    resultClass,
                    methodInvokeOn,
                    methodName,
                    executeSqlFunc,
                    argsCopy);
            return wrapWithProxyIfNeeded(resultClass, result, pluginManager);
        } catch (Exception e) {
            if (exceptionClass.isInstance(e)) {
                throw exceptionClass.cast(e);
            }
            throw new RuntimeException(e);
        }
    }

    protected static @Nullable <T> T wrapWithProxyIfNeeded(
            final Class<T> resultClass,
            @Nullable T toProxy,
            final ConnectionPluginManager pluginManager)
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
            return createInstance(wrapperClass, resultClass, toProxy, pluginManager);
        }

        if (isJdbcInterface(toProxy.getClass())) {
            throw new RuntimeException("No wrapper class exists for " + toProxy.getClass().getName());
        }

        return toProxy;
    }

    /**
     * Check whether the given package is a JDBC package
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
     * Check whether the given class implements a JDBC interface defined in a JDBC package. See {@link #isJdbcPackage(String)}
     * Calls to this function are cached for improved efficiency.
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
                // Ignore any exceptions since they're caused by runtime-generated classes, or due to class load issues.
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
     * Get the {@link Class} objects corresponding to the interfaces implemented by the given class. Calls to this function
     * are cached for improved efficiency.
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

    public static <T> List<T> loadClasses(final String extensionClassNames, final Class<T> clazz, final String errorMessage)
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

    public static <T> T createInstance(final Class<?> classToInstantiate, final Class<T> resultClass, final Object... args)
            throws InstantiationException {

        if (classToInstantiate == null) {
            throw new IllegalArgumentException("classToInstantiate");
        }

        if (resultClass == null) {
            throw new IllegalArgumentException("resultClass");
        }

        try {
            if (args.length == 0) {
                return resultClass.cast(classToInstantiate.newInstance());
            }

            Class<?>[] argClasses = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                argClasses[i] = args[i].getClass();
            }
            Constructor<?> constructor = classToInstantiate.getConstructor(argClasses);
            return resultClass.cast(constructor.newInstance(args));
        }
        catch (Exception e) {
            throw new InstantiationException("Can't initialize class " + classToInstantiate.getName());
        }
    }

    public static <T> T createInstance(final String className, final Class<T> resultClass, final Object... args)
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
        }
        catch (Exception e) {
            throw new InstantiationException("Can't initialize class " + className);
        }

        return createInstance(loaded, resultClass, args);
    }

}
