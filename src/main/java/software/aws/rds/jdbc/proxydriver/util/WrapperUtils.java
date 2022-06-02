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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WrapperUtils {

    private static final ConcurrentMap<Class<?>, Class<?>[]> getImplementedInterfacesCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache = new ConcurrentHashMap<>();

    public static synchronized <T> T executeWithPlugins(ConnectionPluginManager pluginManager,
                                                                     Class<?> methodInvokeOn, String methodName,
                                                                     Callable<T> executeSqlFunc, Object... args) {

        Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

        T result;
        try {
            result = pluginManager.execute_SQLException(
                    methodInvokeOn,
                    methodName,
                    executeSqlFunc,
                    argsCopy);
            result = wrapWithProxyIfNeeded(result, pluginManager);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static synchronized <T> T executeWithPlugins_SQLException(ConnectionPluginManager pluginManager,
                                                                     Class<?> methodInvokeOn, String methodName,
                                                                     Callable<T> executeSqlFunc, Object... args) throws SQLException {

        Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

        T result;
        try {
            result = pluginManager.execute_SQLException(
                    methodInvokeOn,
                    methodName,
                    executeSqlFunc,
                    argsCopy);
            result = wrapWithProxyIfNeeded(result, pluginManager);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static synchronized <T> T executeWithPlugins_SQLClientInfoException(
            ConnectionPluginManager pluginManager,
            Class<?> methodInvokeOn, String methodName,
            Callable<T> executeSqlFunc, Object... args) throws SQLClientInfoException {

        Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

        T result;
        try {
            result = pluginManager.execute_SQLClientInfoException(
                    methodInvokeOn,
                    methodName,
                    executeSqlFunc,
                    argsCopy);
            result = wrapWithProxyIfNeeded(result, pluginManager);
        } catch (SQLClientInfoException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    protected static @Nullable <T> T wrapWithProxyIfNeeded(@Nullable T toProxy,
                                                           ConnectionPluginManager pluginManager) {
        if (toProxy == null) {
            return null;
        }

        if (toProxy instanceof ConnectionWrapper
                || toProxy instanceof CallableStatementWrapper
                || toProxy instanceof PreparedStatementWrapper
                || toProxy instanceof StatementWrapper
                || toProxy instanceof ResultSetWrapper) {
            return toProxy;
        }

        if (toProxy instanceof Connection) {
            throw new UnsupportedOperationException("Shouldn't be here");
        }

        if (toProxy instanceof CallableStatement) {
            return (T) new CallableStatementWrapper((CallableStatement) toProxy, pluginManager);
        }

        if (toProxy instanceof PreparedStatement) {
            return (T) new PreparedStatementWrapper((PreparedStatement) toProxy, pluginManager);
        }

        if (toProxy instanceof Statement) {
            return (T) new StatementWrapper((Statement) toProxy, pluginManager);
        }

        if (toProxy instanceof ResultSet) {
            return (T) new ResultSetWrapper((ResultSet) toProxy, pluginManager);
        }

        if (!isJdbcInterface(toProxy.getClass())) {
            return toProxy;
        }

        // Add more custom wrapper support here

        Class<?> toProxyClass = toProxy.getClass();
        return (T) java.lang.reflect.Proxy.newProxyInstance(toProxyClass.getClassLoader(),
                getImplementedInterfaces(toProxyClass),
                new WrapperUtils.Proxy(pluginManager, toProxy));
    }

    /**
     * This class is a proxy for objects created through the proxied connection (for example,
     * {@link java.sql.Statement} and {@link java.sql.ResultSet}).
     * Similarly to ClusterAwareConnectionProxy, this proxy class
     * monitors the underlying object
     * for communications exceptions and initiates failover when required.
     */
    public static class Proxy implements InvocationHandler {
        static final String METHOD_EQUALS = "equals";
        static final String METHOD_HASH_CODE = "hashCode";

        final Object invocationTarget;
        final ConnectionPluginManager pluginManager;

        Proxy(ConnectionPluginManager pluginManager, Object invocationTarget) {
            this.pluginManager = pluginManager;
            this.invocationTarget = invocationTarget;
        }

        public @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args)
                throws Throwable {
            if (METHOD_EQUALS.equals(method.getName()) && args != null && args[0] != null) {
                return args[0].equals(this);
            }

            if (METHOD_HASH_CODE.equals(method.getName())) {
                return this.hashCode();
            }

            synchronized (WrapperUtils.Proxy.this) {
                Object result;
                Object[] argsCopy = args == null ? null : Arrays.copyOf(args, args.length);

                try {
                    result = this.pluginManager.execute(
                            this.invocationTarget.getClass(),
                            method.getName(),
                            () -> method.invoke(this.invocationTarget, args),
                            argsCopy);
                    result = wrapWithProxyIfNeeded(method.getReturnType(), result);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException() == null ? e : e.getTargetException();
                } catch (IllegalStateException e) {
                    throw e.getCause() == null ? e : e.getCause();
                }

                return result;
            }
        }

        protected @Nullable Object wrapWithProxyIfNeeded(Class<?> returnType,
                                                         @Nullable Object toProxy) {
            if (toProxy == null) {
                return null;
            }

            if (toProxy instanceof ConnectionWrapper
                    || toProxy instanceof CallableStatementWrapper
                    || toProxy instanceof PreparedStatementWrapper
                    || toProxy instanceof StatementWrapper
                    || toProxy instanceof ResultSetWrapper) {
                return toProxy;
            }

            // Add custom wrapper support here

            if (!isJdbcInterface(returnType)) {
                return toProxy;
            }

            Class<?> toProxyClass = toProxy.getClass();
            return java.lang.reflect.Proxy.newProxyInstance(toProxyClass.getClassLoader(),
                    getImplementedInterfaces(toProxyClass),
                    new WrapperUtils.Proxy(this.pluginManager, toProxy));
        }

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
        List<String> interceptorsToCreate = split(extensionClassNames, ",", true);
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

    /**
     * Splits stringToSplit into a list, using the given delimiter
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, boolean trim) {
        if (stringToSplit == null) {
            return new ArrayList<>();
        }

        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        String[] tokens = stringToSplit.split(delimiter, -1);
        Stream<String> tokensStream = Arrays.stream(tokens);
        if (trim) {
            tokensStream = tokensStream.map(String::trim);
        }
        return tokensStream.collect(Collectors.toList());
    }

    public static <T> T createInstance(final String className, final Class<T> clazz, final Object... args)
            throws InstantiationException
    {
        if (StringUtils.isNullOrEmpty(className)) {
            throw new IllegalArgumentException("className");
        }

        try {
            Class<?> loaded = Class.forName(className);
            if (args.length == 0) {
                return clazz.cast(loaded.newInstance());
            }

            Class<?>[] argClasses = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                argClasses[i] = args[i].getClass();
            }
            Constructor<?> constructor = loaded.getConstructor(argClasses);
            return clazz.cast(constructor.newInstance(args));
        }
        catch (Exception e) {
            throw new InstantiationException("Can't initialize class " + className);
        }
    }

}
