package software.aws.rds.jdbc.proxydriver.ds;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.Driver;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxyDriverDataSource implements DataSource, Referenceable, Serializable {

    private static final Logger LOGGER = Logger.getLogger("software.aws.rds.jdbc.proxydriver.ds.ProxyDriverDataSource");

    protected transient @Nullable PrintWriter logWriter;
    protected @Nullable String user;
    protected @Nullable String password;
    protected @Nullable String jdbcUrl;
    protected @Nullable String targetDataSourceClassName;
    protected @Nullable Properties targetDataSourceProperties;

    static {
        try {
            if (!Driver.isRegistered()) {
                Driver.register();
            }
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(this.user, this.password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        this.user = username;
        this.password = password;

        if(isNullOrEmpty(this.targetDataSourceClassName) && isNullOrEmpty(this.jdbcUrl)) {
            throw new SQLException("Target data source class name or JDBC url is required.");
        }

        Connection conn = null;

        if(!isNullOrEmpty(this.targetDataSourceClassName)) {

            DataSource targetDataSource = createInstance(this.targetDataSourceClassName, DataSource.class);
            applyProperties(targetDataSource, this.targetDataSourceProperties);
            conn = targetDataSource.getConnection(username, password);

        } else {

            java.sql.Driver targetDriver = DriverManager.getDriver(this.jdbcUrl);

            if (targetDriver == null) {
                throw new SQLException("Can't find a suitable driver for " + this.jdbcUrl);
            }

            Properties props = copyProperties(this.targetDataSourceProperties);
            props.setProperty("user", this.user);
            props.setProperty("password", this.password);
            conn = targetDriver.connect(this.jdbcUrl, props);
        }

        if(conn == null) {
            return null;
        }

        return conn; // TODO: proxy
    }

    public void setTargetDataSourceClassName(String dataSourceClassName) {
        this.targetDataSourceClassName = dataSourceClassName;
    }

    public String getTargetDataSourceClassName() {
        return this.targetDataSourceClassName;
    }

    public void setJdbcUrl(String url) {
        this.jdbcUrl = url;
    }

    public String getJdbcUrl() {
        return this.jdbcUrl;
    }

    public void setTargetDataSourceProperties(Properties dataSourceProps) {
        this.targetDataSourceProperties = dataSourceProps;
    }

    public Properties getTargetDataSourceProperties() {
        return this.targetDataSourceProperties;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUser() {
        return this.user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return this.logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public Reference getReference() throws NamingException {
        // TODO
        return null;
    }

    protected boolean isNullOrEmpty(final String str) {
        return str == null || str.isEmpty();
    }

    protected <T> T createInstance(final String className, final Class<T> clazz, final Object... args)
            throws SQLException
    {
        if (className == null) {
            throw new SQLException("Target data source class name is required.");
        }

        try {
            Class<?> loaded = this.getClass().getClassLoader().loadClass(className);
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
            throw new SQLException("Target data source is not initialized.", e);
        }
    }

    protected void applyProperties(final Object target, final Properties properties)
    {
        if (target == null || properties == null) {
            return;
        }

        List<Method> methods = Arrays.asList(target.getClass().getMethods());
        Enumeration<?> propertyNames = properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            Object key = propertyNames.nextElement();
            String propName = key.toString();
            Object propValue = properties.getProperty(propName);
            if (propValue == null) {
                propValue = properties.get(key);
            }

            setPropertyOnTarget(target, propName, propValue, methods);
        }
    }

    protected void setPropertyOnTarget(final Object target, final String propName, final Object propValue, final List<Method> methods)
    {
        Method writeMethod = null;
        String methodName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

        for (Method method : methods) {
            if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
                writeMethod = method;
                break;
            }
        }

        if (writeMethod == null) {
            methodName = "set" + propName.toUpperCase();
            for (Method method : methods) {
                if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
                    writeMethod = method;
                    break;
                }
            }
        }

        if (writeMethod == null) {
            LOGGER.log(Level.SEVERE, "Property {0} does not exist on target {1}", new Object[]{ propName, target.getClass() });
            throw new RuntimeException(String.format("Property %s does not exist on target %s", propName, target.getClass()));
        }

        try {
            Class<?> paramClass = writeMethod.getParameterTypes()[0];
            if (paramClass == int.class) {
                writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
            }
            else if (paramClass == long.class) {
                writeMethod.invoke(target, Long.parseLong(propValue.toString()));
            }
            else if (paramClass == boolean.class || paramClass == Boolean.class) {
                writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
            }
            else if (paramClass == String.class) {
                writeMethod.invoke(target, propValue.toString());
            }
            else {
                writeMethod.invoke(target, propValue);
            }
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, String.format("Failed to set property %s on target %s", propName, target.getClass()), e);
            throw new RuntimeException(e);
        }
    }

    protected Properties copyProperties(final Properties props)
    {
        Properties copy = new Properties();

        if(props == null) {
            return copy;
        }

        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            copy.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        return copy;
    }
}
