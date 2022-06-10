/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.ds;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.proxydriver.DataSourceConnectionProvider;
import software.aws.rds.jdbc.proxydriver.Driver;
import software.aws.rds.jdbc.proxydriver.DriverConnectionProvider;
import software.aws.rds.jdbc.proxydriver.util.PropertyUtils;
import software.aws.rds.jdbc.proxydriver.util.SqlState;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
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

        Properties props = PropertyUtils.copyProperties(this.targetDataSourceProperties);
        props.setProperty("user", this.user);
        props.setProperty("password", this.password);

        if(!isNullOrEmpty(this.targetDataSourceClassName)) {

            DataSource targetDataSource;
            try {
                targetDataSource = WrapperUtils.createInstance(this.targetDataSourceClassName, DataSource.class);
            } catch (InstantiationException instEx) {
                throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getCode(), instEx);
            }
            PropertyUtils.applyProperties(targetDataSource, props);

            return new ConnectionWrapper(props, this.jdbcUrl, new DataSourceConnectionProvider(targetDataSource));

        } else {

            java.sql.Driver targetDriver = DriverManager.getDriver(this.jdbcUrl);

            if (targetDriver == null) {
                throw new SQLException("Can't find a suitable driver for " + this.jdbcUrl);
            }

            return new ConnectionWrapper(props, this.jdbcUrl, new DriverConnectionProvider(targetDriver));
        }
    }

    public void setTargetDataSourceClassName(@Nullable String dataSourceClassName) {
        this.targetDataSourceClassName = dataSourceClassName;
    }

    public @Nullable String getTargetDataSourceClassName() {
        return this.targetDataSourceClassName;
    }

    public void setJdbcUrl(@Nullable String url) {
        this.jdbcUrl = url;
    }

    public @Nullable String getJdbcUrl() {
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
}
