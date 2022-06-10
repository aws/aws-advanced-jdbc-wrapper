/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.*;
import software.aws.rds.jdbc.proxydriver.util.SqlState;
import software.aws.rds.jdbc.proxydriver.util.StringUtils;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ConnectionWrapper implements Connection {

    protected ConnectionPluginManager pluginManager;
    protected PluginService pluginService;
    protected HostListProviderService hostListProviderService;

    protected String targetDriverProtocol;
    protected String originalUrl;

    public ConnectionWrapper(@NonNull Properties props,
                             @NonNull String url,
                             @NonNull ConnectionProvider connectionProvider)
            throws SQLException {

        if (StringUtils.isNullOrEmpty(url)) {
            throw new IllegalArgumentException("url");
        }

        ConnectionPluginManager pluginManager = new ConnectionPluginManager(connectionProvider);
        PluginServiceImpl pluginService = new PluginServiceImpl(pluginManager);

        init(props, url, pluginManager, pluginService, pluginService);
    }

    ConnectionWrapper(@NonNull Properties props,
                      @NonNull String url,
                      @NonNull ConnectionPluginManager connectionPluginManager,
                      @NonNull PluginService pluginService,
                      @NonNull HostListProviderService hostListProviderService)
            throws SQLException {

        if (StringUtils.isNullOrEmpty(url)) {
            throw new IllegalArgumentException("url");
        }

        init(props, url, connectionPluginManager, pluginService, hostListProviderService);
    }

    protected void init(Properties props,
                        String url,
                        ConnectionPluginManager connectionPluginManager,
                        PluginService pluginService,
                        HostListProviderService hostListProviderService) throws SQLException {

        this.originalUrl = url;
        this.targetDriverProtocol = getProtocol(url);
        this.pluginManager = connectionPluginManager;
        this.pluginService = pluginService;
        this.hostListProviderService = hostListProviderService;

        this.pluginManager.init(this.pluginService, props);

        this.pluginManager.initHostProvider(this.targetDriverProtocol, this.originalUrl, props, this.hostListProviderService);

        if (this.pluginService.getCurrentConnection() == null) {
            Connection conn = this.pluginManager.connect(
                    this.targetDriverProtocol,
                    this.pluginService.getCurrentHostSpec(),
                    props,
                    true);

            if (conn == null) {
                throw new SQLException("Initial connection isn't open.", SqlState.UNKNOWN_STATE.getCode());
            }

            this.pluginService.setCurrentConnection(conn, this.pluginService.getCurrentHostSpec());
        }
    }

    protected String getProtocol(String url) {
        int index = url.indexOf("//");
        if (index < 0) {
            throw new IllegalArgumentException("Url should contains a driver protocol. Protocol is not found in url " + url);
        }
        return url.substring(0, index + 2);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createStatement",
                () -> this.pluginService.getCurrentConnection().createStatement());
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(sql),
                sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                CallableStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareCall",
                () -> this.pluginService.getCurrentConnection().prepareCall(sql),
                sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.nativeSQL",
                () -> this.pluginService.getCurrentConnection().nativeSQL(sql),
                sql);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getAutoCommit",
                () -> this.pluginService.getCurrentConnection().getAutoCommit());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setAutoCommit",
                () -> this.pluginService.getCurrentConnection().setAutoCommit(autoCommit),
                autoCommit);
    }

    @Override
    public void commit() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.commit",
                () -> this.pluginService.getCurrentConnection().commit());
    }

    @Override
    public void rollback() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.rollback",
                () -> this.pluginService.getCurrentConnection().rollback());
    }

    @Override
    public void close() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.close",
                () -> this.pluginService.getCurrentConnection().close());
    }

    @Override
    public boolean isClosed() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.isClosed",
                () -> this.pluginService.getCurrentConnection().isClosed());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                DatabaseMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getMetaData",
                () -> this.pluginService.getCurrentConnection().getMetaData());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.isReadOnly",
                () -> this.pluginService.getCurrentConnection().isReadOnly());
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setReadOnly",
                () -> this.pluginService.getCurrentConnection().setReadOnly(readOnly),
                readOnly);
    }

    @Override
    public String getCatalog() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getCatalog",
                () -> this.pluginService.getCurrentConnection().getCatalog());
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setCatalog",
                () -> this.pluginService.getCurrentConnection().setCatalog(catalog),
                catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getTransactionIsolation",
                () -> this.pluginService.getCurrentConnection().getTransactionIsolation());
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setTransactionIsolation",
                () -> this.pluginService.getCurrentConnection().setTransactionIsolation(level),
                level);
    }

    @Override
    public synchronized SQLWarning getWarnings() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLWarning.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getWarnings",
                () -> this.pluginService.getCurrentConnection().getWarnings());
    }

    @Override
    public synchronized void clearWarnings() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.clearWarnings",
                () -> this.pluginService.getCurrentConnection().clearWarnings());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createStatement",
                () -> this.pluginService.getCurrentConnection().createStatement(resultSetType, resultSetConcurrency),
                resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(sql, resultSetType, resultSetConcurrency),
                sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                CallableStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareCall",
                () -> this.pluginService.getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency),
                sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        //noinspection unchecked
        return WrapperUtils.executeWithPlugins(
                Map.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getTypeMap",
                () -> this.pluginService.getCurrentConnection().getTypeMap());
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setTypeMap",
                () -> this.pluginService.getCurrentConnection().setTypeMap(map),
                map);
    }

    @Override
    public int getHoldability() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getHoldability",
                () -> this.pluginService.getCurrentConnection().getHoldability());
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setHoldability",
                () -> this.pluginService.getCurrentConnection().setHoldability(holdability),
                holdability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Savepoint.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setSavepoint",
                () -> this.pluginService.getCurrentConnection().setSavepoint());
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Savepoint.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setSavepoint",
                () -> this.pluginService.getCurrentConnection().setSavepoint(name),
                name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.rollback",
                () -> this.pluginService.getCurrentConnection().rollback(savepoint),
                savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.releaseSavepoint",
                () -> this.pluginService.getCurrentConnection().releaseSavepoint(savepoint),
                savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createStatement",
                () -> this.pluginService.getCurrentConnection().createStatement(
                        resultSetType, resultSetConcurrency, resultSetHoldability),
                resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(
                        sql, resultSetType, resultSetConcurrency, resultSetHoldability),
                sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                CallableStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareCall",
                () -> this.pluginService.getCurrentConnection().prepareCall(
                        sql, resultSetType, resultSetConcurrency, resultSetHoldability),
                sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(sql, autoGeneratedKeys),
                sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(sql, columnIndexes),
                sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.prepareStatement",
                () -> this.pluginService.getCurrentConnection().prepareStatement(sql, columnNames),
                sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Clob.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createClob",
                () -> this.pluginService.getCurrentConnection().createClob());
    }

    @Override
    public Blob createBlob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Blob.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createBlob",
                () -> this.pluginService.getCurrentConnection().createBlob());
    }

    @Override
    public NClob createNClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                NClob.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createNClob",
                () -> this.pluginService.getCurrentConnection().createNClob());
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLXML.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createSQLXML",
                () -> this.pluginService.getCurrentConnection().createSQLXML());
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.isValid",
                () -> this.pluginService.getCurrentConnection().isValid(timeout),
                timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        WrapperUtils.runWithPlugins(
                SQLClientInfoException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setClientInfo",
                () -> this.pluginService.getCurrentConnection().setClientInfo(name, value),
                name, value);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getClientInfo",
                () -> this.pluginService.getCurrentConnection().getClientInfo(name),
                name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Properties.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getClientInfo",
                () -> this.pluginService.getCurrentConnection().getClientInfo());
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        WrapperUtils.runWithPlugins(
                SQLClientInfoException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setClientInfo",
                () -> this.pluginService.getCurrentConnection().setClientInfo(properties),
                properties);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Array.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createArrayOf",
                () -> this.pluginService.getCurrentConnection().createArrayOf(typeName, elements),
                typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Struct.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.createStruct",
                () -> this.pluginService.getCurrentConnection().createStruct(typeName, attributes),
                typeName, attributes);
    }

    @Override
    public String getSchema() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getSchema",
                () -> this.pluginService.getCurrentConnection().getSchema());
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setSchema",
                () -> this.pluginService.getCurrentConnection().setSchema(schema),
                schema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.abort",
                () -> this.pluginService.getCurrentConnection().abort(executor),
                executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.setNetworkTimeout",
                () -> this.pluginService.getCurrentConnection().setNetworkTimeout(executor, milliseconds),
                executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.pluginService.getCurrentConnection(),
                "Connection.getNetworkTimeout",
                () -> this.pluginService.getCurrentConnection().getNetworkTimeout());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.pluginService.getCurrentConnection().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.pluginService.getCurrentConnection().isWrapperFor(iface);
    }
}
