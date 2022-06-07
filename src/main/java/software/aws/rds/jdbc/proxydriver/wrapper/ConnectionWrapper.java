/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.ConnectionProvider;
import software.aws.rds.jdbc.proxydriver.CurrentConnectionProvider;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.util.SqlState;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

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

public class ConnectionWrapper implements Connection, CurrentConnectionProvider {

    protected Connection currentConnection;
    protected Class<?> currentConnectionClass;
    protected HostSpec hostSpec;
    protected HostSpec[] hostSpecs;
    protected ConnectionPluginManager pluginManager;

    public ConnectionWrapper(ConnectionProvider connectionProvider, Properties props, String url)
            throws SQLException {
        this(null, props, url, new ConnectionPluginManager(connectionProvider));
    }

    ConnectionWrapper(Connection connection, Properties props, String url,
                      ConnectionPluginManager connectionPluginManager)
            throws SQLException {

        this.currentConnection = connection;
        this.hostSpecs = null; //TODO: it will be replaced by topology
        this.pluginManager = connectionPluginManager;

        if (this.pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.pluginManager.init(this, props);
        if (this.currentConnection == null) {
            this.pluginManager.openInitialConnection(this.hostSpecs, props, url);

            if (this.currentConnection == null) {
                throw new SQLException("Initial connection isn't open.", SqlState.UNKNOWN_STATE.getCode());
            }
        }

        this.currentConnectionClass = this.currentConnection.getClass();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createStatement",
                () -> this.currentConnection.createStatement());
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(sql),
                sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                CallableStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareCall",
                () -> this.currentConnection.prepareCall(sql),
                sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.nativeSQL",
                () -> this.currentConnection.nativeSQL(sql),
                sql);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getAutoCommit",
                () -> this.currentConnection.getAutoCommit());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setAutoCommit",
                () -> {
                    this.currentConnection.setAutoCommit(autoCommit);
                    return null;
                },
                autoCommit);
    }

    @Override
    public void commit() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.commit",
                () -> {
                    this.currentConnection.commit();
                    return null;
                });
    }

    @Override
    public void rollback() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.rollback",
                () -> {
                    this.currentConnection.rollback();
                    return null;
                });
    }

    @Override
    public void close() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.close",
                () -> {
                    this.currentConnection.close();
                    return null;
                });
    }

    @Override
    public boolean isClosed() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.isClosed",
                () -> this.currentConnection.isClosed());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                DatabaseMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getMetaData",
                () -> this.currentConnection.getMetaData());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.isReadOnly",
                () -> this.currentConnection.isReadOnly());
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setReadOnly",
                () -> {
                    this.currentConnection.setReadOnly(readOnly);
                    return null;
                },
                readOnly);
    }

    @Override
    public String getCatalog() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getCatalog",
                () -> this.currentConnection.getCatalog());
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setCatalog",
                () -> {
                    this.currentConnection.setCatalog(catalog);
                    return null;
                },
                catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getTransactionIsolation",
                () -> this.currentConnection.getTransactionIsolation());
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setTransactionIsolation",
                () -> {
                    this.currentConnection.setTransactionIsolation(level);
                    return null;
                },
                level);
    }

    @Override
    public synchronized SQLWarning getWarnings() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLWarning.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getWarnings",
                () -> this.currentConnection.getWarnings());
    }

    @Override
    public synchronized void clearWarnings() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.clearWarnings",
                () -> {
                    this.currentConnection.clearWarnings();
                    return null;
                });
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createStatement",
                () -> this.currentConnection.createStatement(resultSetType, resultSetConcurrency),
                resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(sql, resultSetType, resultSetConcurrency),
                sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return WrapperUtils.executeWithPlugins(
                CallableStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareCall",
                () -> this.currentConnection.prepareCall(sql, resultSetType, resultSetConcurrency),
                sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        //noinspection unchecked
        return WrapperUtils.executeWithPlugins(
                Map.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getTypeMap",
                () -> this.currentConnection.getTypeMap());
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setTypeMap",
                () -> {
                    this.currentConnection.setTypeMap(map);
                    return null;
                },
                map);
    }

    @Override
    public int getHoldability() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getHoldability",
                () -> this.currentConnection.getHoldability());
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setHoldability",
                () -> {
                    this.currentConnection.setHoldability(holdability);
                    return null;
                },
                holdability);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Savepoint.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setSavepoint",
                () -> this.currentConnection.setSavepoint());
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Savepoint.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setSavepoint",
                () -> this.currentConnection.setSavepoint(name),
                name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.rollback",
                () -> {
                    this.currentConnection.rollback(savepoint);
                    return null;
                },
                savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.releaseSavepoint",
                () -> {
                    this.currentConnection.releaseSavepoint(savepoint);
                    return null;
                },
                savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Statement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createStatement",
                () -> this.currentConnection.createStatement(
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
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(
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
                this.currentConnectionClass,
                "Connection.prepareCall",
                () -> this.currentConnection.prepareCall(
                        sql, resultSetType, resultSetConcurrency, resultSetHoldability),
                sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(sql, autoGeneratedKeys),
                sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(sql, columnIndexes),
                sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                PreparedStatement.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.prepareStatement",
                () -> this.currentConnection.prepareStatement(sql, columnNames),
                sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Clob.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createClob",
                () -> this.currentConnection.createClob());
    }

    @Override
    public Blob createBlob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Blob.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createBlob",
                () -> this.currentConnection.createBlob());
    }

    @Override
    public NClob createNClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                NClob.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createNClob",
                () -> this.currentConnection.createNClob());
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLXML.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createSQLXML",
                () -> this.currentConnection.createSQLXML());
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.isValid",
                () -> this.currentConnection.isValid(timeout),
                timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        WrapperUtils.runWithPlugins(
                SQLClientInfoException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setClientInfo",
                () -> {
                    this.currentConnection.setClientInfo(name, value);
                    return null;
                },
                name, value);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getClientInfo",
                () -> this.currentConnection.getClientInfo(name),
                name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Properties.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getClientInfo",
                () -> this.currentConnection.getClientInfo());
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        WrapperUtils.runWithPlugins(
                SQLClientInfoException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setClientInfo",
                () -> {
                    this.currentConnection.setClientInfo(properties);
                    return null;
                },
                properties);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Array.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createArrayOf",
                () -> this.currentConnection.createArrayOf(typeName, elements),
                typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Struct.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.createStruct",
                () -> this.currentConnection.createStruct(typeName, attributes),
                typeName, attributes);
    }

    @Override
    public String getSchema() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getSchema",
                () -> this.currentConnection.getSchema());
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setSchema",
                () -> {
                    this.currentConnection.setSchema(schema);
                    return null;
                },
                schema);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.abort",
                () -> {
                    this.currentConnection.abort(executor);
                    return null;
                },
                executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.setNetworkTimeout",
                () -> {
                    this.currentConnection.setNetworkTimeout(executor, milliseconds);
                    return null;
                },
                executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.currentConnectionClass,
                "Connection.getNetworkTimeout",
                () -> this.currentConnection.getNetworkTimeout());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.currentConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.currentConnection.isWrapperFor(iface);
    }

    @Override
    public Connection getCurrentConnection() {
        return this.currentConnection;
    }

    @Override
    public HostSpec getCurrentHostSpec() {
        return this.hostSpec;
    }

    @Override
    public void setCurrentConnection(Connection connection, HostSpec hostSpec) {
        this.currentConnection = connection;
        this.hostSpec = hostSpec;
    }
}
