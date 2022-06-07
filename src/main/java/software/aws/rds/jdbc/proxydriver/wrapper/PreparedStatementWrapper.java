/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class PreparedStatementWrapper implements PreparedStatement {

    protected PreparedStatement statement;
    protected Class<?> statementClass;
    protected ConnectionPluginManager pluginManager;

    public PreparedStatementWrapper(PreparedStatement statement,
                                    ConnectionPluginManager pluginManager) {
        if (statement == null) {
            throw new IllegalArgumentException("statement");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.statement = statement;
        this.statementClass = this.statement.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeQuery",
                () -> this.statement.executeQuery());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNull",
                () -> {
                    this.statement.setNull(parameterIndex, sqlType);
                    return null;
                },
                parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBoolean",
                () -> {
                    this.statement.setBoolean(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setByte",
                () -> {
                    this.statement.setByte(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setShort",
                () -> {
                    this.statement.setShort(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setInt",
                () -> {
                    this.statement.setInt(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setLong",
                () -> {
                    this.statement.setLong(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setFloat",
                () -> {
                    this.statement.setFloat(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setDouble",
                () -> {
                    this.statement.setDouble(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBigDecimal",
                () -> {
                    this.statement.setBigDecimal(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setString",
                () -> {
                    this.statement.setString(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBytes",
                () -> {
                    this.statement.setBytes(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setDate",
                () -> {
                    this.statement.setDate(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setTime",
                () -> {
                    this.statement.setTime(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setTimestamp",
                () -> {
                    this.statement.setTimestamp(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setAsciiStream",
                () -> {
                    this.statement.setAsciiStream(parameterIndex, x, length);
                    return null;
                },
                parameterIndex, x, length);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setUnicodeStream",
                () -> {
                    this.statement.setUnicodeStream(parameterIndex, x, length);
                    return null;
                },
                parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBinaryStream",
                () -> {
                    this.statement.setBinaryStream(parameterIndex, x, length);
                    return null;
                },
                parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.clearParameters",
                () -> {
                    this.statement.clearParameters();
                    return null;
                });
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setObject",
                () -> {
                    this.statement.setObject(parameterIndex, x, targetSqlType);
                    return null;
                },
                parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setObject",
                () -> {
                    this.statement.setObject(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.execute",
                () -> this.statement.execute());
    }

    @Override
    public void addBatch() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.addBatch",
                () -> {
                    this.statement.addBatch();
                    return null;
                });
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setCharacterStream",
                () -> {
                    this.statement.setCharacterStream(parameterIndex, reader, length);
                    return null;
                },
                parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setRef",
                () -> {
                    this.statement.setRef(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBlob",
                () -> {
                    this.statement.setBlob(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setClob",
                () -> {
                    this.statement.setClob(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setArray",
                () -> {
                    this.statement.setArray(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSetMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getMetaData",
                () -> this.statement.getMetaData());
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setDate",
                () -> {
                    this.statement.setDate(parameterIndex, x, cal);
                    return null;
                },
                parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setTime",
                () -> {
                    this.statement.setTime(parameterIndex, x, cal);
                    return null;
                },
                parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setTimestamp",
                () -> {
                    this.statement.setTimestamp(parameterIndex, x, cal);
                    return null;
                },
                parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNull",
                () -> {
                    this.statement.setNull(parameterIndex, sqlType, typeName);
                    return null;
                },
                parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setURL",
                () -> {
                    this.statement.setURL(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ParameterMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getParameterMetaData",
                () -> this.statement.getParameterMetaData());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setRowId",
                () -> {
                    this.statement.setRowId(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNString",
                () -> {
                    this.statement.setNString(parameterIndex, value);
                    return null;
                },
                parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNCharacterStream",
                () -> {
                    this.statement.setNCharacterStream(parameterIndex, value, length);
                    return null;
                },
                parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNClob",
                () -> {
                    this.statement.setNClob(parameterIndex, value);
                    return null;
                },
                parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setClob",
                () -> {
                    this.statement.setClob(parameterIndex, reader, length);
                    return null;
                },
                parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBlob",
                () -> {
                    this.statement.setBlob(parameterIndex, inputStream, length);
                    return null;
                },
                parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNClob",
                () -> {
                    this.statement.setNClob(parameterIndex, reader, length);
                    return null;
                },
                parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        //noinspection SpellCheckingInspection
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setSQLXML",
                () -> {
                    this.statement.setSQLXML(parameterIndex, xmlObject);
                    return null;
                },
                parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setObject",
                () -> {
                    this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
                    return null;
                },
                parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setAsciiStream",
                () -> {
                    this.statement.setAsciiStream(parameterIndex, x, length);
                    return null;
                },
                parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBinaryStream",
                () -> {
                    this.statement.setBinaryStream(parameterIndex, x, length);
                    return null;
                },
                parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setCharacterStream",
                () -> {
                    this.statement.setCharacterStream(parameterIndex, reader, length);
                    return null;
                },
                parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setAsciiStream",
                () -> {
                    this.statement.setAsciiStream(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBinaryStream",
                () -> {
                    this.statement.setBinaryStream(parameterIndex, x);
                    return null;
                },
                parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setCharacterStream",
                () -> {
                    this.statement.setCharacterStream(parameterIndex, reader);
                    return null;
                },
                parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNCharacterStream",
                () -> {
                    this.statement.setNCharacterStream(parameterIndex, value);
                    return null;
                },
                parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setClob",
                () -> {
                    this.statement.setClob(parameterIndex, reader);
                    return null;
                },
                parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setBlob",
                () -> {
                    this.statement.setBlob(parameterIndex, inputStream);
                    return null;
                },
                parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setNClob",
                () -> {
                    this.statement.setNClob(parameterIndex, reader);
                    return null;
                },
                parameterIndex, reader);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setObject",
                () -> {
                    this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
                    return null;
                },
                parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setObject",
                () -> {
                    this.statement.setObject(parameterIndex, x, targetSqlType);
                    return null;
                },
                parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                long.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeLargeUpdate",
                () -> this.statement.executeLargeUpdate());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeQuery",
                () -> this.statement.executeQuery(sql),
                sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate(sql),
                sql);
    }

    @Override
    public void close() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.close",
                () -> {
                    this.statement.close();
                    return null;
                });
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getMaxFieldSize",
                () -> this.statement.getMaxFieldSize());
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setMaxFieldSize",
                () -> {
                    this.statement.setMaxFieldSize(max);
                    return null;
                },
                max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getMaxRows",
                () -> this.statement.getMaxRows());
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setMaxRows",
                () -> {
                    this.statement.setMaxRows(max);
                    return null;
                },
                max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setEscapeProcessing",
                () -> {
                    this.statement.setEscapeProcessing(enable);
                    return null;
                },
                enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getQueryTimeout",
                () -> this.statement.getQueryTimeout());
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setQueryTimeout",
                () -> {
                    this.statement.setQueryTimeout(seconds);
                    return null;
                },
                seconds);
    }

    @Override
    public void cancel() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.cancel",
                () -> {
                    this.statement.cancel();
                    return null;
                });
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLWarning.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getWarnings",
                () -> this.statement.getWarnings());
    }

    @Override
    public void clearWarnings() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.clearWarnings",
                () -> {
                    this.statement.clearWarnings();
                    return null;
                });
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setCursorName",
                () -> {
                    this.statement.setCursorName(name);
                    return null;
                },
                name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.execute",
                () -> this.statement.execute(sql),
                sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getResultSet",
                () -> this.statement.getResultSet());
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getUpdateCount",
                () -> this.statement.getUpdateCount());
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getMoreResults",
                () -> this.statement.getMoreResults());
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getFetchDirection() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getFetchDirection",
                () -> this.statement.getFetchDirection());
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setFetchDirection",
                () -> {
                    this.statement.setFetchDirection(direction);
                    return null;
                },
                direction);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getFetchSize",
                () -> this.statement.getFetchSize());
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setFetchSize",
                () -> {
                    this.statement.setFetchSize(rows);
                    return null;
                },
                rows);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getResultSetConcurrency() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getResultSetConcurrency",
                () -> this.statement.getResultSetConcurrency());
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getResultSetType() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getResultSetType",
                () -> this.statement.getResultSetType());
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.addBatch",
                () -> {
                    this.statement.addBatch(sql);
                    return null;
                },
                sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.clearBatch",
                () -> {
                    this.statement.clearBatch();
                    return null;
                });
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int[].class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeBatch",
                () -> this.statement.executeBatch());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Connection.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getConnection",
                () -> this.statement.getConnection());
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getMoreResults",
                () -> this.statement.getMoreResults(current),
                current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getGeneratedKeys",
                () -> this.statement.getGeneratedKeys());
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate(sql, autoGeneratedKeys),
                sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate(sql, columnIndexes),
                sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate(sql, columnNames),
                sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.execute",
                () -> this.statement.execute(sql, autoGeneratedKeys),
                sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.execute",
                () -> this.statement.execute(sql, columnIndexes),
                sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.execute",
                () -> this.statement.execute(sql, columnNames),
                sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.getResultSetHoldability",
                () -> this.statement.getResultSetHoldability());
    }

    @Override
    public boolean isClosed() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.isClosed",
                () -> this.statement.isClosed());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public boolean isPoolable() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.isPoolable",
                () -> this.statement.isPoolable());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.setPoolable",
                () -> {
                    this.statement.setPoolable(poolable);
                    return null;
                },
                poolable);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.closeOnCompletion",
                () -> {
                    this.statement.closeOnCompletion();
                    return null;
                });
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statementClass,
                "PreparedStatement.isCloseOnCompletion",
                () -> this.statement.isCloseOnCompletion());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.statement.isWrapperFor(iface);
    }
}
