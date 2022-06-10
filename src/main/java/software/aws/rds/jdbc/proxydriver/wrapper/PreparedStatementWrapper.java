/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
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
    protected ConnectionPluginManager pluginManager;

    public PreparedStatementWrapper(@NonNull PreparedStatement statement,
                                    @NonNull ConnectionPluginManager pluginManager) {
        this.statement = statement;
        this.pluginManager = pluginManager;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.executeQuery",
                () -> this.statement.executeQuery());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNull",
                () -> this.statement.setNull(parameterIndex, sqlType),
                parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBoolean",
                () -> this.statement.setBoolean(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setByte",
                () -> this.statement.setByte(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setShort",
                () -> this.statement.setShort(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setInt",
                () -> this.statement.setInt(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setLong",
                () -> this.statement.setLong(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setFloat",
                () -> this.statement.setFloat(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setDouble",
                () -> this.statement.setDouble(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBigDecimal",
                () -> this.statement.setBigDecimal(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setString",
                () -> this.statement.setString(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBytes",
                () -> this.statement.setBytes(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setDate",
                () -> this.statement.setDate(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setTime",
                () -> this.statement.setTime(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setTimestamp",
                () -> this.statement.setTimestamp(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setAsciiStream",
                () -> this.statement.setAsciiStream(parameterIndex, x, length),
                parameterIndex, x, length);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setUnicodeStream",
                () -> this.statement.setUnicodeStream(parameterIndex, x, length),
                parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBinaryStream",
                () -> this.statement.setBinaryStream(parameterIndex, x, length),
                parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.clearParameters",
                () -> this.statement.clearParameters());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setObject",
                () -> this.statement.setObject(parameterIndex, x, targetSqlType),
                parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setObject",
                () -> this.statement.setObject(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.execute",
                () -> this.statement.execute());
    }

    @Override
    public void addBatch() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.addBatch",
                () -> this.statement.addBatch());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setCharacterStream",
                () -> this.statement.setCharacterStream(parameterIndex, reader, length),
                parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setRef",
                () -> this.statement.setRef(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBlob",
                () -> this.statement.setBlob(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setClob",
                () -> this.statement.setClob(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setArray",
                () -> this.statement.setArray(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSetMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getMetaData",
                () -> this.statement.getMetaData());
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setDate",
                () -> this.statement.setDate(parameterIndex, x, cal),
                parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setTime",
                () -> this.statement.setTime(parameterIndex, x, cal),
                parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setTimestamp",
                () -> this.statement.setTimestamp(parameterIndex, x, cal),
                parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNull",
                () -> this.statement.setNull(parameterIndex, sqlType, typeName),
                parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setURL",
                () -> this.statement.setURL(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ParameterMetaData.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getParameterMetaData",
                () -> this.statement.getParameterMetaData());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setRowId",
                () -> this.statement.setRowId(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNString",
                () -> this.statement.setNString(parameterIndex, value),
                parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNCharacterStream",
                () -> this.statement.setNCharacterStream(parameterIndex, value, length),
                parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNClob",
                () -> this.statement.setNClob(parameterIndex, value),
                parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setClob",
                () -> this.statement.setClob(parameterIndex, reader, length),
                parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBlob",
                () -> this.statement.setBlob(parameterIndex, inputStream, length),
                parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNClob",
                () -> this.statement.setNClob(parameterIndex, reader, length),
                parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        //noinspection SpellCheckingInspection
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setSQLXML",
                () -> this.statement.setSQLXML(parameterIndex, xmlObject),
                parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setObject",
                () -> this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength),
                parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setAsciiStream",
                () -> this.statement.setAsciiStream(parameterIndex, x, length),
                parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBinaryStream",
                () -> this.statement.setBinaryStream(parameterIndex, x, length),
                parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setCharacterStream",
                () -> this.statement.setCharacterStream(parameterIndex, reader, length),
                parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setAsciiStream",
                () -> this.statement.setAsciiStream(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBinaryStream",
                () -> this.statement.setBinaryStream(parameterIndex, x),
                parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setCharacterStream",
                () -> this.statement.setCharacterStream(parameterIndex, reader),
                parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNCharacterStream",
                () -> this.statement.setNCharacterStream(parameterIndex, value),
                parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setClob",
                () -> this.statement.setClob(parameterIndex, reader),
                parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setBlob",
                () -> this.statement.setBlob(parameterIndex, inputStream),
                parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setNClob",
                () -> this.statement.setNClob(parameterIndex, reader),
                parameterIndex, reader);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setObject",
                () -> this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength),
                parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setObject",
                () -> this.statement.setObject(parameterIndex, x, targetSqlType),
                parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                long.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.executeLargeUpdate",
                () -> this.statement.executeLargeUpdate());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                ResultSet.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.executeUpdate",
                () -> this.statement.executeUpdate(sql),
                sql);
    }

    @Override
    public void close() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.close",
                () -> this.statement.close());
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getMaxFieldSize",
                () -> this.statement.getMaxFieldSize());
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setMaxFieldSize",
                () -> this.statement.setMaxFieldSize(max),
                max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getMaxRows",
                () -> this.statement.getMaxRows());
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setMaxRows",
                () -> this.statement.setMaxRows(max),
                max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setEscapeProcessing",
                () -> this.statement.setEscapeProcessing(enable),
                enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getQueryTimeout",
                () -> this.statement.getQueryTimeout());
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setQueryTimeout",
                () -> this.statement.setQueryTimeout(seconds),
                seconds);
    }

    @Override
    public void cancel() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.cancel",
                () -> this.statement.cancel());
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLWarning.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getWarnings",
                () -> this.statement.getWarnings());
    }

    @Override
    public void clearWarnings() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.clearWarnings",
                () -> this.statement.clearWarnings());
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setCursorName",
                () -> this.statement.setCursorName(name),
                name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.getResultSet",
                () -> this.statement.getResultSet());
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getUpdateCount",
                () -> this.statement.getUpdateCount());
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.getFetchDirection",
                () -> this.statement.getFetchDirection());
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setFetchDirection",
                () -> this.statement.setFetchDirection(direction),
                direction);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getFetchSize",
                () -> this.statement.getFetchSize());
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setFetchSize",
                () -> this.statement.setFetchSize(rows),
                rows);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getResultSetConcurrency() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.getResultSetType",
                () -> this.statement.getResultSetType());
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.addBatch",
                () -> this.statement.addBatch(sql),
                sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.clearBatch",
                () -> this.statement.clearBatch());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int[].class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.executeBatch",
                () -> this.statement.executeBatch());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Connection.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.getConnection",
                () -> this.statement.getConnection());
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.getGeneratedKeys",
                () -> this.statement.getGeneratedKeys());
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
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
                this.statement,
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
                this.statement,
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
                this.statement,
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
                this.statement,
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
                this.statement,
                "PreparedStatement.getResultSetHoldability",
                () -> this.statement.getResultSetHoldability());
    }

    @Override
    public boolean isClosed() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
                this.statement,
                "PreparedStatement.isPoolable",
                () -> this.statement.isPoolable());
    }

    @SuppressWarnings("SpellCheckingInspection")
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.setPoolable",
                () -> this.statement.setPoolable(poolable),
                poolable);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.statement,
                "PreparedStatement.closeOnCompletion",
                () -> this.statement.closeOnCompletion());
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.statement,
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
