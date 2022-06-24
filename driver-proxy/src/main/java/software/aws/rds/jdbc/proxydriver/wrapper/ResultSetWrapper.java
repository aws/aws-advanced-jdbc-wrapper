/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

public class ResultSetWrapper implements ResultSet {

  protected ResultSet resultSet;
  protected ConnectionPluginManager pluginManager;

  public ResultSetWrapper(
      @NonNull ResultSet resultSet, @NonNull ConnectionPluginManager pluginManager) {
    this.resultSet = resultSet;
    this.pluginManager = pluginManager;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.absolute",
        () -> this.resultSet.absolute(row),
        row);
  }

  @Override
  public void afterLast() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.afterLast",
        () -> this.resultSet.afterLast());
  }

  @Override
  public void beforeFirst() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.beforeFirst",
        () -> this.resultSet.beforeFirst());
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.cancelRowUpdates",
        () -> this.resultSet.cancelRowUpdates());
  }

  @Override
  public void clearWarnings() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.clearWarnings",
        () -> this.resultSet.clearWarnings());
  }

  @Override
  public void close() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.close",
        () -> this.resultSet.close());
  }

  @Override
  public void deleteRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.deleteRow",
        () -> this.resultSet.deleteRow());
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.findColumn",
        () -> this.resultSet.findColumn(columnLabel),
        columnLabel);
  }

  @Override
  public boolean first() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.first",
        () -> this.resultSet.first());
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getArray",
        () -> this.resultSet.getArray(columnIndex),
        columnIndex);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getArray",
        () -> this.resultSet.getArray(columnLabel),
        columnLabel);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getAsciiStream",
        () -> this.resultSet.getAsciiStream(columnIndex),
        columnIndex);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getAsciiStream",
        () -> this.resultSet.getAsciiStream(columnLabel),
        columnLabel);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        BigDecimal.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBigDecimal",
        () -> this.resultSet.getBigDecimal(columnIndex, scale),
        columnIndex,
        scale);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        BigDecimal.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBigDecimal",
        () -> this.resultSet.getBigDecimal(columnLabel, scale),
        columnLabel,
        scale);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        BigDecimal.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBigDecimal",
        () -> this.resultSet.getBigDecimal(columnIndex),
        columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        BigDecimal.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBigDecimal",
        () -> this.resultSet.getBigDecimal(columnLabel),
        columnLabel);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBinaryStream",
        () -> this.resultSet.getBinaryStream(columnIndex),
        columnIndex);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBinaryStream",
        () -> this.resultSet.getBinaryStream(columnLabel),
        columnLabel);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBlob",
        () -> this.resultSet.getBlob(columnIndex),
        columnIndex);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBlob",
        () -> this.resultSet.getBlob(columnLabel),
        columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBoolean",
        () -> this.resultSet.getBoolean(columnIndex),
        columnIndex);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBoolean",
        () -> this.resultSet.getBoolean(columnLabel),
        columnLabel);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getByte",
        () -> this.resultSet.getByte(columnIndex),
        columnIndex);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getByte",
        () -> this.resultSet.getByte(columnLabel),
        columnLabel);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte[].class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBytes",
        () -> this.resultSet.getBytes(columnIndex),
        columnIndex);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte[].class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getBytes",
        () -> this.resultSet.getBytes(columnLabel),
        columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getCharacterStream",
        () -> this.resultSet.getCharacterStream(columnIndex),
        columnIndex);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getCharacterStream",
        () -> this.resultSet.getCharacterStream(columnLabel),
        columnLabel);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getClob",
        () -> this.resultSet.getClob(columnIndex),
        columnIndex);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getClob",
        () -> this.resultSet.getClob(columnLabel),
        columnLabel);
  }

  @Override
  public int getConcurrency() throws SQLException {
    //noinspection MagicConstant
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getConcurrency",
        () -> this.resultSet.getConcurrency());
  }

  @Override
  public String getCursorName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getCursorName",
        () -> this.resultSet.getCursorName());
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Date.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDate",
        () -> this.resultSet.getDate(columnIndex),
        columnIndex);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Date.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDate",
        () -> this.resultSet.getDate(columnLabel),
        columnLabel);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Date.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDate",
        () -> this.resultSet.getDate(columnIndex, cal),
        columnIndex,
        cal);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Date.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDate",
        () -> this.resultSet.getDate(columnLabel, cal),
        columnLabel,
        cal);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        double.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDouble",
        () -> this.resultSet.getDouble(columnIndex),
        columnIndex);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        double.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getDouble",
        () -> this.resultSet.getDouble(columnLabel),
        columnLabel);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    //noinspection MagicConstant
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getFetchDirection",
        () -> this.resultSet.getFetchDirection());
  }

  @Override
  public int getFetchSize() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getFetchSize",
        () -> this.resultSet.getFetchSize());
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        float.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getFloat",
        () -> this.resultSet.getFloat(columnIndex),
        columnIndex);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        float.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getFloat",
        () -> this.resultSet.getFloat(columnLabel),
        columnLabel);
  }

  @Override
  public int getHoldability() throws SQLException {
    //noinspection MagicConstant
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getHoldability",
        () -> this.resultSet.getHoldability());
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getInt",
        () -> this.resultSet.getInt(columnIndex),
        columnIndex);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getInt",
        () -> this.resultSet.getInt(columnLabel),
        columnLabel);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getLong",
        () -> this.resultSet.getLong(columnIndex),
        columnIndex);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getLong",
        () -> this.resultSet.getLong(columnLabel),
        columnLabel);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSetMetaData.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getMetaData",
        () -> this.resultSet.getMetaData());
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNCharacterStream",
        () -> this.resultSet.getNCharacterStream(columnIndex),
        columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNCharacterStream",
        () -> this.resultSet.getNCharacterStream(columnLabel),
        columnLabel);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNClob",
        () -> this.resultSet.getNClob(columnIndex),
        columnIndex);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNClob",
        () -> this.resultSet.getNClob(columnLabel),
        columnLabel);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNString",
        () -> this.resultSet.getNString(columnIndex),
        columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getNString",
        () -> this.resultSet.getNString(columnLabel),
        columnLabel);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnIndex),
        columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnLabel),
        columnLabel);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnIndex, map),
        columnIndex,
        map);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnLabel, map),
        columnLabel,
        map);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        type,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnIndex, type),
        columnIndex,
        type);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        type,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getObject",
        () -> this.resultSet.getObject(columnLabel, type),
        columnLabel,
        type);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getRef",
        () -> this.resultSet.getRef(columnIndex),
        columnIndex);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getRef",
        () -> this.resultSet.getRef(columnLabel),
        columnLabel);
  }

  @Override
  public int getRow() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getRow",
        () -> this.resultSet.getRow());
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        RowId.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getRowId",
        () -> this.resultSet.getRowId(columnIndex),
        columnIndex);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        RowId.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getRowId",
        () -> this.resultSet.getRowId(columnLabel),
        columnLabel);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    //noinspection SpellCheckingInspection
    return WrapperUtils.executeWithPlugins(
        SQLXML.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getSQLXML",
        () -> this.resultSet.getSQLXML(columnIndex),
        columnIndex);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    //noinspection SpellCheckingInspection
    return WrapperUtils.executeWithPlugins(
        SQLXML.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getSQLXML",
        () -> this.resultSet.getSQLXML(columnLabel),
        columnLabel);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        short.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getShort",
        () -> this.resultSet.getShort(columnIndex),
        columnIndex);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        short.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getShort",
        () -> this.resultSet.getShort(columnLabel),
        columnLabel);
  }

  @Override
  public Statement getStatement() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Statement.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getStatement",
        () -> this.resultSet.getStatement());
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getString",
        () -> this.resultSet.getString(columnIndex),
        columnIndex);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getString",
        () -> this.resultSet.getString(columnLabel),
        columnLabel);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Time.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTime",
        () -> this.resultSet.getTime(columnIndex),
        columnIndex);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Time.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTime",
        () -> this.resultSet.getTime(columnLabel),
        columnLabel);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Time.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTime",
        () -> this.resultSet.getTime(columnIndex, cal),
        columnIndex,
        cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Time.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTime",
        () -> this.resultSet.getTime(columnLabel, cal),
        columnLabel,
        cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Timestamp.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTimestamp",
        () -> this.resultSet.getTimestamp(columnIndex),
        columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Timestamp.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTimestamp",
        () -> this.resultSet.getTimestamp(columnLabel),
        columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Timestamp.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTimestamp",
        () -> this.resultSet.getTimestamp(columnIndex, cal),
        columnIndex,
        cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Timestamp.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getTimestamp",
        () -> this.resultSet.getTimestamp(columnLabel, cal),
        columnLabel,
        cal);
  }

  @Override
  public int getType() throws SQLException {
    //noinspection MagicConstant
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getType",
        () -> this.resultSet.getType());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        URL.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getURL",
        () -> this.resultSet.getURL(columnIndex),
        columnIndex);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        URL.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getURL",
        () -> this.resultSet.getURL(columnLabel),
        columnLabel);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getUnicodeStream",
        () -> this.resultSet.getUnicodeStream(columnIndex),
        columnIndex);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getUnicodeStream",
        () -> this.resultSet.getUnicodeStream(columnLabel),
        columnLabel);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        SQLWarning.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.getWarnings",
        () -> this.resultSet.getWarnings());
  }

  @Override
  public void insertRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.insertRow",
        () -> this.resultSet.insertRow());
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.isAfterLast",
        () -> this.resultSet.isAfterLast());
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.isBeforeFirst",
        () -> this.resultSet.isBeforeFirst());
  }

  @Override
  public boolean isClosed() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.isClosed",
        () -> this.resultSet.isClosed());
  }

  @Override
  public boolean isFirst() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.isFirst",
        () -> this.resultSet.isFirst());
  }

  @Override
  public boolean isLast() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.isLast",
        () -> this.resultSet.isLast());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.resultSet.isWrapperFor(iface);
  }

  @Override
  public boolean last() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.last",
        () -> this.resultSet.last());
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.moveToCurrentRow",
        () -> this.resultSet.moveToCurrentRow());
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.moveToInsertRow",
        () -> this.resultSet.moveToInsertRow());
  }

  @Override
  public boolean next() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.next",
        () -> this.resultSet.next());
  }

  @Override
  public boolean previous() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.previous",
        () -> this.resultSet.previous());
  }

  @Override
  public void refreshRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.refreshRow",
        () -> this.resultSet.refreshRow());
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.relative",
        () -> this.resultSet.relative(rows),
        rows);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.rowDeleted",
        () -> this.resultSet.rowDeleted());
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.rowInserted",
        () -> this.resultSet.rowInserted());
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.rowUpdated",
        () -> this.resultSet.rowUpdated());
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.setFetchDirection",
        () -> this.resultSet.setFetchDirection(direction),
        direction);
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.setFetchSize",
        () -> this.resultSet.setFetchSize(rows),
        rows);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.resultSet.unwrap(iface);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateArray",
        () -> this.resultSet.updateArray(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateArray",
        () -> this.resultSet.updateArray(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnLabel, x, length),
        columnLabel,
        x,
        length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnLabel, x, length),
        columnLabel,
        x,
        length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateAsciiStream",
        () -> this.resultSet.updateAsciiStream(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBigDecimal",
        () -> this.resultSet.updateBigDecimal(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBigDecimal",
        () -> this.resultSet.updateBigDecimal(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnLabel, x, length),
        columnLabel,
        x,
        length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnLabel, x, length),
        columnLabel,
        x,
        length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBinaryStream",
        () -> this.resultSet.updateBinaryStream(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnIndex, inputStream, length),
        columnIndex,
        inputStream,
        length);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnLabel, inputStream, length),
        columnLabel,
        inputStream,
        length);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnIndex, inputStream),
        columnIndex,
        inputStream);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBlob",
        () -> this.resultSet.updateBlob(columnLabel, inputStream),
        columnLabel,
        inputStream);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBoolean",
        () -> this.resultSet.updateBoolean(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBoolean",
        () -> this.resultSet.updateBoolean(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateByte",
        () -> this.resultSet.updateByte(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateByte",
        () -> this.resultSet.updateByte(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBytes",
        () -> this.resultSet.updateBytes(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateBytes",
        () -> this.resultSet.updateBytes(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnLabel, reader, length),
        columnLabel,
        reader,
        length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnLabel, reader, length),
        columnLabel,
        reader,
        length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateCharacterStream",
        () -> this.resultSet.updateCharacterStream(columnLabel, reader),
        columnLabel,
        reader);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnIndex, reader, length),
        columnIndex,
        reader,
        length);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnLabel, reader, length),
        columnLabel,
        reader,
        length);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnIndex, reader),
        columnIndex,
        reader);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateClob",
        () -> this.resultSet.updateClob(columnLabel, reader),
        columnLabel,
        reader);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateDate",
        () -> this.resultSet.updateDate(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateDate",
        () -> this.resultSet.updateDate(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateDouble",
        () -> this.resultSet.updateDouble(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateDouble",
        () -> this.resultSet.updateDouble(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateFloat",
        () -> this.resultSet.updateFloat(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateFloat",
        () -> this.resultSet.updateFloat(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateInt",
        () -> this.resultSet.updateInt(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateInt",
        () -> this.resultSet.updateInt(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateLong",
        () -> this.resultSet.updateLong(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateLong",
        () -> this.resultSet.updateLong(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNCharacterStream",
        () -> this.resultSet.updateNCharacterStream(columnIndex, x, length),
        columnIndex,
        x,
        length);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNCharacterStream",
        () -> this.resultSet.updateNCharacterStream(columnLabel, reader, length),
        columnLabel,
        reader,
        length);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNCharacterStream",
        () -> this.resultSet.updateNCharacterStream(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNCharacterStream",
        () -> this.resultSet.updateNCharacterStream(columnLabel, reader),
        columnLabel,
        reader);
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnIndex, nClob),
        columnIndex,
        nClob);
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnLabel, nClob),
        columnLabel,
        nClob);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnIndex, reader, length),
        columnIndex,
        reader,
        length);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnLabel, reader, length),
        columnLabel,
        reader,
        length);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnIndex, reader),
        columnIndex,
        reader);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNClob",
        () -> this.resultSet.updateNClob(columnLabel, reader),
        columnLabel,
        reader);
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNString",
        () -> this.resultSet.updateNString(columnIndex, nString),
        columnIndex,
        nString);
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNString",
        () -> this.resultSet.updateNString(columnLabel, nString),
        columnLabel,
        nString);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNull",
        () -> this.resultSet.updateNull(columnIndex),
        columnIndex);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateNull",
        () -> this.resultSet.updateNull(columnLabel),
        columnLabel);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnIndex, x, scaleOrLength),
        columnIndex,
        x,
        scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnLabel, x, scaleOrLength),
        columnLabel,
        x,
        scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnIndex, x, targetSqlType, scaleOrLength),
        columnIndex,
        x,
        targetSqlType,
        scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnLabel, x, targetSqlType, scaleOrLength),
        columnLabel,
        x,
        targetSqlType,
        scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnIndex, x, targetSqlType),
        columnIndex,
        x,
        targetSqlType);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateObject",
        () -> this.resultSet.updateObject(columnLabel, x, targetSqlType),
        columnLabel,
        x,
        targetSqlType);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateRef",
        () -> this.resultSet.updateRef(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateRef",
        () -> this.resultSet.updateRef(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateRow() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateRow",
        () -> this.resultSet.updateRow());
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateRowId",
        () -> this.resultSet.updateRowId(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateRowId",
        () -> this.resultSet.updateRowId(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateSQLXML",
        () -> this.resultSet.updateSQLXML(columnIndex, xmlObject),
        columnIndex,
        xmlObject);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateSQLXML",
        () -> this.resultSet.updateSQLXML(columnLabel, xmlObject),
        columnLabel,
        xmlObject);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateShort",
        () -> this.resultSet.updateShort(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateShort",
        () -> this.resultSet.updateShort(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateString",
        () -> this.resultSet.updateString(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateString",
        () -> this.resultSet.updateString(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateTime",
        () -> this.resultSet.updateTime(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateTime",
        () -> this.resultSet.updateTime(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateTimestamp",
        () -> this.resultSet.updateTimestamp(columnIndex, x),
        columnIndex,
        x);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.updateTimestamp",
        () -> this.resultSet.updateTimestamp(columnLabel, x),
        columnLabel,
        x);
  }

  @Override
  public boolean wasNull() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        "ResultSet.wasNull",
        () -> this.resultSet.wasNull());
  }
}
