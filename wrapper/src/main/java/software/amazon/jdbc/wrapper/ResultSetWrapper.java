/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.wrapper;

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
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ABSOLUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ABSOLUTE,
          () -> this.resultSet.absolute(row),
          row);
    } else {
      return this.resultSet.absolute(row);
    }
  }

  @Override
  public void afterLast() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_AFTERLAST)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_AFTERLAST,
          () -> this.resultSet.afterLast());
    } else {
      this.resultSet.afterLast();
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_BEFOREFIRST)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_BEFOREFIRST,
          () -> this.resultSet.beforeFirst());
    } else {
      this.resultSet.beforeFirst();
    }
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_CANCELROWUPDATES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_CANCELROWUPDATES,
          () -> this.resultSet.cancelRowUpdates());
    } else {
      this.resultSet.cancelRowUpdates();
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_CLEARWARNINGS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_CLEARWARNINGS,
          () -> this.resultSet.clearWarnings());
    } else {
      this.resultSet.clearWarnings();
    }
  }

  @Override
  public void close() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_CLOSE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_CLOSE,
          () -> this.resultSet.close());
    } else {
      this.resultSet.close();
    }
  }

  @Override
  public void deleteRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_DELETEROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_DELETEROW,
          () -> this.resultSet.deleteRow());
    } else {
      this.resultSet.deleteRow();
    }
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_FINDCOLUMN)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_FINDCOLUMN,
          () -> this.resultSet.findColumn(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.findColumn(columnLabel);
    }
  }

  @Override
  public boolean first() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_FIRST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_FIRST,
          () -> this.resultSet.first());
    } else {
      return this.resultSet.first();
    }
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETARRAY,
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
        JdbcMethod.RESULTSET_GETARRAY,
        () -> this.resultSet.getArray(columnLabel),
        columnLabel);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETASCIISTREAM,
          () -> this.resultSet.getAsciiStream(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getAsciiStream(columnIndex);
    }
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETASCIISTREAM,
          () -> this.resultSet.getAsciiStream(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getAsciiStream(columnLabel);
    }
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBIGDECIMAL,
          () -> this.resultSet.getBigDecimal(columnIndex, scale),
          columnIndex,
          scale);
    } else {
      return this.resultSet.getBigDecimal(columnIndex, scale);
    }
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBIGDECIMAL,
          () -> this.resultSet.getBigDecimal(columnLabel, scale),
          columnLabel,
          scale);
    } else {
      return this.resultSet.getBigDecimal(columnLabel, scale);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBIGDECIMAL,
          () -> this.resultSet.getBigDecimal(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getBigDecimal(columnIndex);
    }
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBIGDECIMAL,
          () -> this.resultSet.getBigDecimal(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getBigDecimal(columnLabel);
    }
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBINARYSTREAM,
          () -> this.resultSet.getBinaryStream(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getBinaryStream(columnIndex);
    }
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBINARYSTREAM,
          () -> this.resultSet.getBinaryStream(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getBinaryStream(columnLabel);
    }
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETBLOB,
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
        JdbcMethod.RESULTSET_GETBLOB,
        () -> this.resultSet.getBlob(columnLabel),
        columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBOOLEAN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBOOLEAN,
          () -> this.resultSet.getBoolean(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getBoolean(columnIndex);
    }
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBOOLEAN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBOOLEAN,
          () -> this.resultSet.getBoolean(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getBoolean(columnLabel);
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBYTE)) {
      return WrapperUtils.executeWithPlugins(
          byte.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBYTE,
          () -> this.resultSet.getByte(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getByte(columnIndex);
    }
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBYTE)) {
      return WrapperUtils.executeWithPlugins(
          byte.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBYTE,
          () -> this.resultSet.getByte(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getByte(columnLabel);
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBYTES,
          () -> this.resultSet.getBytes(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getBytes(columnIndex);
    }
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETBYTES,
          () -> this.resultSet.getBytes(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getBytes(columnLabel);
    }
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETCHARACTERSTREAM,
          () -> this.resultSet.getCharacterStream(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getCharacterStream(columnIndex);
    }
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETCHARACTERSTREAM,
          () -> this.resultSet.getCharacterStream(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getCharacterStream(columnLabel);
    }
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETCLOB,
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
        JdbcMethod.RESULTSET_GETCLOB,
        () -> this.resultSet.getClob(columnLabel),
        columnLabel);
  }

  @Override
  public int getConcurrency() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETCONCURRENCY)) {
      //noinspection MagicConstant
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETCONCURRENCY,
          () -> this.resultSet.getConcurrency());
    } else {
      return this.resultSet.getConcurrency();
    }
  }

  @Override
  public String getCursorName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETCURSORNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETCURSORNAME,
          () -> this.resultSet.getCursorName());
    } else {
      return this.resultSet.getCursorName();
    }
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDATE,
          () -> this.resultSet.getDate(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getDate(columnIndex);
    }
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDATE,
          () -> this.resultSet.getDate(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getDate(columnLabel);
    }
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDATE,
          () -> this.resultSet.getDate(columnIndex, cal),
          columnIndex,
          cal);
    } else {
      return this.resultSet.getDate(columnIndex, cal);
    }
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDATE,
          () -> this.resultSet.getDate(columnLabel, cal),
          columnLabel,
          cal);
    } else {
      return this.resultSet.getDate(columnLabel, cal);
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDOUBLE)) {
      return WrapperUtils.executeWithPlugins(
          double.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDOUBLE,
          () -> this.resultSet.getDouble(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getDouble(columnIndex);
    }
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETDOUBLE)) {
      return WrapperUtils.executeWithPlugins(
          double.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETDOUBLE,
          () -> this.resultSet.getDouble(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getDouble(columnLabel);
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETFETCHDIRECTION)) {
      //noinspection MagicConstant
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETFETCHDIRECTION,
          () -> this.resultSet.getFetchDirection());
    } else {
      return this.resultSet.getFetchDirection();
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETFETCHSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETFETCHSIZE,
          () -> this.resultSet.getFetchSize());
    } else {
      return this.resultSet.getFetchSize();
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETFLOAT)) {
      return WrapperUtils.executeWithPlugins(
          float.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETFLOAT,
          () -> this.resultSet.getFloat(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getFloat(columnIndex);
    }
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETFLOAT)) {
      return WrapperUtils.executeWithPlugins(
          float.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETFLOAT,
          () -> this.resultSet.getFloat(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getFloat(columnLabel);
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETHOLDABILITY)) {
      //noinspection MagicConstant
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETHOLDABILITY,
          () -> this.resultSet.getHoldability());
    } else {
      return this.resultSet.getHoldability();
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETINT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETINT,
          () -> this.resultSet.getInt(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getInt(columnIndex);
    }
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETINT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETINT,
          () -> this.resultSet.getInt(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getInt(columnLabel);
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETLONG)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETLONG,
          () -> this.resultSet.getLong(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getLong(columnIndex);
    }
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETLONG)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETLONG,
          () -> this.resultSet.getLong(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getLong(columnLabel);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSetMetaData.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETMETADATA,
        () -> this.resultSet.getMetaData());
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETNCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETNCHARACTERSTREAM,
          () -> this.resultSet.getNCharacterStream(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getNCharacterStream(columnIndex);
    }
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETNCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETNCHARACTERSTREAM,
          () -> this.resultSet.getNCharacterStream(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getNCharacterStream(columnLabel);
    }
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETNCLOB,
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
        JdbcMethod.RESULTSET_GETNCLOB,
        () -> this.resultSet.getNClob(columnLabel),
        columnLabel);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETNSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETNSTRING,
          () -> this.resultSet.getNString(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getNString(columnIndex);
    }
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETNSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETNSTRING,
          () -> this.resultSet.getNString(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getNString(columnLabel);
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getObject(columnIndex);
    }
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getObject(columnLabel);
    }
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnIndex, map),
          columnIndex,
          map);
    } else {
      return this.resultSet.getObject(columnIndex, map);
    }
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnLabel, map),
          columnLabel,
          map);
    } else {
      return this.resultSet.getObject(columnLabel, map);
    }
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          type,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnIndex, type),
          columnIndex,
          type);
    } else {
      return this.resultSet.getObject(columnIndex, type);
    }
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          type,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETOBJECT,
          () -> this.resultSet.getObject(columnLabel, type),
          columnLabel,
          type);
    } else {
      return this.resultSet.getObject(columnLabel, type);
    }
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETREF,
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
        JdbcMethod.RESULTSET_GETREF,
        () -> this.resultSet.getRef(columnLabel),
        columnLabel);
  }

  @Override
  public int getRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETROW)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETROW,
          () -> this.resultSet.getRow());
    } else {
      return this.resultSet.getRow();
    }
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETROWID)) {
      return WrapperUtils.executeWithPlugins(
          RowId.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETROWID,
          () -> this.resultSet.getRowId(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getRowId(columnIndex);
    }
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETROWID)) {
      return WrapperUtils.executeWithPlugins(
          RowId.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETROWID,
          () -> this.resultSet.getRowId(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getRowId(columnLabel);
    }
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSQLXML)) {
      //noinspection SpellCheckingInspection
      return WrapperUtils.executeWithPlugins(
          SQLXML.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSQLXML,
          () -> this.resultSet.getSQLXML(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getSQLXML(columnIndex);
    }
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSQLXML)) {
      //noinspection SpellCheckingInspection
      return WrapperUtils.executeWithPlugins(
          SQLXML.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSQLXML,
          () -> this.resultSet.getSQLXML(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getSQLXML(columnLabel);
    }
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSHORT)) {
      return WrapperUtils.executeWithPlugins(
          short.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSHORT,
          () -> this.resultSet.getShort(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getShort(columnIndex);
    }
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSHORT)) {
      return WrapperUtils.executeWithPlugins(
          short.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSHORT,
          () -> this.resultSet.getShort(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getShort(columnLabel);
    }
  }

  @Override
  public Statement getStatement() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Statement.class,
        SQLException.class,
        this.pluginManager,
        this.resultSet,
        JdbcMethod.RESULTSET_GETSTATEMENT,
        () -> this.resultSet.getStatement());
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSTRING,
          () -> this.resultSet.getString(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getString(columnIndex);
    }
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETSTRING,
          () -> this.resultSet.getString(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getString(columnLabel);
    }
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIME,
          () -> this.resultSet.getTime(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getTime(columnIndex);
    }
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIME,
          () -> this.resultSet.getTime(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getTime(columnLabel);
    }
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIME,
          () -> this.resultSet.getTime(columnIndex, cal),
          columnIndex,
          cal);
    } else {
      return this.resultSet.getTime(columnIndex, cal);
    }
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIME,
          () -> this.resultSet.getTime(columnLabel, cal),
          columnLabel,
          cal);
    } else {
      return this.resultSet.getTime(columnLabel, cal);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIMESTAMP,
          () -> this.resultSet.getTimestamp(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getTimestamp(columnIndex);
    }
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIMESTAMP,
          () -> this.resultSet.getTimestamp(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getTimestamp(columnLabel);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIMESTAMP,
          () -> this.resultSet.getTimestamp(columnIndex, cal),
          columnIndex,
          cal);
    } else {
      return this.resultSet.getTimestamp(columnIndex, cal);
    }
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTIMESTAMP,
          () -> this.resultSet.getTimestamp(columnLabel, cal),
          columnLabel,
          cal);
    } else {
      return this.resultSet.getTimestamp(columnLabel, cal);
    }
  }

  @Override
  public int getType() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETTYPE)) {
      //noinspection MagicConstant
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETTYPE,
          () -> this.resultSet.getType());
    } else {
      return this.resultSet.getType();
    }
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETURL)) {
      return WrapperUtils.executeWithPlugins(
          URL.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETURL,
          () -> this.resultSet.getURL(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getURL(columnIndex);
    }
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETURL)) {
      return WrapperUtils.executeWithPlugins(
          URL.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETURL,
          () -> this.resultSet.getURL(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getURL(columnLabel);
    }
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETUNICODESTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETUNICODESTREAM,
          () -> this.resultSet.getUnicodeStream(columnIndex),
          columnIndex);
    } else {
      return this.resultSet.getUnicodeStream(columnIndex);
    }
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETUNICODESTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETUNICODESTREAM,
          () -> this.resultSet.getUnicodeStream(columnLabel),
          columnLabel);
    } else {
      return this.resultSet.getUnicodeStream(columnLabel);
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_GETWARNINGS)) {
      return WrapperUtils.executeWithPlugins(
          SQLWarning.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_GETWARNINGS,
          () -> this.resultSet.getWarnings());
    } else {
      return this.resultSet.getWarnings();
    }
  }

  @Override
  public void insertRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_INSERTROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_INSERTROW,
          () -> this.resultSet.insertRow());
    } else {
      this.resultSet.insertRow();
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ISAFTERLAST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ISAFTERLAST,
          () -> this.resultSet.isAfterLast());
    } else {
      return this.resultSet.isAfterLast();
    }
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ISBEFOREFIRST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ISBEFOREFIRST,
          () -> this.resultSet.isBeforeFirst());
    } else {
      return this.resultSet.isBeforeFirst();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ISCLOSED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ISCLOSED,
          () -> this.resultSet.isClosed());
    } else {
      return this.resultSet.isClosed();
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ISFIRST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ISFIRST,
          () -> this.resultSet.isFirst());
    } else {
      return this.resultSet.isFirst();
    }
  }

  @Override
  public boolean isLast() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ISLAST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ISLAST,
          () -> this.resultSet.isLast());
    } else {
      return this.resultSet.isLast();
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.resultSet.isWrapperFor(iface);
  }

  @Override
  public boolean last() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_LAST)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_LAST,
          () -> this.resultSet.last());
    } else {
      return this.resultSet.last();
    }
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_MOVETOCURRENTROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_MOVETOCURRENTROW,
          () -> this.resultSet.moveToCurrentRow());
    } else {
      this.resultSet.moveToCurrentRow();
    }
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_MOVETOINSERTROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_MOVETOINSERTROW,
          () -> this.resultSet.moveToInsertRow());
    } else {
      this.resultSet.moveToInsertRow();
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_NEXT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_NEXT,
          () -> this.resultSet.next());
    } else {
      return this.resultSet.next();
    }
  }

  @Override
  public boolean previous() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_PREVIOUS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_PREVIOUS,
          () -> this.resultSet.previous());
    } else {
      return this.resultSet.previous();
    }
  }

  @Override
  public void refreshRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_REFRESHROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_REFRESHROW,
          () -> this.resultSet.refreshRow());
    } else {
      this.resultSet.refreshRow();
    }
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_RELATIVE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_RELATIVE,
          () -> this.resultSet.relative(rows),
          rows);
    } else {
      return this.resultSet.relative(rows);
    }
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ROWDELETED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ROWDELETED,
          () -> this.resultSet.rowDeleted());
    } else {
      return this.resultSet.rowDeleted();
    }
  }

  @Override
  public boolean rowInserted() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ROWINSERTED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ROWINSERTED,
          () -> this.resultSet.rowInserted());
    } else {
      return this.resultSet.rowInserted();
    }
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_ROWUPDATED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_ROWUPDATED,
          () -> this.resultSet.rowUpdated());
    } else {
      return this.resultSet.rowUpdated();
    }
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_SETFETCHDIRECTION)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_SETFETCHDIRECTION,
          () -> this.resultSet.setFetchDirection(direction),
          direction);
    } else {
      this.resultSet.setFetchDirection(direction);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_SETFETCHSIZE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_SETFETCHSIZE,
          () -> this.resultSet.setFetchSize(rows),
          rows);
    } else {
      this.resultSet.setFetchSize(rows);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.resultSet.unwrap(iface);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEARRAY)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEARRAY,
          () -> this.resultSet.updateArray(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateArray(columnIndex, x);
    }
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEARRAY)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEARRAY,
          () -> this.resultSet.updateArray(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateArray(columnLabel, x);
    }
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateAsciiStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnLabel, x, length),
          columnLabel,
          x,
          length);
    } else {
      this.resultSet.updateAsciiStream(columnLabel, x, length);
    }
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateAsciiStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnLabel, x, length),
          columnLabel,
          x,
          length);
    } else {
      this.resultSet.updateAsciiStream(columnLabel, x, length);
    }
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateAsciiStream(columnIndex, x);
    }
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEASCIISTREAM,
          () -> this.resultSet.updateAsciiStream(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateAsciiStream(columnLabel, x);
    }
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBIGDECIMAL,
          () -> this.resultSet.updateBigDecimal(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateBigDecimal(columnIndex, x);
    }
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBIGDECIMAL,
          () -> this.resultSet.updateBigDecimal(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateBigDecimal(columnLabel, x);
    }
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateBinaryStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnLabel, x, length),
          columnLabel,
          x,
          length);
    } else {
      this.resultSet.updateBinaryStream(columnLabel, x, length);
    }
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateBinaryStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnLabel, x, length),
          columnLabel,
          x,
          length);
    } else {
      this.resultSet.updateBinaryStream(columnLabel, x, length);
    }
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateBinaryStream(columnIndex, x);
    }
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBINARYSTREAM,
          () -> this.resultSet.updateBinaryStream(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateBinaryStream(columnLabel, x);
    }
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateBlob(columnIndex, x);
    }
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateBlob(columnLabel, x);
    }
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnIndex, inputStream, length),
          columnIndex,
          inputStream,
          length);
    } else {
      this.resultSet.updateBlob(columnIndex, inputStream, length);
    }
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnLabel, inputStream, length),
          columnLabel,
          inputStream,
          length);
    } else {
      this.resultSet.updateBlob(columnLabel, inputStream, length);
    }
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnIndex, inputStream),
          columnIndex,
          inputStream);
    } else {
      this.resultSet.updateBlob(columnIndex, inputStream);
    }
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBLOB,
          () -> this.resultSet.updateBlob(columnLabel, inputStream),
          columnLabel,
          inputStream);
    } else {
      this.resultSet.updateBlob(columnLabel, inputStream);
    }
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBOOLEAN,
          () -> this.resultSet.updateBoolean(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateBoolean(columnIndex, x);
    }
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBOOLEAN,
          () -> this.resultSet.updateBoolean(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateBoolean(columnLabel, x);
    }
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBYTE,
          () -> this.resultSet.updateByte(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateByte(columnIndex, x);
    }
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBYTE,
          () -> this.resultSet.updateByte(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateByte(columnLabel, x);
    }
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBYTES,
          () -> this.resultSet.updateBytes(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateBytes(columnIndex, x);
    }
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEBYTES,
          () -> this.resultSet.updateBytes(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateBytes(columnLabel, x);
    }
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateCharacterStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnLabel, reader, length),
          columnLabel,
          reader,
          length);
    } else {
      this.resultSet.updateCharacterStream(columnLabel, reader, length);
    }
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateCharacterStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnLabel, reader, length),
          columnLabel,
          reader,
          length);
    } else {
      this.resultSet.updateCharacterStream(columnLabel, reader, length);
    }
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateCharacterStream(columnIndex, x);
    }
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECHARACTERSTREAM,
          () -> this.resultSet.updateCharacterStream(columnLabel, reader),
          columnLabel,
          reader);
    } else {
      this.resultSet.updateCharacterStream(columnLabel, reader);
    }
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateClob(columnIndex, x);
    }
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateClob(columnLabel, x);
    }
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnIndex, reader, length),
          columnIndex,
          reader,
          length);
    } else {
      this.resultSet.updateClob(columnIndex, reader, length);
    }
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnLabel, reader, length),
          columnLabel,
          reader,
          length);
    } else {
      this.resultSet.updateClob(columnLabel, reader, length);
    }
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnIndex, reader),
          columnIndex,
          reader);
    } else {
      this.resultSet.updateClob(columnIndex, reader);
    }
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATECLOB,
          () -> this.resultSet.updateClob(columnLabel, reader),
          columnLabel,
          reader);
    } else {
      this.resultSet.updateClob(columnLabel, reader);
    }
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEDATE,
          () -> this.resultSet.updateDate(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateDate(columnIndex, x);
    }
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEDATE,
          () -> this.resultSet.updateDate(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateDate(columnLabel, x);
    }
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEDOUBLE,
          () -> this.resultSet.updateDouble(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateDouble(columnIndex, x);
    }
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEDOUBLE,
          () -> this.resultSet.updateDouble(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateDouble(columnLabel, x);
    }
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEFLOAT,
          () -> this.resultSet.updateFloat(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateFloat(columnIndex, x);
    }
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEFLOAT,
          () -> this.resultSet.updateFloat(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateFloat(columnLabel, x);
    }
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEINT,
          () -> this.resultSet.updateInt(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateInt(columnIndex, x);
    }
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEINT,
          () -> this.resultSet.updateInt(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateInt(columnLabel, x);
    }
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATELONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATELONG,
          () -> this.resultSet.updateLong(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateLong(columnIndex, x);
    }
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATELONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATELONG,
          () -> this.resultSet.updateLong(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateLong(columnLabel, x);
    }
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM,
          () -> this.resultSet.updateNCharacterStream(columnIndex, x, length),
          columnIndex,
          x,
          length);
    } else {
      this.resultSet.updateNCharacterStream(columnIndex, x, length);
    }
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM,
          () -> this.resultSet.updateNCharacterStream(columnLabel, reader, length),
          columnLabel,
          reader,
          length);
    } else {
      this.resultSet.updateNCharacterStream(columnLabel, reader, length);
    }
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM,
          () -> this.resultSet.updateNCharacterStream(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateNCharacterStream(columnIndex, x);
    }
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCHARACTERSTREAM,
          () -> this.resultSet.updateNCharacterStream(columnLabel, reader),
          columnLabel,
          reader);
    } else {
      this.resultSet.updateNCharacterStream(columnLabel, reader);
    }
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnIndex, nClob),
          columnIndex,
          nClob);
    } else {
      this.resultSet.updateNClob(columnIndex, nClob);
    }
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnLabel, nClob),
          columnLabel,
          nClob);
    } else {
      this.resultSet.updateNClob(columnLabel, nClob);
    }
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnIndex, reader, length),
          columnIndex,
          reader,
          length);
    } else {
      this.resultSet.updateNClob(columnIndex, reader, length);
    }
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnLabel, reader, length),
          columnLabel,
          reader,
          length);
    } else {
      this.resultSet.updateNClob(columnLabel, reader, length);
    }
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnIndex, reader),
          columnIndex,
          reader);
    } else {
      this.resultSet.updateNClob(columnIndex, reader);
    }
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENCLOB,
          () -> this.resultSet.updateNClob(columnLabel, reader),
          columnLabel,
          reader);
    } else {
      this.resultSet.updateNClob(columnLabel, reader);
    }
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENSTRING,
          () -> this.resultSet.updateNString(columnIndex, nString),
          columnIndex,
          nString);
    } else {
      this.resultSet.updateNString(columnIndex, nString);
    }
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENSTRING,
          () -> this.resultSet.updateNString(columnLabel, nString),
          columnLabel,
          nString);
    } else {
      this.resultSet.updateNString(columnLabel, nString);
    }
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENULL,
          () -> this.resultSet.updateNull(columnIndex),
          columnIndex);
    } else {
      this.resultSet.updateNull(columnIndex);
    }
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATENULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATENULL,
          () -> this.resultSet.updateNull(columnLabel),
          columnLabel);
    } else {
      this.resultSet.updateNull(columnLabel);
    }
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnIndex, x, scaleOrLength),
          columnIndex,
          x,
          scaleOrLength);
    } else {
      this.resultSet.updateObject(columnIndex, x, scaleOrLength);
    }
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateObject(columnIndex, x);
    }
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnLabel, x, scaleOrLength),
          columnLabel,
          x,
          scaleOrLength);
    } else {
      this.resultSet.updateObject(columnLabel, x, scaleOrLength);
    }
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateObject(columnLabel, x);
    }
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnIndex, x, targetSqlType, scaleOrLength),
          columnIndex,
          x,
          targetSqlType,
          scaleOrLength);
    } else {
      this.resultSet.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnLabel, x, targetSqlType, scaleOrLength),
          columnLabel,
          x,
          targetSqlType,
          scaleOrLength);
    } else {
      this.resultSet.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnIndex, x, targetSqlType),
          columnIndex,
          x,
          targetSqlType);
    } else {
      this.resultSet.updateObject(columnIndex, x, targetSqlType);
    }
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEOBJECT,
          () -> this.resultSet.updateObject(columnLabel, x, targetSqlType),
          columnLabel,
          x,
          targetSqlType);
    } else {
      this.resultSet.updateObject(columnLabel, x, targetSqlType);
    }
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEREF)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEREF,
          () -> this.resultSet.updateRef(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateRef(columnIndex, x);
    }
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEREF)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEREF,
          () -> this.resultSet.updateRef(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateRef(columnLabel, x);
    }
  }

  @Override
  public void updateRow() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEROW)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEROW,
          () -> this.resultSet.updateRow());
    } else {
      this.resultSet.updateRow();
    }
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEROWID,
          () -> this.resultSet.updateRowId(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateRowId(columnIndex, x);
    }
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATEROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATEROWID,
          () -> this.resultSet.updateRowId(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateRowId(columnLabel, x);
    }
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESQLXML,
          () -> this.resultSet.updateSQLXML(columnIndex, xmlObject),
          columnIndex,
          xmlObject);
    } else {
      this.resultSet.updateSQLXML(columnIndex, xmlObject);
    }
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESQLXML,
          () -> this.resultSet.updateSQLXML(columnLabel, xmlObject),
          columnLabel,
          xmlObject);
    } else {
      this.resultSet.updateSQLXML(columnLabel, xmlObject);
    }
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESHORT,
          () -> this.resultSet.updateShort(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateShort(columnIndex, x);
    }
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESHORT,
          () -> this.resultSet.updateShort(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateShort(columnLabel, x);
    }
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESTRING,
          () -> this.resultSet.updateString(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateString(columnIndex, x);
    }
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATESTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATESTRING,
          () -> this.resultSet.updateString(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateString(columnLabel, x);
    }
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATETIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATETIME,
          () -> this.resultSet.updateTime(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateTime(columnIndex, x);
    }
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATETIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATETIME,
          () -> this.resultSet.updateTime(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateTime(columnLabel, x);
    }
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATETIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATETIMESTAMP,
          () -> this.resultSet.updateTimestamp(columnIndex, x),
          columnIndex,
          x);
    } else {
      this.resultSet.updateTimestamp(columnIndex, x);
    }
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_UPDATETIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_UPDATETIMESTAMP,
          () -> this.resultSet.updateTimestamp(columnLabel, x),
          columnLabel,
          x);
    } else {
      this.resultSet.updateTimestamp(columnLabel, x);
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSET_WASNULL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSet,
          JdbcMethod.RESULTSET_WASNULL,
          () -> this.resultSet.wasNull());
    } else {
      return this.resultSet.wasNull();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.resultSet;
  }
}
