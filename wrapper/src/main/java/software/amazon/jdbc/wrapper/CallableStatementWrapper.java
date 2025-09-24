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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
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
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class CallableStatementWrapper implements CallableStatement {

  protected final CallableStatement statement;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public CallableStatementWrapper(
      @NonNull CallableStatement statement,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.statement = statement;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public void addBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_ADDBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_ADDBATCH,
          this.statement::addBatch);
    } else {
      this.statement.addBatch();
    }
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_ADDBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_ADDBATCH,
          () -> this.statement.addBatch(sql),
          sql);
    } else {
      this.statement.addBatch(sql);
    }
  }

  @Override
  public void cancel() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CANCEL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CANCEL,
          this.statement::cancel);
    } else {
      this.statement.cancel();
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CLEARBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CLEARBATCH,
          this.statement::clearBatch);
    } else {
      this.statement.clearBatch();
    }
  }

  @Override
  public void clearParameters() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CLEARPARAMETERS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CLEARPARAMETERS,
          this.statement::clearParameters);
    } else {
      this.statement.clearParameters();
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CLEARWARNINGS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CLEARWARNINGS,
          this.statement::clearWarnings);
    } else {
      this.statement.clearWarnings();
    }
  }

  @Override
  public void close() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CLOSE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CLOSE,
          this.statement::close);
    } else {
      this.statement.close();
    }
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_CLOSEONCOMPLETION)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_CLOSEONCOMPLETION,
          this.statement::closeOnCompletion);
    } else {
      this.statement.closeOnCompletion();
    }
  }

  @Override
  public boolean execute() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE,
          this.statement::execute);
    } else {
      return this.statement.execute();
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE,
          () -> this.statement.execute(sql),
          sql);
    } else {
      return this.statement.execute(sql);
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, autoGeneratedKeys),
          sql,
          autoGeneratedKeys);
    } else {
      return this.statement.execute(sql, autoGeneratedKeys);
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, columnIndexes),
          sql,
          columnIndexes);
    } else {
      return this.statement.execute(sql, columnIndexes);
    }
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, columnNames),
          sql,
          columnNames);
    } else {
      return this.statement.execute(sql, columnNames);
    }
  }

  @Override
  public int[] executeBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH)) {
      return WrapperUtils.executeWithPlugins(
          int[].class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH,
          this.statement::executeBatch);
    } else {
      return this.statement.executeBatch();
    }
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE,
          this.statement::executeLargeUpdate);
    } else {
      return this.statement.executeLargeUpdate();
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY,
        this.statement::executeQuery);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY,
        () -> this.statement.executeQuery(sql),
        sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE,
          this.statement::executeUpdate);
    } else {
      return this.statement.executeUpdate();
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql),
          sql);
    } else {
      return this.statement.executeUpdate(sql);
    }
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, autoGeneratedKeys),
          sql,
          autoGeneratedKeys);
    } else {
      return this.statement.executeUpdate(sql, autoGeneratedKeys);
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, columnIndexes),
          sql,
          columnIndexes);
    } else {
      return this.statement.executeUpdate(sql, columnIndexes);
    }
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, columnNames),
          sql,
          columnNames);
    } else {
      return this.statement.executeUpdate(sql, columnNames);
    }
  }

  @Override
  public Array getArray(int parameterIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETARRAY,
        () -> this.statement.getArray(parameterIndex),
        parameterIndex);
  }

  @Override
  public Array getArray(String parameterName) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETARRAY,
        () -> this.statement.getArray(parameterName),
        parameterName);
  }

  @Override
  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL,
          () -> this.statement.getBigDecimal(parameterIndex, scale),
          parameterIndex,
          scale);
    } else {
      return this.statement.getBigDecimal(parameterIndex, scale);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL,
          () -> this.statement.getBigDecimal(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getBigDecimal(parameterIndex);
    }
  }

  @Override
  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL,
          () -> this.statement.getBigDecimal(parameterName),
          parameterName);
    } else {
      return this.statement.getBigDecimal(parameterName);
    }
  }

  @Override
  public Blob getBlob(int parameterIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETBLOB,
        () -> this.statement.getBlob(parameterIndex),
        parameterIndex);
  }

  @Override
  public Blob getBlob(String parameterName) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETBLOB,
        () -> this.statement.getBlob(parameterName),
        parameterName);
  }

  @Override
  public boolean getBoolean(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN,
          () -> this.statement.getBoolean(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getBoolean(parameterIndex);
    }
  }

  @Override
  public boolean getBoolean(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN,
          () -> this.statement.getBoolean(parameterName),
          parameterName);
    } else {
      return this.statement.getBoolean(parameterName);
    }
  }

  @Override
  public byte getByte(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBYTE)) {
      return WrapperUtils.executeWithPlugins(
          byte.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBYTE,
          () -> this.statement.getByte(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getByte(parameterIndex);
    }
  }

  @Override
  public byte getByte(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBYTE)) {
      return WrapperUtils.executeWithPlugins(
          byte.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBYTE,
          () -> this.statement.getByte(parameterName),
          parameterName);
    } else {
      return this.statement.getByte(parameterName);
    }
  }

  @Override
  public byte[] getBytes(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBYTES,
          () -> this.statement.getBytes(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getBytes(parameterIndex);
    }
  }

  @Override
  public byte[] getBytes(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETBYTES,
          () -> this.statement.getBytes(parameterName),
          parameterName);
    } else {
      return this.statement.getBytes(parameterName);
    }
  }

  @Override
  public Reader getCharacterStream(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETCHARACTERSTREAM,
          () -> this.statement.getCharacterStream(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getCharacterStream(parameterIndex);
    }
  }

  @Override
  public Reader getCharacterStream(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETCHARACTERSTREAM,
          () -> this.statement.getCharacterStream(parameterName),
          parameterName);
    } else {
      return this.statement.getCharacterStream(parameterName);
    }
  }

  @Override
  public Clob getClob(int parameterIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETCLOB,
        () -> this.statement.getClob(parameterIndex),
        parameterIndex);
  }

  @Override
  public Clob getClob(String parameterName) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETCLOB,
        () -> this.statement.getClob(parameterName),
        parameterName);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Connection.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETCONNECTION,
        () -> this.connectionWrapper);
  }

  @Override
  public Date getDate(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDATE,
          () -> this.statement.getDate(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getDate(parameterIndex);
    }
  }

  @Override
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDATE,
          () -> this.statement.getDate(parameterIndex, cal),
          parameterIndex,
          cal);
    } else {
      return this.statement.getDate(parameterIndex, cal);
    }
  }

  @Override
  public Date getDate(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDATE,
          () -> this.statement.getDate(parameterName),
          parameterName);
    } else {
      return this.statement.getDate(parameterName);
    }
  }

  @Override
  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDATE,
          () -> this.statement.getDate(parameterName, cal),
          parameterName,
          cal);
    } else {
      return this.statement.getDate(parameterName, cal);
    }
  }

  @Override
  public double getDouble(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDOUBLE)) {
      return WrapperUtils.executeWithPlugins(
          double.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDOUBLE,
          () -> this.statement.getDouble(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getDouble(parameterIndex);
    }
  }

  @Override
  public double getDouble(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETDOUBLE)) {
      return WrapperUtils.executeWithPlugins(
          double.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETDOUBLE,
          () -> this.statement.getDouble(parameterName),
          parameterName);
    } else {
      return this.statement.getDouble(parameterName);
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getFetchDirection() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETFETCHDIRECTION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETFETCHDIRECTION,
          this.statement::getFetchDirection);
    } else {
      return this.statement.getFetchDirection();
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETFETCHSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETFETCHSIZE,
          this.statement::getFetchSize);
    } else {
      return this.statement.getFetchSize();
    }
  }

  @Override
  public float getFloat(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETFLOAT)) {
      return WrapperUtils.executeWithPlugins(
          float.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETFLOAT,
          () -> this.statement.getFloat(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getFloat(parameterIndex);
    }
  }

  @Override
  public float getFloat(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETFLOAT)) {
      return WrapperUtils.executeWithPlugins(
          float.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETFLOAT,
          () -> this.statement.getFloat(parameterName),
          parameterName);
    } else {
      return this.statement.getFloat(parameterName);
    }
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETGENERATEDKEYS,
        this.statement::getGeneratedKeys);
  }

  @Override
  public int getInt(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETINT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETINT,
          () -> this.statement.getInt(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getInt(parameterIndex);
    }
  }

  @Override
  public int getInt(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETINT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETINT,
          () -> this.statement.getInt(parameterName),
          parameterName);
    } else {
      return this.statement.getInt(parameterName);
    }
  }

  @Override
  public long getLong(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETLONG)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETLONG,
          () -> this.statement.getLong(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getLong(parameterIndex);
    }
  }

  @Override
  public long getLong(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETLONG)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETLONG,
          () -> this.statement.getLong(parameterName),
          parameterName);
    } else {
      return this.statement.getLong(parameterName);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETMAXFIELDSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETMAXFIELDSIZE,
          this.statement::getMaxFieldSize);
    } else {
      return this.statement.getMaxFieldSize();
    }
  }

  @Override
  public int getMaxRows() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETMAXROWS)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETMAXROWS,
          this.statement::getMaxRows);
    } else {
      return this.statement.getMaxRows();
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSetMetaData.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETMETADATA,
        this.statement::getMetaData);
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS,
          this.statement::getMoreResults);
    } else {
      return this.statement.getMoreResults();
    }
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETMORERESULTS,
          () -> this.statement.getMoreResults(current),
          current);
    } else {
      return this.statement.getMoreResults(current);
    }
  }

  @Override
  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETNCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETNCHARACTERSTREAM,
          () -> this.statement.getNCharacterStream(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getNCharacterStream(parameterIndex);
    }
  }

  @Override
  public Reader getNCharacterStream(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETNCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETNCHARACTERSTREAM,
          () -> this.statement.getNCharacterStream(parameterName),
          parameterName);
    } else {
      return this.statement.getNCharacterStream(parameterName);
    }
  }

  @Override
  public NClob getNClob(int parameterIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETNCLOB,
        () -> this.statement.getNClob(parameterIndex),
        parameterIndex);
  }

  @Override
  public NClob getNClob(String parameterName) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETNCLOB,
        () -> this.statement.getNClob(parameterName),
        parameterName);
  }

  @Override
  public String getNString(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETNSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETNSTRING,
          () -> this.statement.getNString(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getNString(parameterIndex);
    }
  }

  @Override
  public String getNString(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETNSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETNSTRING,
          () -> this.statement.getNString(parameterName),
          parameterName);
    } else {
      return this.statement.getNString(parameterName);
    }
  }

  @Override
  public Object getObject(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getObject(parameterIndex);
    }
  }

  @Override
  public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterIndex, map),
          parameterIndex,
          map);
    } else {
      return this.statement.getObject(parameterIndex, map);
    }
  }

  @Override
  public Object getObject(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterName),
          parameterName);
    } else {
      return this.statement.getObject(parameterName);
    }
  }

  @Override
  public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterName, map),
          parameterName,
          map);
    } else {
      return this.statement.getObject(parameterName, map);
    }
  }

  @Override
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          type,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterIndex, type),
          parameterIndex,
          type);
    } else {
      return this.statement.getObject(parameterIndex, type);
    }
  }

  @Override
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          type,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETOBJECT,
          () -> this.statement.getObject(parameterName, type),
          parameterName,
          type);
    } else {
      return this.statement.getObject(parameterName, type);
    }
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ParameterMetaData.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETPARAMETERMETADATA,
        this.statement::getParameterMetaData);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETQUERYTIMEOUT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETQUERYTIMEOUT,
          this.statement::getQueryTimeout);
    } else {
      return this.statement.getQueryTimeout();
    }
  }

  @Override
  public Ref getRef(int parameterIndex) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETREF,
        () -> this.statement.getRef(parameterIndex),
        parameterIndex);
  }

  @Override
  public Ref getRef(String parameterName) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETREF,
        () -> this.statement.getRef(parameterName),
        parameterName);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.statement,
        JdbcMethod.CALLABLESTATEMENT_GETRESULTSET,
        this.statement::getResultSet);
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getResultSetConcurrency() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETRESULTSETCONCURRENCY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETRESULTSETCONCURRENCY,
          this.statement::getResultSetConcurrency);
    } else {
      return this.statement.getResultSetConcurrency();
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETRESULTSETHOLDABILITY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETRESULTSETHOLDABILITY,
          this.statement::getResultSetHoldability);
    } else {
      return this.statement.getResultSetHoldability();
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getResultSetType() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETRESULTSETTYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETRESULTSETTYPE,
          this.statement::getResultSetType);
    } else {
      return this.statement.getResultSetType();
    }
  }

  @Override
  public RowId getRowId(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETROWID)) {
      return WrapperUtils.executeWithPlugins(
          RowId.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETROWID,
          () -> this.statement.getRowId(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getRowId(parameterIndex);
    }
  }

  @Override
  public RowId getRowId(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETROWID)) {
      return WrapperUtils.executeWithPlugins(
          RowId.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETROWID,
          () -> this.statement.getRowId(parameterName),
          parameterName);
    } else {
      return this.statement.getRowId(parameterName);
    }
  }

  @Override
  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSQLXML)) {
      return WrapperUtils.executeWithPlugins(
          SQLXML.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSQLXML,
          () -> this.statement.getSQLXML(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getSQLXML(parameterIndex);
    }
  }

  @Override
  public SQLXML getSQLXML(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSQLXML)) {
      return WrapperUtils.executeWithPlugins(
          SQLXML.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSQLXML,
          () -> this.statement.getSQLXML(parameterName),
          parameterName);
    } else {
      return this.statement.getSQLXML(parameterName);
    }
  }

  @Override
  public short getShort(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSHORT)) {
      return WrapperUtils.executeWithPlugins(
          short.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSHORT,
          () -> this.statement.getShort(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getShort(parameterIndex);
    }
  }

  @Override
  public short getShort(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSHORT)) {
      return WrapperUtils.executeWithPlugins(
          short.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSHORT,
          () -> this.statement.getShort(parameterName),
          parameterName);
    } else {
      return this.statement.getShort(parameterName);
    }
  }

  @Override
  public String getString(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSTRING,
          () -> this.statement.getString(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getString(parameterIndex);
    }
  }

  @Override
  public String getString(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETSTRING,
          () -> this.statement.getString(parameterName),
          parameterName);
    } else {
      return this.statement.getString(parameterName);
    }
  }

  @Override
  public Time getTime(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIME,
          () -> this.statement.getTime(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getTime(parameterIndex);
    }
  }

  @Override
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIME,
          () -> this.statement.getTime(parameterIndex, cal),
          parameterIndex,
          cal);
    } else {
      return this.statement.getTime(parameterIndex, cal);
    }
  }

  @Override
  public Time getTime(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIME,
          () -> this.statement.getTime(parameterName),
          parameterName);
    } else {
      return this.statement.getTime(parameterName);
    }
  }

  @Override
  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIME,
          () -> this.statement.getTime(parameterName, cal),
          parameterName,
          cal);
    } else {
      return this.statement.getTime(parameterName, cal);
    }
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP,
          () -> this.statement.getTimestamp(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getTimestamp(parameterIndex);
    }
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP,
          () -> this.statement.getTimestamp(parameterIndex, cal),
          parameterIndex,
          cal);
    } else {
      return this.statement.getTimestamp(parameterIndex, cal);
    }
  }

  @Override
  public Timestamp getTimestamp(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP,
          () -> this.statement.getTimestamp(parameterName),
          parameterName);
    } else {
      return this.statement.getTimestamp(parameterName);
    }
  }

  @Override
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP,
          () -> this.statement.getTimestamp(parameterName, cal),
          parameterName,
          cal);
    } else {
      return this.statement.getTimestamp(parameterName, cal);
    }
  }

  @Override
  public URL getURL(int parameterIndex) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETURL)) {
      return WrapperUtils.executeWithPlugins(
          URL.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETURL,
          () -> this.statement.getURL(parameterIndex),
          parameterIndex);
    } else {
      return this.statement.getURL(parameterIndex);
    }
  }

  @Override
  public URL getURL(String parameterName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETURL)) {
      return WrapperUtils.executeWithPlugins(
          URL.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETURL,
          () -> this.statement.getURL(parameterName),
          parameterName);
    } else {
      return this.statement.getURL(parameterName);
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETUPDATECOUNT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETUPDATECOUNT,
          this.statement::getUpdateCount);
    } else {
      return this.statement.getUpdateCount();
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_GETWARNINGS)) {
      return WrapperUtils.executeWithPlugins(
          SQLWarning.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_GETWARNINGS,
          this.statement::getWarnings);
    } else {
      return this.statement.getWarnings();
    }
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_ISCLOSEONCOMPLETION)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_ISCLOSEONCOMPLETION,
          this.statement::isCloseOnCompletion);
    } else {
      return this.statement.isCloseOnCompletion();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_ISCLOSED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_ISCLOSED,
          this.statement::isClosed);
    } else {
      return this.statement.isClosed();
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_ISPOOLABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_ISPOOLABLE,
          this.statement::isPoolable);
    } else {
      return this.statement.isPoolable();
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.statement.isWrapperFor(iface);
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType),
          parameterIndex,
          sqlType);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType);
    }
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType, scale),
          parameterIndex,
          sqlType,
          scale);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType, scale);
    }
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType, typeName),
          parameterIndex,
          sqlType,
          typeName);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType, typeName);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType),
          parameterName,
          sqlType);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType, scale),
          parameterName,
          sqlType,
          scale);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType, scale);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, String typeName)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType, typeName),
          parameterName,
          sqlType,
          typeName);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType, typeName);
    }
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType),
          parameterIndex,
          sqlType);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType);
    }
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType, scale),
          parameterIndex,
          sqlType,
          scale);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType, scale);
    }
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterIndex, sqlType, typeName),
          parameterIndex,
          sqlType,
          typeName);
    } else {
      this.statement.registerOutParameter(parameterIndex, sqlType, typeName);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType),
          parameterName,
          sqlType);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, int scale)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType, scale),
          parameterName,
          sqlType,
          scale);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType, scale);
    }
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, String typeName)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_REGISTEROUTPARAMETER,
          () -> this.statement.registerOutParameter(parameterName, sqlType, typeName),
          parameterName,
          sqlType,
          typeName);
    } else {
      this.statement.registerOutParameter(parameterName, sqlType, typeName);
    }
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETARRAY)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETARRAY,
          () -> this.statement.setArray(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setArray(parameterIndex, x);
    }
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterName, x, length),
          parameterName,
          x,
          length);
    } else {
      this.statement.setAsciiStream(parameterName, x, length);
    }
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterName, x, length),
          parameterName,
          x,
          length);
    } else {
      this.statement.setAsciiStream(parameterName, x, length);
    }
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setAsciiStream(parameterName, x);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterIndex, x, length),
          parameterIndex,
          x,
          length);
    } else {
      this.statement.setAsciiStream(parameterIndex, x, length);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterIndex, x, length),
          parameterIndex,
          x,
          length);
    } else {
      this.statement.setAsciiStream(parameterIndex, x, length);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setAsciiStream(parameterIndex, x);
    }
  }

  @Override
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBIGDECIMAL,
          () -> this.statement.setBigDecimal(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setBigDecimal(parameterName, x);
    }
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBIGDECIMAL,
          () -> this.statement.setBigDecimal(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBigDecimal(parameterIndex, x);
    }
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterName, x, length),
          parameterName,
          x,
          length);
    } else {
      this.statement.setBinaryStream(parameterName, x, length);
    }
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterName, x, length),
          parameterName,
          x,
          length);
    } else {
      this.statement.setBinaryStream(parameterName, x, length);
    }
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setBinaryStream(parameterName, x);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterIndex, x, length),
          parameterIndex,
          x,
          length);
    } else {
      this.statement.setBinaryStream(parameterIndex, x, length);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterIndex, x, length),
          parameterIndex,
          x,
          length);
    } else {
      this.statement.setBinaryStream(parameterIndex, x, length);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBinaryStream(parameterIndex, x);
    }
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterName, inputStream, length),
          parameterName,
          inputStream,
          length);
    } else {
      this.statement.setBlob(parameterName, inputStream, length);
    }
  }

  @Override
  public void setBlob(String parameterName, Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setBlob(parameterName, x);
    }
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterName, inputStream),
          parameterName,
          inputStream);
    } else {
      this.statement.setBlob(parameterName, inputStream);
    }
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBlob(parameterIndex, x);
    }
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterIndex, inputStream, length),
          parameterIndex,
          inputStream,
          length);
    } else {
      this.statement.setBlob(parameterIndex, inputStream, length);
    }
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterIndex, inputStream),
          parameterIndex,
          inputStream);
    } else {
      this.statement.setBlob(parameterIndex, inputStream);
    }
  }

  @Override
  public void setBoolean(String parameterName, boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBOOLEAN,
          () -> this.statement.setBoolean(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setBoolean(parameterName, x);
    }
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBOOLEAN,
          () -> this.statement.setBoolean(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBoolean(parameterIndex, x);
    }
  }

  @Override
  public void setByte(String parameterName, byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBYTE,
          () -> this.statement.setByte(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setByte(parameterName, x);
    }
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBYTE,
          () -> this.statement.setByte(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setByte(parameterIndex, x);
    }
  }

  @Override
  public void setBytes(String parameterName, byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBYTES,
          () -> this.statement.setBytes(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setBytes(parameterName, x);
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETBYTES,
          () -> this.statement.setBytes(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBytes(parameterIndex, x);
    }
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterName, reader, length),
          parameterName,
          reader,
          length);
    } else {
      this.statement.setCharacterStream(parameterName, reader, length);
    }
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterName, reader, length),
          parameterName,
          reader,
          length);
    } else {
      this.statement.setCharacterStream(parameterName, reader, length);
    }
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterName, reader),
          parameterName,
          reader);
    } else {
      this.statement.setCharacterStream(parameterName, reader);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterIndex, reader, length),
          parameterIndex,
          reader,
          length);
    } else {
      this.statement.setCharacterStream(parameterIndex, reader, length);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterIndex, reader, length),
          parameterIndex,
          reader,
          length);
    } else {
      this.statement.setCharacterStream(parameterIndex, reader, length);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setCharacterStream(parameterIndex, reader);
    }
  }

  @Override
  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterName, reader, length),
          parameterName,
          reader,
          length);
    } else {
      this.statement.setClob(parameterName, reader, length);
    }
  }

  @Override
  public void setClob(String parameterName, Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setClob(parameterName, x);
    }
  }

  @Override
  public void setClob(String parameterName, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterName, reader),
          parameterName,
          reader);
    } else {
      this.statement.setClob(parameterName, reader);
    }
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setClob(parameterIndex, x);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterIndex, reader, length),
          parameterIndex,
          reader,
          length);
    } else {
      this.statement.setClob(parameterIndex, reader, length);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setClob(parameterIndex, reader);
    }
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETCURSORNAME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETCURSORNAME,
          () -> this.statement.setCursorName(name),
          name);
    } else {
      this.statement.setCursorName(name);
    }
  }

  @Override
  public void setDate(String parameterName, Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setDate(parameterName, x);
    }
  }

  @Override
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterName, x, cal),
          parameterName,
          x,
          cal);
    } else {
      this.statement.setDate(parameterName, x, cal);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setDate(parameterIndex, x);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setDate(parameterIndex, x, cal);
    }
  }

  @Override
  public void setDouble(String parameterName, double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDOUBLE,
          () -> this.statement.setDouble(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setDouble(parameterName, x);
    }
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETDOUBLE,
          () -> this.statement.setDouble(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setDouble(parameterIndex, x);
    }
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETESCAPEPROCESSING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETESCAPEPROCESSING,
          () -> this.statement.setEscapeProcessing(enable),
          enable);
    } else {
      this.statement.setEscapeProcessing(enable);
    }
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETFETCHDIRECTION)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETFETCHDIRECTION,
          () -> this.statement.setFetchDirection(direction),
          direction);
    } else {
      this.statement.setFetchDirection(direction);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETFETCHSIZE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETFETCHSIZE,
          () -> this.statement.setFetchSize(rows),
          rows);
    } else {
      this.statement.setFetchSize(rows);
    }
  }

  @Override
  public void setFloat(String parameterName, float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETFLOAT,
          () -> this.statement.setFloat(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setFloat(parameterName, x);
    }
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETFLOAT,
          () -> this.statement.setFloat(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setFloat(parameterIndex, x);
    }
  }

  @Override
  public void setInt(String parameterName, int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETINT,
          () -> this.statement.setInt(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setInt(parameterName, x);
    }
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETINT,
          () -> this.statement.setInt(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setInt(parameterIndex, x);
    }
  }

  @Override
  public void setLong(String parameterName, long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETLONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETLONG,
          () -> this.statement.setLong(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setLong(parameterName, x);
    }
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETLONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETLONG,
          () -> this.statement.setLong(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setLong(parameterIndex, x);
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETMAXFIELDSIZE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETMAXFIELDSIZE,
          () -> this.statement.setMaxFieldSize(max),
          max);
    } else {
      this.statement.setMaxFieldSize(max);
    }
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETMAXROWS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETMAXROWS,
          () -> this.statement.setMaxRows(max),
          max);
    } else {
      this.statement.setMaxRows(max);
    }
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM,
          () -> this.statement.setNCharacterStream(parameterName, value, length),
          parameterName,
          value,
          length);
    } else {
      this.statement.setNCharacterStream(parameterName, value, length);
    }
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM,
          () -> this.statement.setNCharacterStream(parameterName, value),
          parameterName,
          value);
    } else {
      this.statement.setNCharacterStream(parameterName, value);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM,
          () -> this.statement.setNCharacterStream(parameterIndex, value, length),
          parameterIndex,
          value,
          length);
    } else {
      this.statement.setNCharacterStream(parameterIndex, value, length);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCHARACTERSTREAM,
          () -> this.statement.setNCharacterStream(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNCharacterStream(parameterIndex, value);
    }
  }

  @Override
  public void setNClob(String parameterName, NClob value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterName, value),
          parameterName,
          value);
    } else {
      this.statement.setNClob(parameterName, value);
    }
  }

  @Override
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterName, reader, length),
          parameterName,
          reader,
          length);
    } else {
      this.statement.setNClob(parameterName, reader, length);
    }
  }

  @Override
  public void setNClob(String parameterName, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterName, reader),
          parameterName,
          reader);
    } else {
      this.statement.setNClob(parameterName, reader);
    }
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNClob(parameterIndex, value);
    }
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterIndex, reader, length),
          parameterIndex,
          reader,
          length);
    } else {
      this.statement.setNClob(parameterIndex, reader, length);
    }
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setNClob(parameterIndex, reader);
    }
  }

  @Override
  public void setNString(String parameterName, String value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNSTRING,
          () -> this.statement.setNString(parameterName, value),
          parameterName,
          value);
    } else {
      this.statement.setNString(parameterName, value);
    }
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNSTRING,
          () -> this.statement.setNString(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNString(parameterIndex, value);
    }
  }

  @Override
  public void setNull(String parameterName, int sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterName, sqlType),
          parameterName,
          sqlType);
    } else {
      this.statement.setNull(parameterName, sqlType);
    }
  }

  @Override
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterName, sqlType, typeName),
          parameterName,
          sqlType,
          typeName);
    } else {
      this.statement.setNull(parameterName, sqlType, typeName);
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterIndex, sqlType),
          parameterIndex,
          sqlType);
    } else {
      this.statement.setNull(parameterIndex, sqlType);
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterIndex, sqlType, typeName),
          parameterIndex,
          sqlType,
          typeName);
    } else {
      this.statement.setNull(parameterIndex, sqlType, typeName);
    }
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType, int scale)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterName, x, targetSqlType, scale),
          parameterName,
          x,
          targetSqlType,
          scale);
    } else {
      this.statement.setObject(parameterName, x, targetSqlType, scale);
    }
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterName, x, targetSqlType),
          parameterName,
          x,
          targetSqlType);
    } else {
      this.statement.setObject(parameterName, x, targetSqlType);
    }
  }

  @Override
  public void setObject(String parameterName, Object x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setObject(parameterName, x);
    }
  }

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterName, x, targetSqlType, scaleOrLength),
          parameterName,
          x,
          targetSqlType,
          scaleOrLength);
    } else {
      this.statement.setObject(parameterName, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterName, x, targetSqlType),
          parameterName,
          x,
          targetSqlType);
    } else {
      this.statement.setObject(parameterName, x, targetSqlType);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterIndex, x, targetSqlType),
          parameterIndex,
          x,
          targetSqlType);
    } else {
      this.statement.setObject(parameterIndex, x, targetSqlType);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setObject(parameterIndex, x);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength),
          parameterIndex,
          x,
          targetSqlType,
          scaleOrLength);
    } else {
      this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength),
          parameterIndex,
          x,
          targetSqlType,
          scaleOrLength);
    } else {
      this.statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETOBJECT,
          () -> this.statement.setObject(parameterIndex, x, targetSqlType),
          parameterIndex,
          x,
          targetSqlType);
    } else {
      this.statement.setObject(parameterIndex, x, targetSqlType);
    }
  }

  @SuppressWarnings("SpellCheckingInspection")
  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETPOOLABLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETPOOLABLE,
          () -> this.statement.setPoolable(poolable),
          poolable);
    } else {
      this.statement.setPoolable(poolable);
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETQUERYTIMEOUT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETQUERYTIMEOUT,
          () -> this.statement.setQueryTimeout(seconds),
          seconds);
    } else {
      this.statement.setQueryTimeout(seconds);
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETREF)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETREF,
          () -> this.statement.setRef(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setRef(parameterIndex, x);
    }
  }

  @Override
  public void setRowId(String parameterName, RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETROWID,
          () -> this.statement.setRowId(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setRowId(parameterName, x);
    }
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETROWID,
          () -> this.statement.setRowId(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setRowId(parameterIndex, x);
    }
  }

  @Override
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSQLXML,
          () -> this.statement.setSQLXML(parameterName, xmlObject),
          parameterName,
          xmlObject);
    } else {
      this.statement.setSQLXML(parameterName, xmlObject);
    }
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSQLXML,
          () -> this.statement.setSQLXML(parameterIndex, xmlObject),
          parameterIndex,
          xmlObject);
    } else {
      this.statement.setSQLXML(parameterIndex, xmlObject);
    }
  }

  @Override
  public void setShort(String parameterName, short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSHORT,
          () -> this.statement.setShort(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setShort(parameterName, x);
    }
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSHORT,
          () -> this.statement.setShort(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setShort(parameterIndex, x);
    }
  }

  @Override
  public void setString(String parameterName, String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSTRING,
          () -> this.statement.setString(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setString(parameterName, x);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETSTRING,
          () -> this.statement.setString(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setString(parameterIndex, x);
    }
  }

  @Override
  public void setTime(String parameterName, Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setTime(parameterName, x);
    }
  }

  @Override
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterName, x, cal),
          parameterName,
          x,
          cal);
    } else {
      this.statement.setTime(parameterName, x, cal);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setTime(parameterIndex, x);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setTime(parameterIndex, x, cal);
    }
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterName, x),
          parameterName,
          x);
    } else {
      this.statement.setTimestamp(parameterName, x);
    }
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterName, x, cal),
          parameterName,
          x,
          cal);
    } else {
      this.statement.setTimestamp(parameterName, x, cal);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setTimestamp(parameterIndex, x);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setTimestamp(parameterIndex, x, cal);
    }
  }

  @Override
  public void setURL(String parameterName, URL val) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETURL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETURL,
          () -> this.statement.setURL(parameterName, val),
          parameterName,
          val);
    } else {
      this.statement.setURL(parameterName, val);
    }
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETURL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETURL,
          () -> this.statement.setURL(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setURL(parameterIndex, x);
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_SETUNICODESTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_SETUNICODESTREAM,
          () -> this.statement.setUnicodeStream(parameterIndex, x, length),
          parameterIndex,
          x,
          length);
    } else {
      this.statement.setUnicodeStream(parameterIndex, x, length);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.statement.unwrap(iface);
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CALLABLESTATEMENT_WASNULL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.statement,
          JdbcMethod.CALLABLESTATEMENT_WASNULL,
          this.statement::wasNull);
    } else {
      return this.statement.wasNull();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.statement;
  }
}
