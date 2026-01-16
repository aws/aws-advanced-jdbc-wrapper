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
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class PreparedStatementWrapper implements PreparedStatement {

  protected final PreparedStatement statement;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public PreparedStatementWrapper(
      @NonNull PreparedStatement statement,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.statement = statement;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public void addBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_ADDBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_ADDBATCH,
          this.statement::addBatch);
    } else {
      this.statement.addBatch();
    }
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_ADDBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_ADDBATCH,
          () -> this.statement.addBatch(sql),
          sql);
    } else {
      this.statement.addBatch(sql);
    }
  }

  @Override
  public void cancel() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CANCEL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CANCEL,
          this.statement::cancel);
    } else {
      this.statement.cancel();
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CLEARBATCH)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CLEARBATCH,
          this.statement::clearBatch);
    } else {
      this.statement.clearBatch();
    }
  }

  @Override
  public void clearParameters() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CLEARPARAMETERS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CLEARPARAMETERS,
          this.statement::clearParameters);
    } else {
      this.statement.clearParameters();
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CLEARWARNINGS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CLEARWARNINGS,
          this.statement::clearWarnings);
    } else {
      this.statement.clearWarnings();
    }
  }

  @Override
  public void close() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CLOSE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CLOSE,
          this.statement::close);
    } else {
      this.statement.close();
    }
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_CLOSEONCOMPLETION)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_CLOSEONCOMPLETION,
          this.statement::closeOnCompletion);
    } else {
      this.statement.closeOnCompletion();
    }
  }

  @Override
  public boolean execute() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE,
          this.statement::execute);
    } else {
      return this.statement.execute();
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE,
          () -> this.statement.execute(sql),
          sql);
    } else {
      return this.statement.execute(sql);
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, autoGeneratedKeys),
          sql,
          autoGeneratedKeys);
    } else {
      return this.statement.execute(sql, autoGeneratedKeys);
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, columnIndexes),
          sql,
          columnIndexes);
    } else {
      return this.statement.execute(sql, columnIndexes);
    }
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE,
          () -> this.statement.execute(sql, columnNames),
          sql,
          columnNames);
    } else {
      return this.statement.execute(sql, columnNames);
    }
  }

  @Override
  public int[] executeBatch() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH)) {
      return WrapperUtils.executeWithPlugins(
          int[].class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH,
          this.statement::executeBatch);
    } else {
      return this.statement.executeBatch();
    }
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE,
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
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY,
        this.statement::executeQuery);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY,
        () -> this.statement.executeQuery(sql),
        sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE,
          this.statement::executeUpdate);
    } else {
      return this.statement.executeUpdate();
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql),
          sql);
    } else {
      return this.statement.executeUpdate(sql);
    }
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, autoGeneratedKeys),
          sql,
          autoGeneratedKeys);
    } else {
      return this.statement.executeUpdate(sql, autoGeneratedKeys);
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, columnIndexes),
          sql,
          columnIndexes);
    } else {
      return this.statement.executeUpdate(sql, columnIndexes);
    }
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE,
          () -> this.statement.executeUpdate(sql, columnNames),
          sql,
          columnNames);
    } else {
      return this.statement.executeUpdate(sql, columnNames);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Connection.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_GETCONNECTION,
        () -> this.connectionWrapper);
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getFetchDirection() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETFETCHDIRECTION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETFETCHDIRECTION,
          this.statement::getFetchDirection);
    } else {
      return this.statement.getFetchDirection();
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETFETCHSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETFETCHSIZE,
          this.statement::getFetchSize);
    } else {
      return this.statement.getFetchSize();
    }
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_GETGENERATEDKEYS,
        this.statement::getGeneratedKeys);
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETMAXFIELDSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETMAXFIELDSIZE,
          this.statement::getMaxFieldSize);
    } else {
      return this.statement.getMaxFieldSize();
    }
  }

  @Override
  public int getMaxRows() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETMAXROWS)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETMAXROWS,
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
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_GETMETADATA,
        this.statement::getMetaData);
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETMORERESULTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETMORERESULTS,
          this.statement::getMoreResults);
    } else {
      return this.statement.getMoreResults();
    }
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETMORERESULTS)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETMORERESULTS,
          () -> this.statement.getMoreResults(current),
          current);
    } else {
      return this.statement.getMoreResults(current);
    }
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ParameterMetaData.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_GETPARAMETERMETADATA,
        this.statement::getParameterMetaData);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETQUERYTIMEOUT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETQUERYTIMEOUT,
          this.statement::getQueryTimeout);
    } else {
      return this.statement.getQueryTimeout();
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.statement,
        JdbcMethod.PREPAREDSTATEMENT_GETRESULTSET,
        this.statement::getResultSet);
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getResultSetConcurrency() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETCONCURRENCY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETCONCURRENCY,
          this.statement::getResultSetConcurrency);
    } else {
      return this.statement.getResultSetConcurrency();
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETHOLDABILITY)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETHOLDABILITY,
          this.statement::getResultSetHoldability);
    } else {
      return this.statement.getResultSetHoldability();
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getResultSetType() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETTYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETRESULTSETTYPE,
          this.statement::getResultSetType);
    } else {
      return this.statement.getResultSetType();
    }
  }

  @Override
  public int getUpdateCount() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETUPDATECOUNT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETUPDATECOUNT,
          this.statement::getUpdateCount);
    } else {
      return this.statement.getUpdateCount();
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_GETWARNINGS)) {
      return WrapperUtils.executeWithPlugins(
          SQLWarning.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_GETWARNINGS,
          this.statement::getWarnings);
    } else {
      return this.statement.getWarnings();
    }
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_ISCLOSEONCOMPLETION)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_ISCLOSEONCOMPLETION,
          this.statement::isCloseOnCompletion);
    } else {
      return this.statement.isCloseOnCompletion();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_ISCLOSED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_ISCLOSED,
          this.statement::isClosed);
    } else {
      return this.statement.isClosed();
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_ISPOOLABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_ISPOOLABLE,
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
  public void setArray(int parameterIndex, Array x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETARRAY)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETARRAY,
          () -> this.statement.setArray(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setArray(parameterIndex, x);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETASCIISTREAM,
          () -> this.statement.setAsciiStream(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setAsciiStream(parameterIndex, x);
    }
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBIGDECIMAL,
          () -> this.statement.setBigDecimal(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBigDecimal(parameterIndex, x);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBINARYSTREAM,
          () -> this.statement.setBinaryStream(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBinaryStream(parameterIndex, x);
    }
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBLOB,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBLOB,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBLOB,
          () -> this.statement.setBlob(parameterIndex, inputStream),
          parameterIndex,
          inputStream);
    } else {
      this.statement.setBlob(parameterIndex, inputStream);
    }
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBOOLEAN,
          () -> this.statement.setBoolean(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBoolean(parameterIndex, x);
    }
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBYTE,
          () -> this.statement.setByte(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setByte(parameterIndex, x);
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETBYTES,
          () -> this.statement.setBytes(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setBytes(parameterIndex, x);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCHARACTERSTREAM,
          () -> this.statement.setCharacterStream(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setCharacterStream(parameterIndex, reader);
    }
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setClob(parameterIndex, x);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCLOB,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCLOB,
          () -> this.statement.setClob(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setClob(parameterIndex, reader);
    }
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETCURSORNAME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETCURSORNAME,
          () -> this.statement.setCursorName(name),
          name);
    } else {
      this.statement.setCursorName(name);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setDate(parameterIndex, x);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETDATE,
          () -> this.statement.setDate(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setDate(parameterIndex, x, cal);
    }
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETDOUBLE,
          () -> this.statement.setDouble(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setDouble(parameterIndex, x);
    }
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETESCAPEPROCESSING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETESCAPEPROCESSING,
          () -> this.statement.setEscapeProcessing(enable),
          enable);
    } else {
      this.statement.setEscapeProcessing(enable);
    }
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETFETCHDIRECTION)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETFETCHDIRECTION,
          () -> this.statement.setFetchDirection(direction),
          direction);
    } else {
      this.statement.setFetchDirection(direction);
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETFETCHSIZE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETFETCHSIZE,
          () -> this.statement.setFetchSize(rows),
          rows);
    } else {
      this.statement.setFetchSize(rows);
    }
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETFLOAT,
          () -> this.statement.setFloat(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setFloat(parameterIndex, x);
    }
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETINT,
          () -> this.statement.setInt(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setInt(parameterIndex, x);
    }
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETLONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETLONG,
          () -> this.statement.setLong(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setLong(parameterIndex, x);
    }
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETMAXFIELDSIZE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETMAXFIELDSIZE,
          () -> this.statement.setMaxFieldSize(max),
          max);
    } else {
      this.statement.setMaxFieldSize(max);
    }
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETMAXROWS)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETMAXROWS,
          () -> this.statement.setMaxRows(max),
          max);
    } else {
      this.statement.setMaxRows(max);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNCHARACTERSTREAM,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNCHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNCHARACTERSTREAM,
          () -> this.statement.setNCharacterStream(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNCharacterStream(parameterIndex, value);
    }
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNClob(parameterIndex, value);
    }
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNCLOB,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNCLOB,
          () -> this.statement.setNClob(parameterIndex, reader),
          parameterIndex,
          reader);
    } else {
      this.statement.setNClob(parameterIndex, reader);
    }
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNSTRING,
          () -> this.statement.setNString(parameterIndex, value),
          parameterIndex,
          value);
    } else {
      this.statement.setNString(parameterIndex, value);
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterIndex, sqlType),
          parameterIndex,
          sqlType);
    } else {
      this.statement.setNull(parameterIndex, sqlType);
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETNULL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETNULL,
          () -> this.statement.setNull(parameterIndex, sqlType, typeName),
          parameterIndex,
          sqlType,
          typeName);
    } else {
      this.statement.setNull(parameterIndex, sqlType, typeName);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETOBJECT,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETOBJECT,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETOBJECT,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETOBJECT,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETOBJECT,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETPOOLABLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETPOOLABLE,
          () -> this.statement.setPoolable(poolable),
          poolable);
    } else {
      this.statement.setPoolable(poolable);
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETQUERYTIMEOUT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETQUERYTIMEOUT,
          () -> this.statement.setQueryTimeout(seconds),
          seconds);
    } else {
      this.statement.setQueryTimeout(seconds);
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETREF)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETREF,
          () -> this.statement.setRef(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setRef(parameterIndex, x);
    }
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETROWID,
          () -> this.statement.setRowId(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setRowId(parameterIndex, x);
    }
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETSQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETSQLXML,
          () -> this.statement.setSQLXML(parameterIndex, xmlObject),
          parameterIndex,
          xmlObject);
    } else {
      this.statement.setSQLXML(parameterIndex, xmlObject);
    }
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETSHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETSHORT,
          () -> this.statement.setShort(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setShort(parameterIndex, x);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETSTRING,
          () -> this.statement.setString(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setString(parameterIndex, x);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setTime(parameterIndex, x);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETTIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETTIME,
          () -> this.statement.setTime(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setTime(parameterIndex, x, cal);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterIndex, x),
          parameterIndex,
          x);
    } else {
      this.statement.setTimestamp(parameterIndex, x);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETTIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETTIMESTAMP,
          () -> this.statement.setTimestamp(parameterIndex, x, cal),
          parameterIndex,
          x,
          cal);
    } else {
      this.statement.setTimestamp(parameterIndex, x, cal);
    }
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETURL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETURL,
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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PREPAREDSTATEMENT_SETUNICODESTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.statement,
          JdbcMethod.PREPAREDSTATEMENT_SETUNICODESTREAM,
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
  public String toString() {
    return super.toString() + " - " + this.statement;
  }
}
