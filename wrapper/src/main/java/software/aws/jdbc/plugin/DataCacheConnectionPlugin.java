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

package software.aws.jdbc.plugin;

import software.aws.jdbc.AwsWrapperProperty;
import software.aws.jdbc.JdbcCallable;
import software.aws.jdbc.util.StringUtils;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataCacheConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(DataCacheConnectionPlugin.class.getName());

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("Statement.executeQuery", "Statement.execute",
          "PreparedStatement.execute", "PreparedStatement.executeQuery",
          "CallableStatement.execute", "CallableStatement.executeQuery")));

  public static final AwsWrapperProperty DATA_CACHE_TRIGGER_CONDITION = new AwsWrapperProperty(
      "dataCacheTriggerCondition", "false",
      "A regular expression that, if it's matched, allows the plugin to cache SQL results.");

  protected static final Map<String, ResultSet> dataCache = new ConcurrentHashMap<>();

  protected final String dataCacheTriggerCondition;

  public DataCacheConnectionPlugin(Properties props) {
    this.dataCacheTriggerCondition = DATA_CACHE_TRIGGER_CONDITION.getString(props);
  }

  public static void clearCache() {
    dataCache.clear();
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(Class<T> resultClass, Class<E> exceptionClass, Object methodInvokeOn,
      String methodName, JdbcCallable<T, E> jdbcMethodFunc, Object[] jdbcMethodArgs) throws E {

    if (StringUtils.isNullOrEmpty(this.dataCacheTriggerCondition) || resultClass != ResultSet.class) {
      return jdbcMethodFunc.call();
    }

    ResultSet result;
    boolean needToCache = false;
    String sql = getQuery(jdbcMethodArgs);

    if (!StringUtils.isNullOrEmpty(sql) && sql.matches(this.dataCacheTriggerCondition)) {
      result = dataCache.get(sql);
      if (result == null) {
        needToCache = true;
        LOGGER.log(Level.FINEST, "[{0}] Query results will be cached: {1}", new Object[]{methodName, sql});
      } else {
        try {
          result.beforeFirst();
        } catch (SQLException ex) {
          if (exceptionClass.isAssignableFrom(ex.getClass())) {
            throw exceptionClass.cast(ex);
          }
          throw new RuntimeException(ex);
        }
        return resultClass.cast(result);
      }
    }

    result = (ResultSet) jdbcMethodFunc.call();

    if (needToCache) {
      ResultSet cachedResultSet;
      try {
        cachedResultSet = new CachedResultSet(result);
        dataCache.put(sql, cachedResultSet);
        cachedResultSet.beforeFirst();
        return resultClass.cast(cachedResultSet);
      } catch (SQLException ex) {
        // ignore exception
      }
    }

    return resultClass.cast(result);
  }

  protected String getQuery(Object[] jdbcMethodArgs) {

    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString();
    }
    return null;
  }

  public static class CachedRow {
    protected final HashMap<Integer, Object> columnByIndex = new HashMap<>();
    protected final HashMap<String, Object> columnByName = new HashMap<>();

    public void put(int columnIndex, String columnName, Object columnValue) {
      columnByIndex.put(columnIndex, columnValue);
      columnByName.put(columnName, columnValue);
    }

    @SuppressWarnings("unused")
    public Object get(int columnIndex) {
      return columnByIndex.get(columnIndex);
    }

    @SuppressWarnings("unused")
    public Object get(String columnName) {
      return columnByName.get(columnName);
    }
  }

  @SuppressWarnings({"RedundantThrows", "checkstyle:OverloadMethodsDeclarationOrder"})
  public static class CachedResultSet implements ResultSet {

    protected ArrayList<CachedRow> rows;
    protected int currentRow;

    public CachedResultSet(ResultSet resultSet) throws SQLException {

      ResultSetMetaData md = resultSet.getMetaData();
      int columns = md.getColumnCount();
      rows = new ArrayList<>();

      while (resultSet.next()) {
        CachedRow row = new CachedRow();
        for (int i = 1; i <= columns; ++i) {
          row.put(i, md.getColumnName(i), resultSet.getObject(i));
        }
        rows.add(row);
      }
      currentRow = -1;
    }

    @Override
    public boolean next() throws SQLException {
      if (rows.size() == 0 || isLast()) {
        return false;
      }
      currentRow++;
      return true;
    }

    @Override
    public void close() throws SQLException {
      currentRow = rows.size() - 1;
    }

    @Override
    public boolean wasNull() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getCursorName() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      if (this.currentRow < 0 || this.currentRow >= this.rows.size()) {
        return null; // out of boundaries
      }
      CachedRow row = this.rows.get(this.currentRow);
      if (!row.columnByIndex.containsKey(columnIndex)) {
        return null; // column index out of boundaries
      }
      return row.columnByIndex.get(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
      if (this.currentRow < 0 || this.currentRow >= this.rows.size()) {
        return null; // out of boundaries
      }
      CachedRow row = this.rows.get(this.currentRow);
      if (!row.columnByName.containsKey(columnLabel)) {
        return null; // column name not found
      }
      return row.columnByName.get(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
      return this.currentRow < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
      return this.currentRow >= this.rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
      return this.currentRow == 0 && this.rows.size() > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
      return this.currentRow == (this.rows.size() - 1) && this.rows.size() > 0;
    }

    @Override
    public void beforeFirst() throws SQLException {
      this.currentRow = -1;
    }

    @Override
    public void afterLast() throws SQLException {
      this.currentRow = this.rows.size();
    }

    @Override
    public boolean first() throws SQLException {
      this.currentRow = 0;
      return this.currentRow < this.rows.size();
    }

    @Override
    public boolean last() throws SQLException {
      this.currentRow = this.rows.size() - 1;
      return this.currentRow >= 0;
    }

    @Override
    public int getRow() throws SQLException {
      return this.currentRow + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
      if (row > 0) {
        this.currentRow = row - 1;
      } else {
        this.currentRow = this.rows.size() + row;
      }
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
      this.currentRow += rows;
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public boolean previous() throws SQLException {
      this.currentRow--;
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getType() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getConcurrency() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void insertRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Statement getStatement() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
      return false;
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNString(int columnIndex, String nString) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNString(String columnLabel, String nString) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("checkstyle:MethodName")
    public NClob getNClob(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      return type.cast(getObject(columnIndex));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      return type.cast(getObject(columnLabel));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return iface == ResultSet.class ? iface.cast(this) : null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface != null && iface.isAssignableFrom(this.getClass());
    }
  }
}
