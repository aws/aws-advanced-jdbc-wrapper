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

package software.amazon.jdbc.plugin;

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
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

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

  static {
    PropertyDefinition.registerPluginProperties(DataCacheConnectionPlugin.class);
  }

  public DataCacheConnectionPlugin(final Properties props) {
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
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    if (StringUtils.isNullOrEmpty(this.dataCacheTriggerCondition) || resultClass != ResultSet.class) {
      return jdbcMethodFunc.call();
    }

    ResultSet result;
    boolean needToCache = false;
    final String sql = getQuery(jdbcMethodArgs);

    if (!StringUtils.isNullOrEmpty(sql) && sql.matches(this.dataCacheTriggerCondition)) {
      result = dataCache.get(sql);
      if (result == null) {
        needToCache = true;
        LOGGER.finest(
            () -> Messages.get(
                "DataCacheConnectionPlugin.queryResultsCached",
                new Object[]{methodName, sql}));
      } else {
        try {
          result.beforeFirst();
        } catch (final SQLException ex) {
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
      final ResultSet cachedResultSet;
      try {
        cachedResultSet = new CachedResultSet(result);
        dataCache.put(sql, cachedResultSet);
        cachedResultSet.beforeFirst();
        return resultClass.cast(cachedResultSet);
      } catch (final SQLException ex) {
        // ignore exception
      }
    }

    return resultClass.cast(result);
  }

  protected String getQuery(final Object[] jdbcMethodArgs) {

    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString();
    }
    return null;
  }

  public static class CachedRow {
    protected final HashMap<Integer, Object> columnByIndex = new HashMap<>();
    protected final HashMap<String, Object> columnByName = new HashMap<>();

    public void put(final int columnIndex, final String columnName, final Object columnValue) {
      columnByIndex.put(columnIndex, columnValue);
      columnByName.put(columnName, columnValue);
    }

    @SuppressWarnings("unused")
    public Object get(final int columnIndex) {
      return columnByIndex.get(columnIndex);
    }

    @SuppressWarnings("unused")
    public Object get(final String columnName) {
      return columnByName.get(columnName);
    }
  }

  @SuppressWarnings({"RedundantThrows", "checkstyle:OverloadMethodsDeclarationOrder"})
  public static class CachedResultSet implements ResultSet {

    protected ArrayList<CachedRow> rows;
    protected int currentRow;

    public CachedResultSet(final ResultSet resultSet) throws SQLException {

      final ResultSetMetaData md = resultSet.getMetaData();
      final int columns = md.getColumnCount();
      rows = new ArrayList<>();

      while (resultSet.next()) {
        final CachedRow row = new CachedRow();
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
    public String getString(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
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
    public Object getObject(final int columnIndex) throws SQLException {
      if (this.currentRow < 0 || this.currentRow >= this.rows.size()) {
        return null; // out of boundaries
      }
      final CachedRow row = this.rows.get(this.currentRow);
      if (!row.columnByIndex.containsKey(columnIndex)) {
        return null; // column index out of boundaries
      }
      return row.columnByIndex.get(columnIndex);
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
      if (this.currentRow < 0 || this.currentRow >= this.rows.size()) {
        return null; // out of boundaries
      }
      final CachedRow row = this.rows.get(this.currentRow);
      if (!row.columnByName.containsKey(columnLabel)) {
        return null; // column name not found
      }
      return row.columnByName.get(columnLabel);
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
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
    public boolean absolute(final int row) throws SQLException {
      if (row > 0) {
        this.currentRow = row - 1;
      } else {
        this.currentRow = this.rows.size() + row;
      }
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
      this.currentRow += rows;
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public boolean previous() throws SQLException {
      this.currentRow--;
      return this.currentRow >= 0 && this.currentRow < this.rows.size();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
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
    public void updateNull(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(final int columnIndex, final long x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(final String columnLabel, final byte x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(final String columnLabel, final short x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(final String columnLabel, final int x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(final String columnLabel, final long x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(final String columnLabel, final float x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(final String columnLabel, final double x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(final String columnLabel, final String x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(final String columnLabel, final Date x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(final String columnLabel, final Time x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final int length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final int length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader, final int length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(final String columnLabel, final Object x, final int scaleOrLength)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(final String columnLabel, final Object x) throws SQLException {
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
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(final String columnLabel, final Ref x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final String columnLabel, final Clob x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(final String columnLabel, final Array x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
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
    public void updateNString(final int columnIndex, final String nString) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNString(final String columnLabel, final String nString) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
    public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("checkstyle:MethodName")
    public NClob getNClob(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader, final long length)
        throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
      return type.cast(getObject(columnIndex));
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
      return type.cast(getObject(columnLabel));
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
      return iface == ResultSet.class ? iface.cast(this) : null;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
      return iface != null && iface.isAssignableFrom(this.getClass());
    }
  }
}
