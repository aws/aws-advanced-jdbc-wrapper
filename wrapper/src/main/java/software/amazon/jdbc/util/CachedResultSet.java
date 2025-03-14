package software.amazon.jdbc.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"RedundantThrows", "checkstyle:OverloadMethodsDeclarationOrder"})
public class CachedResultSet implements ResultSet {

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

  public CachedResultSet(final List<Map<String, Object>> resultList) {
    rows = new ArrayList<>();
    for (Map<String, Object> rowMap : resultList) {
      final CachedRow row = new CachedRow();
      int i = 1;
      for (String columnName : rowMap.keySet()) {
        row.put(i, columnName, rowMap.get(columnName));
      }
      rows.add(row);
    }
    currentRow = -1;
  }

  public static String serializeIntoJsonString(ResultSet rs) throws SQLException {
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> resultList = new ArrayList<>();
    ResultSetMetaData metaData = rs.getMetaData();
    int columns = metaData.getColumnCount();

    while (rs.next()) {
      Map<String, Object> rowMap = new HashMap<>();
      for (int i = 1; i <= columns; i++) {
        rowMap.put(metaData.getColumnName(i), rs.getObject(i));
      }
      resultList.add(rowMap);
    }
    try {
      return mapper.writeValueAsString(resultList);
    } catch (JsonProcessingException e) {
      throw new SQLException("Error serializing ResultSet to JSON", e);
    }
  }

  public static ResultSet deserializeFromJsonString(String jsonString) throws SQLException {
    if (jsonString == null || jsonString.isEmpty()) { return null; }
    try {
      ObjectMapper mapper = new ObjectMapper();
      List<Map<String, Object>> resultList = mapper.readValue(jsonString,
          mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
      return new CachedResultSet(resultList);
    } catch (JsonProcessingException e) {
      throw new SQLException("Error de-serializing ResultSet from JSON", e);
    }
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

  // TODO: implement all the getXXX APIs.
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
