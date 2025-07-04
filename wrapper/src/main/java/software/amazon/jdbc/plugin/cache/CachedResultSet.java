package software.amazon.jdbc.plugin.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
import java.time.Instant;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.GregorianCalendar;

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
  protected boolean wasNullFlag;
  protected ResultSetMetaData metadata;
  protected static ObjectMapper mapper = new ObjectMapper();
  protected static boolean mapperInitialized = false;
  protected static final TimeZone defaultTimeZone = TimeZone.getDefault();
  private static final Calendar calendarWithUserTz = new GregorianCalendar();

  public CachedResultSet(final ResultSet resultSet) throws SQLException {
    metadata = resultSet.getMetaData();
    final int columns = metadata.getColumnCount();
    rows = new ArrayList<>();

    while (resultSet.next()) {
      final CachedRow row = new CachedRow();
      for (int i = 1; i <= columns; ++i) {
        row.put(i, metadata.getColumnName(i), resultSet.getObject(i));
      }
      rows.add(row);
    }
    currentRow = -1;
    initializeObjectMapper();
  }

  public CachedResultSet(final List<Map<String, Object>> resultList) {
    rows = new ArrayList<>();
    int numFields = resultList.isEmpty() ? 0 : resultList.get(0).size();
    CachedResultSetMetaData.Field[] fields = new CachedResultSetMetaData.Field[numFields];
    if (!resultList.isEmpty()) {
      boolean fieldsInitialized = false;
      for (Map<String, Object> rowMap : resultList) {
        final CachedRow row = new CachedRow();
        int i = 0;
        for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
          String columnName = entry.getKey();
          if (!fieldsInitialized) {
            fields[i] = new CachedResultSetMetaData.Field(columnName, columnName);
          }
          row.put(++i, columnName, entry.getValue());
        }
        rows.add(row);
        fieldsInitialized = true;
      }
    }
    currentRow = -1;
    metadata = new CachedResultSetMetaData(fields);
    initializeObjectMapper();
  }

  // Helper method to initialize the object mapper for serialization of objects
  private void initializeObjectMapper() {
    if (mapperInitialized) return;
    // For serialization of Date/LocalDateTime etc, set up the time module,
    // and use standard string format (i.e. ISO)
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapperInitialized = true;
  }

  public String serializeIntoJsonString() throws SQLException {
    List<Map<String, Object>> resultList = new ArrayList<>();
    ResultSetMetaData metaData = this.getMetaData();
    int columns = metaData.getColumnCount();

    while (this.next()) {
      Map<String, Object> rowMap = new HashMap<>();
      for (int i = 1; i <= columns; i++) {
        rowMap.put(metaData.getColumnName(i), this.getObject(i));
      }
      resultList.add(rowMap);
    }
    try {
      return mapper.writeValueAsString(resultList);
    } catch (JsonProcessingException e) {
      throw new SQLException("Error serializing ResultSet to JSON: " + e.getMessage(), e);
    }
  }

  public static ResultSet deserializeFromJsonString(String jsonString) throws SQLException {
    if (jsonString == null || jsonString.isEmpty()) { return null; }
    try {
      List<Map<String, Object>> resultList = mapper.readValue(jsonString,
          mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
      return new CachedResultSet(resultList);
    } catch (JsonProcessingException e) {
      throw new SQLException("Error de-serializing ResultSet from JSON", e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (rows.isEmpty() || isLast()) {
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
    if (isClosed()) {
      throw new SQLException("This result set is closed");
    }
    return this.wasNullFlag;
  }

  // TODO: implement all the getXXX APIs.
  @Override
  public String getString(final int columnIndex) throws SQLException {
    Object value = this.getObject(columnIndex);
    if (value == null) return null;
    return value.toString();
  }

  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) return false;
    return Boolean.parseBoolean(value);
  }

  @Override
  public byte getByte(final int columnIndex) throws SQLException {
    return (byte)this.getInt(columnIndex);
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) throw new SQLException("Column index " + columnIndex + " doesn't exist");
    return Short.parseShort(value);
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) throw new SQLException("Column index " + columnIndex + " doesn't exist");
    return Integer.parseInt(value);
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) throw new SQLException("Column index " + columnIndex + " doesn't exist");
    return Long.parseLong(value);
  }

  @Override
  public float getFloat(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) throw new SQLException("Column index " + columnIndex + " doesn't exist");
    return Float.parseFloat(value);
  }

  @Override
  public double getDouble(final int columnIndex) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) throw new SQLException("Column index " + columnIndex + " doesn't exist");
    return Double.parseDouble(value);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    String value = this.getString(columnIndex);
    if (value == null) return null;
    return new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public byte[] getBytes(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  private Timestamp convertLocalTimeToTimestamp(final LocalDateTime localTime, Calendar cal) {
    long epochTimeInMillis;
    if (cal != null) {
      epochTimeInMillis = localTime.atZone(cal.getTimeZone().toZoneId()).toInstant().toEpochMilli();
    } else {
      epochTimeInMillis = localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
    return new Timestamp(epochTimeInMillis);
  }

  private Timestamp parseIntoTimestamp(String timestampStr, Calendar cal) {
    if (timestampStr.endsWith("Z")) { // ISO format timestamp in UTC like 2025-06-03T11:59:21.822364Z
      return Timestamp.from(Instant.parse(timestampStr));
    } else if (timestampStr.contains("+")  || timestampStr.contains("-")) { // Offset timestamp format like 2023-10-27T10:00:00+02:00
      try {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestampStr);
        return Timestamp.from(offsetDateTime.toInstant());
      } catch (DateTimeParseException e) {
        // swallow this exception and move on with parsing
      }
    }

    if (timestampStr.contains(":")) { // timestamp without time zone info with HH:MM:ss info
      // The timestamp string doesn't contain time zone information (not recommended for storage). We need
      // to use the specified calendar for timezone. If calendar is not specified, use the local time zone.
      String ts = timestampStr;
      if (timestampStr.contains(" ")) {
        ts = timestampStr.replace(" ", "T");
      }
      // Obtains an instance of LocalDateTime from a text string that is in ISO_LOCAL_DATE_TIME format
      return convertLocalTimeToTimestamp(LocalDateTime.parse(ts), cal);
    } else { // timestamp without time zone info without HH:MM:ss info
      return new Timestamp(Date.valueOf(timestampStr).getTime());
    }
  }

  private Date convertToDate(Object dateObj, Calendar cal) {
    if (dateObj == null) return null;
    Timestamp timestamp;
    if (dateObj instanceof Date) {
      // Create and return a Timestamp from the milliseconds
      return (Date)dateObj;
    } else if (dateObj instanceof Timestamp) {
      timestamp = (Timestamp) dateObj;
    } else {
      // Try to parse as Date with hour/minute/second.
      // If Date parsing fails, try to parse it as Timestamp
      try {
        return Date.valueOf(dateObj.toString());
      } catch (IllegalArgumentException e) {
        // Failed to parse the string as Date object. Try parsing it as Timestamp instead
        timestamp = parseIntoTimestamp(dateObj.toString(), cal);
      }
    }

    // If the dateObj is not already the Date type, then the value cached is the
    // epoch time in milliseconds. Here we need to de-serialize it as a long
    if (cal == null) {
      calendarWithUserTz.setTimeZone(defaultTimeZone);
    } else {
      calendarWithUserTz.setTimeZone(cal.getTimeZone());
    }
    calendarWithUserTz.setTimeInMillis(timestamp.getTime());
    return new Date(calendarWithUserTz.getTimeInMillis());
  }

  @Override
  public Date getDate(final int columnIndex) throws SQLException {
    // The value cached is the string representation of epoch time in milliseconds
    return convertToDate(this.getObject(columnIndex), null);
  }

  private Time convertToTime(Object timeObj, Calendar cal) {
    if (timeObj == null) return null;
    Timestamp ts;
    if (timeObj instanceof Time) {
      return (Time) timeObj;
    } else if (timeObj instanceof Timestamp) {
      ts = (Timestamp) timeObj;
    } else {
      // Parse the time object from string. If it can't be parsed
      // as a Time object, then try to parse it as Timestamp.
      try {
        String timeStr = timeObj.toString();
        if (timeStr.contains("Z")) {
          // TODO: fix getTime with a different time zone
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ssX");
          LocalTime localTime = LocalTime.parse(timeStr, formatter);
          return Time.valueOf(localTime);
        } else if (timeStr.contains("+") || timeStr.contains("-")) {
          LocalTime localTime = OffsetTime.parse(timeStr).toLocalTime();
          return Time.valueOf(localTime);
        } else {
          LocalTime localTime = LocalTime.parse(timeObj.toString());
          return Time.valueOf(localTime);
        }
      } catch (DateTimeParseException e) {
        ts = parseIntoTimestamp(timeObj.toString(), cal);
      }
    }
    // use the timezone in the cal (if set) to indicate proper time for "1:00:00"
    // e.g. 10:00:00 in EST is 07:00:00 in local time zone (PST)
    if (cal == null) {
      calendarWithUserTz.setTimeZone(defaultTimeZone);
    } else {
      calendarWithUserTz.setTimeZone(cal.getTimeZone());
    }
    calendarWithUserTz.setTimeInMillis(ts.getTime());
    return new Time(calendarWithUserTz.getTimeInMillis());
  }

  @Override
  public Time getTime(final int columnIndex) throws SQLException {
    return convertToTime(this.getObject(columnIndex), null);
  }

  private Timestamp convertToTimestamp(Object timestampObj, Calendar calendar) {
    if (timestampObj == null) return null;
    if (timestampObj instanceof Timestamp) {
      return (Timestamp) timestampObj;
    } else if (timestampObj instanceof LocalDateTime) {
      return convertLocalTimeToTimestamp((LocalDateTime) timestampObj, calendar);
    } else {
      // De-serialize it from string representation
      return parseIntoTimestamp(timestampObj.toString(), calendar);
    }
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex) throws SQLException {
    return convertToTimestamp(this.getObject(columnIndex), null);
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
    Object value = this.getObject(columnLabel);
    if (value == null) return null;
    return value.toString();
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) return false;
    return Boolean.parseBoolean(value);
  }

  @Override
  public byte getByte(final String columnLabel) throws SQLException {
    return (byte)this.getInt(columnLabel);
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) throw new SQLException("Column " + columnLabel + " doesn't exist");
    return Short.parseShort(value);
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) throw new SQLException("Column " + columnLabel + " doesn't exist");
    return Integer.parseInt(value);
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) throw new SQLException("Column " + columnLabel + " doesn't exist");
    return Long.parseLong(value);
  }

  @Override
  public float getFloat(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) throw new SQLException("Column " + columnLabel + " doesn't exist");
    return Float.parseFloat(value);
  }

  @Override
  public double getDouble(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) throw new SQLException("Column " + columnLabel + " doesn't exist");
    return Double.parseDouble(value);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) return null;
    return new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public byte[] getBytes(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(final String columnLabel) throws SQLException {
    return convertToDate(this.getObject(columnLabel), null);
  }

  @Override
  public Time getTime(final String columnLabel) throws SQLException {
    return convertToTime(this.getObject(columnLabel), null);
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel) throws SQLException {
    return convertToTimestamp(this.getObject(columnLabel), null);
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
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // no-op
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
  }

  private void checkCurrentRow() throws SQLException {
    if (this.currentRow < 0 || this.currentRow >= this.rows.size()) {
      throw new SQLException("The current row index " + this.currentRow + " is out of range.");
    }
  }

  @Override
  public Object getObject(final int columnIndex) throws SQLException {
    checkCurrentRow();
    final CachedRow row = this.rows.get(this.currentRow);
    if (!row.columnByIndex.containsKey(columnIndex)) {
      throw new SQLException("The column index: " + columnIndex + " is out of range, number of columns: " + row.columnByIndex.size());
    }
    Object obj = row.columnByIndex.get(columnIndex);
    this.wasNullFlag = (obj == null);
    return obj;
  }

  @Override
  public Object getObject(final String columnLabel) throws SQLException {
    checkCurrentRow();
    final CachedRow row = this.rows.get(this.currentRow);
    if (!row.columnByName.containsKey(columnLabel)) {
      throw new SQLException("The column label: " + columnLabel + " is not found");
    }
    Object obj = row.columnByName.get(columnLabel);
    this.wasNullFlag = (obj == null);
    return obj;
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
    String value = this.getString(columnIndex);
    if (value == null) return null;
    return new BigDecimal(value);
  }

  @Override
  public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    String value = this.getString(columnLabel);
    if (value == null) return null;
    return new BigDecimal(value);
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
    return this.currentRow == 0 && !this.rows.isEmpty();
  }

  @Override
  public boolean isLast() throws SQLException {
    return this.currentRow == (this.rows.size() - 1) && !this.rows.isEmpty();
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
    if (row == 0) {
      this.beforeFirst();
      return false;
    } else {
      int rowsSize = this.rows.size();
      if (row < 0) {
        if (row < -rowsSize) {
          this.beforeFirst();
          return false;
        }
        this.currentRow = rowsSize + row;
      } else { // row > 0
        if (row > rowsSize) {
          this.afterLast();
          return false;
        }
        this.currentRow = row - 1;
      }
    }
    return true;
  }

  @Override
  public boolean relative(final int rows) throws SQLException {
    this.currentRow += rows;
    if (this.currentRow < 0) {
      this.beforeFirst();
      return false;
    } else if (this.currentRow >= this.rows.size()) {
      this.afterLast();
      return false;
    }
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    if (this.currentRow < 1) {
      this.beforeFirst();
      return false;
    }
    this.currentRow--;
    return true;
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
    return convertToDate(this.getObject(columnIndex), cal);
  }

  @Override
  public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
    return convertToDate(this.getObject(columnLabel), cal);
  }

  @Override
  public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
    return convertToTime(this.getObject(columnIndex), null);
  }

  @Override
  public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
    return convertToTime(this.getObject(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
    return convertToTimestamp(this.getObject(columnIndex), cal);
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
    return convertToTimestamp(this.getObject(columnLabel), cal);
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
    return this.rows == null;
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
