package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;

public class CachedResultSetTest {
  private CachedResultSet testResultSet;
  @Mock ResultSet mockResultSet;
  @Mock ResultSetMetaData mockResultSetMetadata;
  private AutoCloseable closeable;
  private static Calendar estCal = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
  private TimeZone defaultTimeZone = TimeZone.getDefault();

  // Column values: label, name, typeName, type, displaySize, precision, tableName,
  // scale, schemaName, isAutoIncrement, isCaseSensitive, isCurrency, isDefinitelyWritable,
  // isNullable, isReadOnly, isSearchable, isSigned, isWritable
  private static final Object [][] testColumnMetadata = {
      {"fieldNull", "fieldNull", "String", Types.VARCHAR, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldInt", "fieldInt", "Integer", Types.INTEGER, 10, 2, "table", 1, "public", true, false, false, false, 0, false, true, true, true},
      {"fieldString", "fieldString", "String", Types.VARCHAR, 10, 2, "table", 1, "public", false, false, false, false, 0, false, true, false, true},
      {"fieldBoolean", "fieldBoolean", "Boolean", Types.BOOLEAN, 10, 2, "table", 1, "public", false, false, false, false, 0, false, true, false, true},
      {"fieldByte", "fieldByte", "Byte", Types.TINYINT, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldShort", "fieldShort", "Short", Types.SMALLINT, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldLong", "fieldLong", "Long", Types.BIGINT, 10, 2, "table", 1, "public", false, false, false, false, 1, false, true, false, false},
      {"fieldFloat", "fieldFloat", "Float", Types.REAL, 10, 2, "table", 1, "public", false, false, false, false, 0, true, true, false, false},
      {"fieldDouble", "fieldDouble", "Double", Types.DOUBLE, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldBigDecimal", "fieldBigDecimal", "BigDecimal", Types.DECIMAL, 10, 2, "table", 1, "public", false, false, false, false, 0, true, true, false, false},
      {"fieldDate", "fieldDate", "Date", Types.DATE, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldTime", "fieldTime", "Time", Types.TIME, 10, 2, "table", 1, "public", false, false, false, false, 1, true, true, false, false},
      {"fieldDateTime", "fieldDateTime", "Timestamp", Types.TIMESTAMP, 10, 2, "table", 1, "public", false, false, false, false, 0, true, true, false, false}
  };

  private static final Object [][] testColumnValues = {
      {null, null},
      {1, 123456},
      {"John Doe", "Tony Stark"},
      {true, false},
      {(byte)100, (byte)70}, // Letter d and F in ASCII
      {(short)55, (short)135},
      {2^33L, -2^35L},
      {3.14159f, -233.14159f},
      {2345.23345d, -2344355.4543d},
      {new BigDecimal("15.33"), new BigDecimal("-12.45")},
      {Date.valueOf("2025-03-15"), Date.valueOf("1102-01-15")},
      {Time.valueOf("22:54:00"), Time.valueOf("01:10:00")},
      {Timestamp.valueOf("2025-03-15 22:54:00"), Timestamp.valueOf("1950-01-18 21:50:05")}
  };

  private void mockGetMetadataFields(int column, int testMetadataCol) throws SQLException {
    when(mockResultSetMetadata.getCatalogName(column)).thenReturn("");
    when(mockResultSetMetadata.getColumnClassName(column)).thenReturn("MyClass" + testMetadataCol);
    when(mockResultSetMetadata.getColumnLabel(column)).thenReturn((String) testColumnMetadata[testMetadataCol][0]);
    when(mockResultSetMetadata.getColumnName(column)).thenReturn((String) testColumnMetadata[testMetadataCol][1]);
    when(mockResultSetMetadata.getColumnTypeName(column)).thenReturn((String) testColumnMetadata[testMetadataCol][2]);
    when(mockResultSetMetadata.getColumnType(column)).thenReturn((Integer) testColumnMetadata[testMetadataCol][3]);
    when(mockResultSetMetadata.getColumnDisplaySize(column)).thenReturn((Integer) testColumnMetadata[testMetadataCol][4]);
    when(mockResultSetMetadata.getPrecision(column)).thenReturn((Integer) testColumnMetadata[testMetadataCol][5]);
    when(mockResultSetMetadata.getTableName(column)).thenReturn((String) testColumnMetadata[testMetadataCol][6]);
    when(mockResultSetMetadata.getScale(column)).thenReturn((Integer) testColumnMetadata[testMetadataCol][7]);
    when(mockResultSetMetadata.getSchemaName(column)).thenReturn((String) testColumnMetadata[testMetadataCol][8]);
    when(mockResultSetMetadata.isAutoIncrement(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][9]);
    when(mockResultSetMetadata.isCaseSensitive(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][10]);
    when(mockResultSetMetadata.isCurrency(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][11]);
    when(mockResultSetMetadata.isDefinitelyWritable(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][12]);
    when(mockResultSetMetadata.isNullable(column)).thenReturn((Integer) testColumnMetadata[testMetadataCol][13]);
    when(mockResultSetMetadata.isReadOnly(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][14]);
    when(mockResultSetMetadata.isSearchable(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][15]);
    when(mockResultSetMetadata.isSigned(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][16]);
    when(mockResultSetMetadata.isWritable(column)).thenReturn((Boolean) testColumnMetadata[testMetadataCol][17]);
  }

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
  }

  @AfterEach
  void cleanUp() {
    TimeZone.setDefault(defaultTimeZone);
  }

  void setUpDefaultTestResultSet() throws SQLException {
    // Create the default CachedResultSet for testing
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    when(mockResultSetMetadata.getColumnCount()).thenReturn(13);
    for (int i = 0; i < 13; i++) {
      mockGetMetadataFields(1+i, i);
      when(mockResultSet.getObject(1+i)).thenReturn(testColumnValues[i][0], testColumnValues[i][1]);
    }
    when(mockResultSet.next()).thenReturn(true, true, false);
    testResultSet = new CachedResultSet(mockResultSet);
  }

  private void verifyDefaultMetadata(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    for (int i = 0; i < md.getColumnCount(); i++) {
      assertEquals("", md.getCatalogName(i+1));
      assertEquals("MyClass" + i, md.getColumnClassName(i+1));
      assertEquals(testColumnMetadata[i][0], md.getColumnLabel(i+1));
      assertEquals(testColumnMetadata[i][1], md.getColumnName(i+1));
      assertEquals(testColumnMetadata[i][2], md.getColumnTypeName(i+1));
      assertEquals(testColumnMetadata[i][3], md.getColumnType(i+1));
      assertEquals(testColumnMetadata[i][4], md.getColumnDisplaySize(i+1));
      assertEquals(testColumnMetadata[i][5], md.getPrecision(i+1));
      assertEquals(testColumnMetadata[i][6], md.getTableName(i+1));
      assertEquals(testColumnMetadata[i][7], md.getScale(i+1));
      assertEquals(testColumnMetadata[i][8], md.getSchemaName(i+1));
      assertEquals(testColumnMetadata[i][9], md.isAutoIncrement(i+1));
      assertEquals(testColumnMetadata[i][10], md.isCaseSensitive(i+1));
      assertEquals(testColumnMetadata[i][11], md.isCurrency(i+1));
      assertEquals(testColumnMetadata[i][12], md.isDefinitelyWritable(i+1));
      assertEquals(testColumnMetadata[i][13], md.isNullable(i+1));
      assertEquals(testColumnMetadata[i][14], md.isReadOnly(i+1));
      assertEquals(testColumnMetadata[i][15], md.isSearchable(i+1));
      assertEquals(testColumnMetadata[i][16], md.isSigned(i+1));
      assertEquals(testColumnMetadata[i][17], md.isWritable(i+1));
    }
  }

  private void verifyDefaultRow(ResultSet rs, int row) throws SQLException {
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(1)); // fieldNull
    assertEquals(1, rs.findColumn("fieldNull"));
    assertTrue(rs.wasNull());
    assertEquals((int) testColumnValues[1][row], rs.getInt(2)); // fieldInt
    assertFalse(rs.wasNull());
    assertEquals((int) testColumnValues[1][row], rs.getInt("fieldInt"));
    assertEquals(2, rs.findColumn("fieldInt"));
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[2][row], rs.getString(3)); // fieldString
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[2][row], rs.getString("fieldString"));
    assertEquals(3, rs.findColumn("fieldString"));
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[3][row], rs.getBoolean(4)); // fieldBoolean
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[3][row], rs.getBoolean("fieldBoolean"));
    assertEquals(4, rs.findColumn("fieldBoolean"));
    assertFalse(rs.wasNull());
    assertEquals((byte) testColumnValues[4][row], rs.getByte(5)); // fieldByte
    assertFalse(rs.wasNull());
    assertEquals((byte) testColumnValues[4][row], rs.getByte("fieldByte"));
    assertEquals(5, rs.findColumn("fieldByte"));
    assertFalse(rs.wasNull());
    assertEquals((short) testColumnValues[5][row], rs.getShort(6)); // fieldShort
    assertFalse(rs.wasNull());
    assertEquals((short) testColumnValues[5][row], rs.getShort("fieldShort"));
    assertEquals(6, rs.findColumn("fieldShort"));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject("fieldNull"));
    assertTrue(rs.wasNull());
    assertEquals((Long) testColumnValues[6][row], rs.getLong(7)); // fieldLong
    assertFalse(rs.wasNull());
    assertEquals((Long) testColumnValues[6][row], rs.getLong("fieldLong"));
    assertEquals(7, rs.findColumn("fieldLong"));
    assertFalse(rs.wasNull());
    assertEquals((float) testColumnValues[7][row], rs.getFloat(8), 0); // fieldFloat
    assertFalse(rs.wasNull());
    assertEquals((float) testColumnValues[7][row], rs.getFloat("fieldFloat"), 0);
    assertEquals(8, rs.findColumn("fieldFloat"));
    assertFalse(rs.wasNull());
    assertEquals((double) testColumnValues[8][row], rs.getDouble(9));  // fieldDouble
    assertFalse(rs.wasNull());
    assertEquals((double) testColumnValues[8][row], rs.getDouble("fieldDouble"));
    assertEquals(9, rs.findColumn("fieldDouble"));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getBigDecimal(10).compareTo((BigDecimal) testColumnValues[9][row])); // fieldBigDecimal
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getBigDecimal("fieldBigDecimal").compareTo((BigDecimal) testColumnValues[9][row]));
    assertEquals(10, rs.findColumn("fieldBigDecimal"));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(1)); // fieldNull
    assertTrue(rs.wasNull());
    assertEquals(testColumnValues[10][row], rs.getDate(11)); // fieldDate
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[10][row], rs.getDate("fieldDate"));
    assertEquals(11, rs.findColumn("fieldDate"));
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[11][row], rs.getTime(12)); // fieldTime
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[11][row], rs.getTime("fieldTime"));
    assertEquals(12, rs.findColumn("fieldTime"));
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[12][row], rs.getTimestamp(13)); // fieldDateTime
    assertFalse(rs.wasNull());
    assertEquals(testColumnValues[12][row], rs.getTimestamp("fieldDateTime"));
    assertEquals(13, rs.findColumn("fieldDateTime"));
    assertFalse(rs.wasNull());
    verifyNonexistingField(rs);
  }

  private void verifyNonexistingField(ResultSet rs) {
    try {
      rs.getObject("nonExistingField");
      throw new IllegalStateException("Expected an exception due to column doesn't exist");
    } catch (SQLException e) {
      // Expected an exception if the column doesn't exist
    }
    try {
      rs.findColumn("nonExistingField");
      throw new IllegalStateException("Expected an exception due to column doesn't exist");
    } catch (SQLException e) {
      // Expected an exception if the column doesn't exist
    }
  }

  @Test
  void test_basic_cached_result_set() throws Exception {
    // Basic verification of the test result set
    setUpDefaultTestResultSet();
    verifyDefaultMetadata(testResultSet);
    assertEquals(0, testResultSet.getRow());
    assertTrue(testResultSet.next());
    assertEquals(1, testResultSet.getRow());
    verifyDefaultRow(testResultSet, 0);
    assertTrue(testResultSet.next());
    assertEquals(2, testResultSet.getRow());
    verifyDefaultRow(testResultSet, 1);
    assertFalse(testResultSet.next());
    assertEquals(0, testResultSet.getRow());
    assertNull(testResultSet.getWarnings());
    testResultSet.clearWarnings();
    assertNull(testResultSet.getWarnings());
    testResultSet.beforeFirst();
    // Test serialization and de-serialization of the result set
    byte[] serialized_data = testResultSet.serializeIntoByteArray();
    ResultSet rs = CachedResultSet.deserializeFromByteArray(serialized_data);
    verifyDefaultMetadata(rs);
    assertTrue(rs.next());
    verifyDefaultRow(rs, 0);
    assertTrue(rs.next());
    verifyDefaultRow(rs, 1);
    assertFalse(rs.next());
    assertNull(rs.getWarnings());
    rs.relative(-10); // We should be before the start of the rows
    assertTrue(rs.isBeforeFirst());
    assertEquals(0, rs.getRow());
    rs.relative(10); // We should be after the end of the rows
    assertTrue(rs.isAfterLast());
    assertEquals(0, rs.getRow());
    rs.absolute(-10); // We should be before the start of the rows
    assertTrue(rs.isBeforeFirst());
    assertFalse(rs.absolute(100)); // Jump to after the end of the rows
    assertTrue(rs.isAfterLast());
    assertEquals(0, rs.getRow());
    assertFalse(rs.absolute(0)); // Go to the beginning of rows
    assertTrue(rs.isBeforeFirst());
    assertTrue(rs.next()); // We are at first row
    verifyDefaultRow(rs, 0);
    rs.relative(1); // Advances to next row
    verifyDefaultRow(rs, 1);
    assertTrue(rs.previous()); // Go back to first row
    verifyDefaultRow(rs, 0);
    assertFalse(rs.previous());
    assertTrue(rs.absolute(2)); // Jump to second row
    verifyDefaultRow(rs, 1);
    assertTrue(rs.first()); // go to first row
    verifyDefaultRow(rs, 0);
    assertEquals(1, rs.getRow());
    assertTrue(rs.last()); // go to last row
    verifyDefaultRow(rs, 1);
    assertEquals(2, rs.getRow());
  }

  @Test
  void test_get_special_bigDecimal() throws SQLException {
    // Create the default CachedResultSet for testing
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    when(mockResultSetMetadata.getColumnCount()).thenReturn(1);
    mockGetMetadataFields(1, 9);
    when(mockResultSet.getObject(1)).thenReturn(
        12450.567,
        -132.45,
        "142.346",
        "invalid",
        null);
    when(mockResultSet.next()).thenReturn(true, true, true, true, true, false);
    CachedResultSet rs = new CachedResultSet(mockResultSet);

    assertTrue(rs.next());
    assertEquals(0, rs.getBigDecimal(1).compareTo(new BigDecimal("12450.567")));

    assertTrue(rs.next());
    assertEquals(0, rs.getBigDecimal(1).compareTo(new BigDecimal("-132.45")));
    assertTrue(rs.next());
    assertEquals(0, rs.getBigDecimal(1).compareTo(new BigDecimal("142.346")));
    assertTrue(rs.next());
    try {
      rs.getBigDecimal(1);
      fail("Invalid value should cause a test failure");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // Value is null
    assertTrue(rs.next());
    assertNull(rs.getBigDecimal(1));
  }

  @Test
  void test_get_special_timestamp() throws SQLException {
    // Create the default CachedResultSet for testing
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    when(mockResultSetMetadata.getColumnCount()).thenReturn(1);
    mockGetMetadataFields(1, 12);
    when(mockResultSet.getObject(1)).thenReturn(
          1504844311000L,
          LocalDateTime.of(1981, 3, 10, 1, 10, 20),
          OffsetDateTime.parse("2025-08-10T10:00:00+03:00"),
          ZonedDateTime.parse("2024-07-30T10:00:00+02:00[Europe/Berlin]"),
          "2015-03-15 12:50:04",
          "invalidDateTime",
          null);
    when(mockResultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
    CachedResultSet cachedRs = new CachedResultSet(mockResultSet);

    // Timestamp from a number
    assertTrue(cachedRs.next());
    assertEquals(new Timestamp(1504844311000L), cachedRs.getTimestamp(1));
    // Timestamp from LocalDateTime
    assertTrue(cachedRs.next());
    assertEquals(Timestamp.valueOf("1981-03-10 01:10:20"), cachedRs.getTimestamp(1));
    assertEquals(Timestamp.valueOf("1981-03-09 22:10:20"), cachedRs.getTimestamp(1, estCal));
    // Timestamp from OffsetDateTime (containing time zone info)
    assertTrue(cachedRs.next());
    assertEquals(Timestamp.valueOf("2025-08-10 00:00:00"), cachedRs.getTimestamp(1));
    assertEquals(Timestamp.valueOf("2025-08-10 00:00:00"), cachedRs.getTimestamp(1, estCal));
    // Timestmap from ZonedDateTime (containing time zone info)
    assertTrue(cachedRs.next());
    assertEquals(Timestamp.valueOf("2024-07-30 01:00:00"), cachedRs.getTimestamp(1));
    assertEquals(Timestamp.valueOf("2024-07-30 01:00:00"), cachedRs.getTimestamp(1, estCal));
    // Timestamp from String
    assertTrue(cachedRs.next());
    assertEquals(Timestamp.valueOf("2015-03-15 12:50:04"), cachedRs.getTimestamp(1));
    assertEquals(Timestamp.valueOf("2015-03-15 12:50:04"), cachedRs.getTimestamp(1, estCal));
    assertTrue(cachedRs.next());
    try {
      cachedRs.getTimestamp(1);
      fail("Invalid timestamp should cause a test failure");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // Timestamp is null
    assertTrue(cachedRs.next());
    assertNull(cachedRs.getTimestamp(1));
  }

  @Test
  void test_get_special_time() throws SQLException {
    // Create the default CachedResultSet for testing
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    when(mockResultSetMetadata.getColumnCount()).thenReturn(1);
    mockGetMetadataFields(1, 11);
    when(mockResultSet.getObject(1)).thenReturn(
        4362000L,
        LocalTime.of(10, 20, 30),
        OffsetTime.of(12, 15, 30, 0, ZoneOffset.UTC),
        "15:34:20",
        "InvalidTime",
        null);
    when(mockResultSet.next()).thenReturn(true, true, true, true, true, true, false);
    CachedResultSet cachedRs = new CachedResultSet(mockResultSet);

    // Time from a number
    assertTrue(cachedRs.next());
    assertEquals(new Time(4362000L), cachedRs.getTime(1));
    // Time from LocalTime
    assertTrue(cachedRs.next());
    assertEquals(Time.valueOf("10:20:30"), cachedRs.getTime(1));
    assertEquals(Time.valueOf("07:20:30"), cachedRs.getTime(1, estCal));
    // Time from OffsetTime
    assertTrue(cachedRs.next());
    assertEquals(Time.valueOf("05:15:30"), cachedRs.getTime(1));
    assertEquals(Time.valueOf("05:15:30"), cachedRs.getTime(1, estCal));
    // Timestamp from String
    assertTrue(cachedRs.next());
    assertEquals(Time.valueOf("15:34:20"), cachedRs.getTime(1));
    assertEquals(Time.valueOf("15:34:20"), cachedRs.getTime(1, estCal));
    assertTrue(cachedRs.next());
    try {
      cachedRs.getTime(1);
      fail("Invalid time should cause a test failure");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // Time is null
    assertTrue(cachedRs.next());
    assertNull(cachedRs.getTime(1));
  }

  @Test
  void test_get_special_date() throws SQLException {
    // Create the default CachedResultSet for testing
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetadata);
    when(mockResultSetMetadata.getColumnCount()).thenReturn(1);
    mockGetMetadataFields(1, 10);
    when(mockResultSet.getObject(1)).thenReturn(
        1515944311000L,
        -1000000000L,
        LocalDate.of(2010, 10, 30),
        "2025-03-15",
        "InvalidDate",
        null);
    when(mockResultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
    CachedResultSet cachedRs = new CachedResultSet(mockResultSet);

    // Date from a number
    assertTrue(cachedRs.next());
    Date date = cachedRs.getDate(1);
    assertEquals(new Date(1515944311000L), date);
    assertTrue(cachedRs.next());
    assertEquals(new Date(-1000000000L), cachedRs.getDate(1));
    // Date from LocalDate
    assertTrue(cachedRs.next());
    assertEquals(Date.valueOf("2010-10-30"), cachedRs.getDate(1));
    assertEquals(Date.valueOf("2010-10-29"), cachedRs.getDate(1, estCal));
    // Date from String
    assertTrue(cachedRs.next());
    assertEquals(Date.valueOf("2025-03-15"), cachedRs.getDate(1));
    assertEquals(Date.valueOf("2025-03-15"), cachedRs.getDate(1, estCal));
    assertTrue(cachedRs.next());
    try {
      cachedRs.getDate(1);
      fail("Invalid date should cause a test failure");
    } catch (IllegalArgumentException e) {
      // pass
    }
    // Date is null
    assertTrue(cachedRs.next());
    assertNull(cachedRs.getDate(1));
  }
}
