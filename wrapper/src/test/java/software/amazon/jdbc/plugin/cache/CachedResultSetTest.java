package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class CachedResultSetTest {
  static List<Map<String, Object>> testResultList = new ArrayList<>();
  static Calendar estCalendar = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));

  @BeforeAll
  static void setUp() {
    Map<String, Object> row = new HashMap<>();
    row.put("fieldInt", 1); // Integer
    row.put("fieldString", "John Doe"); // String
    row.put("fieldBoolean", true);
    row.put("fieldByte", (byte)100); // 100 in ASCII is letter d
    row.put("fieldShort", (short)55);
    row.put("fieldLong", 8589934592L); // 2^33
    row.put("fieldFloat", 3.14159f);
    row.put("fieldDouble", 2345.23345d);
    row.put("fieldBigDecimal", new BigDecimal("15.33"));
    row.put("fieldDate", Date.valueOf("2025-03-15"));
    row.put("fieldTime", Time.valueOf("22:54:00"));
    row.put("fieldDateTime", Timestamp.valueOf("2025-03-15 22:54:00"));
    testResultList.add(row);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("fieldInt", 123456); // Integer
    row2.put("fieldString", "Tony Stark"); // String
    row2.put("fieldBoolean", false);
    row2.put("fieldByte", (byte)70); // 100 in ASCII is letter F
    row2.put("fieldShort", (short)135);
    row2.put("fieldLong", -34359738368L); // -2^35
    row2.put("fieldFloat", -233.14159f);
    row2.put("fieldDouble", -2344355.4543d);
    row2.put("fieldBigDecimal", new BigDecimal("-12.45"));
    row2.put("fieldDate", Date.valueOf("1102-01-15"));
    row2.put("fieldTime", Time.valueOf("01:10:00"));
    row2.put("fieldDateTime", LocalDateTime.of(1981, 3, 10, 1, 10, 20));
    testResultList.add(row2);
  }

  private void verifyRow1(ResultSet rs) throws SQLException {
    Map<String, Integer> colNameToIndexMap = new HashMap<String, Integer>();
    ResultSetMetaData rsmd = rs.getMetaData();
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      colNameToIndexMap.put(rsmd.getColumnName(i), i);
    }
    assertEquals(1, rs.getInt(colNameToIndexMap.get("fieldInt")));
    assertEquals("John Doe", rs.getString(colNameToIndexMap.get("fieldString")));
    assertTrue(rs.getBoolean(colNameToIndexMap.get("fieldBoolean")));
    assertEquals(100, rs.getByte(colNameToIndexMap.get("fieldByte")));
    assertEquals(55, rs.getShort(colNameToIndexMap.get("fieldShort")));
    assertEquals(8589934592L, rs.getLong(colNameToIndexMap.get("fieldLong")));
    assertEquals(3.14159f, rs.getFloat(colNameToIndexMap.get("fieldFloat")), 0);
    assertEquals(2345.23345d, rs.getDouble(colNameToIndexMap.get("fieldDouble")));
    assertEquals(0, rs.getBigDecimal(colNameToIndexMap.get("fieldBigDecimal")).compareTo(new BigDecimal("15.33")));
    Date date = rs.getDate(colNameToIndexMap.get("fieldDate"));
    assertEquals(1742022000000L, date.getTime());
    Time time = rs.getTime(colNameToIndexMap.get("fieldTime"));
    assertEquals(111240000, time.getTime());
    Timestamp ts = rs.getTimestamp(colNameToIndexMap.get("fieldDateTime"));
    assertEquals(1742104440000L, ts.getTime());
  }

  private void verifyRow2(ResultSet rs) throws SQLException {
    assertEquals(123456, rs.getInt("fieldInt"));
    assertEquals("Tony Stark", rs.getString("fieldString"));
    assertFalse(rs.getBoolean("fieldBoolean"));
    assertEquals(70, rs.getByte("fieldByte"));
    assertEquals(135, rs.getShort("fieldShort"));
    assertEquals(-34359738368L, rs.getLong("fieldLong"));
    assertEquals(-233.14159f, rs.getFloat("fieldFloat"));
    assertEquals(-2344355.4543d, rs.getDouble("fieldDouble"));
    assertEquals(0, rs.getBigDecimal("fieldBigDecimal").compareTo(new BigDecimal("-12.45")));
    Date date = rs.getDate("fieldDate");
    assertEquals("1102-01-15", date.toString());
    Time time = rs.getTime("fieldTime");
    assertEquals("01:10:00", time.toString());
    Timestamp ts = rs.getTimestamp("fieldDateTime");
    assertTrue(ts.toString().startsWith("1981-03-10 01:10:20"));
  }

  @Test
  void test_create_and_verify_basic() throws Exception {
    ResultSet rs = new CachedResultSet(testResultList);
    verifyMetadata(rs);
    verifyContent(rs);
    rs.beforeFirst();
    CachedResultSet cachedRs = new CachedResultSet(rs);
    verifyMetadata(cachedRs);
    verifyContent(cachedRs);
  }

  @Test
  void test_serialize_and_deserialize_basic() throws Exception {
    CachedResultSet cachedRs = new CachedResultSet(testResultList);
    String serialized_data = cachedRs.serializeIntoJsonString();
    ResultSet rs = CachedResultSet.deserializeFromJsonString(serialized_data);
    verifyContent(rs);
  }

  private void verifyContent(ResultSet rs) throws SQLException {
    assertTrue(rs.next());
    if (rs.getInt("fieldInt") == 1) {
      verifyRow1(rs);
      assertTrue(rs.next());
      verifyRow2(rs);
    } else {
      verifyRow2(rs);
      assertTrue(rs.next());
      verifyRow1(rs);
    }
    assertFalse(rs.next());
  }

  private void verifyMetadata(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    List<String> expectedCols = Arrays.asList("fieldInt", "fieldString", "fieldBoolean", "fieldByte", "fieldShort", "fieldLong", "fieldFloat", "fieldDouble", "fieldBigDecimal", "fieldDate", "fieldTime", "fieldDateTime");
    assertEquals(md.getColumnCount(), testResultList.get(0).size());
    List<String> actualColNames = new ArrayList<>();
    List<String> actualColLabels = new ArrayList<>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      actualColNames.add(md.getColumnName(i));
      actualColLabels.add(md.getColumnLabel(i));
    }
    assertTrue(actualColNames.containsAll(expectedCols));
    assertTrue(expectedCols.containsAll(actualColNames));
    assertTrue(actualColLabels.containsAll(expectedCols));
    assertTrue(expectedCols.containsAll(actualColLabels));
  }

  @Test
  void test_get_timestamp() throws SQLException {
    // Timestamp string that is in ISO format with time zone information in UTC
    Map<String, Object> row = new HashMap<>();
    row.put("fieldTimestamp0", "2025-06-03T11:59:21.822364Z");
    // Timestamp string that is in ISO format with time zone information as offset
    row.put("fieldTimestamp1", "2024-02-13T07:40:30.822364-05:00");
    row.put("fieldTimestamp2", "2023-10-27T10:00:00+02:00");
    // Timestamp string doesn't contain time zone information.
    row.put("fieldTimestamp3", "1760-06-03T11:59:21.822364");
    row.put("fieldTimestamp4", "2020-05-04 10:06:10.822364");
    row.put("fieldTimestamp5", "2015-09-01 23:33:00");
    // Timestamp string doesn't contain time zone or HH:MM:SS information
    row.put("fieldTimestamp6", "2019-03-15");
    row.put("fieldTimestamp7", Timestamp.from(Instant.parse("2024-08-01T10:30:20.822364Z")));
    row.put("fieldTimestamp8", LocalDateTime.parse("2025-04-01T21:55:21.822364"));
    List<Map<String, Object>> testTimestamps = Collections.singletonList(row);
    CachedResultSet cachedRs = new CachedResultSet(testTimestamps);
    assertTrue(cachedRs.next());
    verifyTimestamps(cachedRs);
    cachedRs.beforeFirst();
    String serialized_data = cachedRs.serializeIntoJsonString();
    ResultSet rs = CachedResultSet.deserializeFromJsonString(serialized_data);
    assertTrue(rs.next());
    verifyTimestamps(rs);
  }

  private void verifyTimestamps(ResultSet rs) throws SQLException {
    // Verifying the timestamp with time zone information. The specified calendar doesn't matter.
    Timestamp expectedTs = Timestamp.from(Instant.parse("2025-06-03T11:59:21.822364Z"));
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp0").getTime());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp0", estCalendar).getTime());

    expectedTs = Timestamp.from(OffsetDateTime.parse("2024-02-13T07:40:30.822364-05:00").toInstant());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp1").getTime());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp1", estCalendar).getTime());

    expectedTs = Timestamp.from(OffsetDateTime.parse("2023-10-27T10:00:00+02:00").toInstant());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp2").getTime());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp2", estCalendar).getTime());

    // Verify timestamp without time zone information. The specified calendar matters here
    LocalDateTime localTime = LocalDateTime.parse("1760-06-03T11:59:21.822364");
    ZoneId estZone = ZoneId.of("America/New_York");
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp3").getTime());
    assertEquals(localTime.atZone(estZone).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp3", estCalendar).getTime());

    localTime = LocalDateTime.parse("2020-05-04T10:06:10.822364");
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp4").getTime());
    assertEquals(localTime.atZone(estZone).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp4", estCalendar).getTime());

    localTime = LocalDateTime.parse("2015-09-01T23:33:00");
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp5").getTime());
    assertEquals(localTime.atZone(estZone).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp5", estCalendar).getTime());

    localTime = LocalDateTime.parse("2019-03-15T00:00:00");
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp6").getTime());
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp6", estCalendar).getTime());

    expectedTs = Timestamp.from(Instant.parse("2024-08-01T10:30:20.822364Z"));
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp7").getTime());
    assertEquals(expectedTs.getTime(), rs.getTimestamp("fieldTimestamp7", estCalendar).getTime());

    localTime = LocalDateTime.parse("2025-04-01T21:55:21.822364");
    assertEquals(localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp8").getTime());
    assertEquals(localTime.atZone(estZone).toInstant().toEpochMilli(), rs.getTimestamp("fieldTimestamp8", estCalendar).getTime());

    assertNull(rs.getTimestamp("nonExistingField"));
  }

  @Test
  void test_parse_time() throws SQLException {
    // Timestamp string that is in ISO format with time zone information in UTC
    Map<String, Object> row = new HashMap<>();
    row.put("fieldTime0", Time.valueOf("18:45:20"));
    row.put("fieldTime1", Timestamp.from(Instant.parse("2024-08-01T10:30:20.822364Z")));
    row.put("fieldTime2", "10:30:00");
    row.put("fieldTime3", "11:59:21.822364");
    // Timestamp string that is in ISO format with time zone information
    row.put("fieldTime4", "10:00:00Z");
    row.put("fieldTime5", "05:30:00-02:00");
    row.put("fieldTime6", "08:25:10+02:00");
    // Timestamp string doesn't contain time zone information.
    row.put("fieldTime7", "2025-06-03T11:59:21.822364");
    row.put("fieldTime8", "1901-05-04 10:06:10.822364");
    row.put("fieldTime9", "2015-09-01 23:33:00");
    // Timestamp string doesn't contain time zone or HH:MM:SS information
    row.put("fieldTime10", "2019-03-15");
    List<Map<String, Object>> testTimes = Collections.singletonList(row);
    CachedResultSet cachedRs = new CachedResultSet(testTimes);
    assertTrue(cachedRs.next());
    verifyTimes(cachedRs);
    cachedRs.beforeFirst();
    String serialized_data = cachedRs.serializeIntoJsonString();
    ResultSet rs = CachedResultSet.deserializeFromJsonString(serialized_data);
    assertTrue(rs.next());
    verifyTimes(rs);
  }

  private void verifyTimes(ResultSet rs) throws SQLException {
    // Verifying the timestamp with time zone information. The specified calendar doesn't matter.
    assertEquals("18:45:20", rs.getTime("fieldTime0").toString());
    assertEquals("18:45:20", rs.getTime("fieldTime0", estCalendar).toString());

    // Convert from timestamp with time zone info
    assertEquals("03:30:20", rs.getTime("fieldTime1").toString());
    assertEquals("03:30:20", rs.getTime("fieldTime1", estCalendar).toString());

    // Verify timestamp without time zone information. The specified calendar matters here
    assertEquals("10:30:00", rs.getTime("fieldTime2").toString());
    assertEquals("10:30:00", rs.getTime("fieldTime2", estCalendar).toString()); // Should be 07:30:00

    assertEquals("11:59:21", rs.getTime("fieldTime3").toString());
    assertEquals("11:59:21", rs.getTime("fieldTime3", estCalendar).toString());

    assertEquals("10:00:00", rs.getTime("fieldTime4").toString());
    assertEquals("10:00:00", rs.getTime("fieldTime4", estCalendar).toString());

    assertEquals("05:30:00", rs.getTime("fieldTime5").toString());
    assertEquals("05:30:00", rs.getTime("fieldTime5", estCalendar).toString());

    assertEquals("08:25:10", rs.getTime("fieldTime6").toString());
    assertEquals("08:25:10", rs.getTime("fieldTime6", estCalendar).toString());

    assertEquals("11:59:21", rs.getTime("fieldTime7").toString());
    assertEquals("08:59:21", rs.getTime("fieldTime7", estCalendar).toString());

    assertEquals("10:06:10", rs.getTime("fieldTime8").toString());
    assertEquals("07:06:10", rs.getTime("fieldTime8", estCalendar).toString());

    assertEquals("23:33:00", rs.getTime("fieldTime9").toString());
    assertEquals("20:33:00", rs.getTime("fieldTime9", estCalendar).toString());

    assertEquals("00:00:00", rs.getTime("fieldTime10").toString());
    assertEquals("00:00:00", rs.getTime("fieldTime10", estCalendar).toString());

    assertNull(rs.getTime("nonExistingField"));
  }

  @Test
  void test_parse_date() throws SQLException {
    Map<String, Object> row = new HashMap<>();
    row.put("fieldDate0", Date.valueOf("2009-09-30"));
    row.put("fieldDate1", Timestamp.from(Instant.parse("2024-08-01T10:30:20.822364Z")));
    row.put("fieldDate2", "2012-10-01");
    row.put("fieldDate3", "1930-03-20T05:30:20.822364Z");
    // Timestamp string doesn't contain time zone information.
    row.put("fieldDate4", "2025-06-03T11:59:21.822364");
    row.put("fieldDate5", "1901-05-04 10:06:10.822364");
    row.put("fieldDate6", "2015-09-01 23:33:00");
    // Timestamp string doesn't contain time zone or HH:MM:SS information
    List<Map<String, Object>> testTimes = Collections.singletonList(row);
    CachedResultSet cachedRs = new CachedResultSet(testTimes);
    assertTrue(cachedRs.next());
    verifyDates(cachedRs);
    cachedRs.beforeFirst();
    String serialized_data = cachedRs.serializeIntoJsonString();
    ResultSet rs = CachedResultSet.deserializeFromJsonString(serialized_data);
    assertTrue(rs.next());
    verifyDates(rs);
  }

  private void verifyDates(ResultSet rs) throws SQLException {
    assertEquals("2009-09-30", rs.getDate("fieldDate0").toString());
    assertEquals("2009-09-30", rs.getDate("fieldDate0", estCalendar).toString());

    assertEquals("2024-08-01", rs.getDate("fieldDate1").toString());
    assertEquals("2024-08-01", rs.getDate("fieldDate1", estCalendar).toString());

    assertEquals("2012-10-01", rs.getDate("fieldDate2").toString());
    assertEquals("2012-10-01", rs.getDate("fieldDate2", estCalendar).toString());

    assertEquals("1930-03-19", rs.getDate("fieldDate3").toString());
    assertEquals("1930-03-19", rs.getDate("fieldDate3", estCalendar).toString());

    assertEquals("2025-06-03", rs.getDate("fieldDate4").toString());
    assertEquals("2025-06-03", rs.getDate("fieldDate4", estCalendar).toString());

    assertEquals("1901-05-04", rs.getDate("fieldDate5").toString());
    assertEquals("1901-05-04", rs.getDate("fieldDate5", estCalendar).toString());

    assertEquals("2015-09-01", rs.getDate("fieldDate6").toString());
    assertEquals("2015-09-01", rs.getDate("fieldDate6", estCalendar).toString());
  }
}
