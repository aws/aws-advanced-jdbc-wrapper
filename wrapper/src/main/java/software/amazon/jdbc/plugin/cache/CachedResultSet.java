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

package software.amazon.jdbc.plugin.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.WrapperUtils;

public class CachedResultSet implements ResultSet {

  public static class CachedRow {
    private final @Nullable Object[] rowData;
    final byte[] @Nullable [] rawData;

    public CachedRow(int numColumns) {
      rowData = new @Nullable Object[numColumns];
      rawData = new byte[numColumns][];
    }

    private void checkColumnIndex(final int columnIndex) throws SQLException {
      if (columnIndex < 1 || columnIndex > rowData.length) {
        throw new SQLException(Messages.get("CachedResultSet.invalidColumnIndex", new Object[]{columnIndex}));
      }
    }

    public void put(final int columnIndex, final @Nullable Object columnValue) throws SQLException {
      checkColumnIndex(columnIndex);
      rowData[columnIndex - 1] = columnValue;
    }

    public void putRaw(final int columnIndex, final byte[] rawColumnValue) throws SQLException {
      checkColumnIndex(columnIndex);
      rawData[columnIndex - 1] = rawColumnValue;
    }

    public @Nullable Object get(final int columnIndex) throws SQLException {
      checkColumnIndex(columnIndex);
      // De-serialize the data object from raw bytes if needed.
      if (rowData[columnIndex - 1] == null && rawData[columnIndex - 1] != null) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rawData[columnIndex - 1]);
             ObjectInputStream ois = new SafeObjectInputStream(bis)) {
          rowData[columnIndex - 1] = ois.readObject();
          rawData[columnIndex - 1] = null;
        } catch (ClassNotFoundException e) {
          throw new SQLException(Messages.get("CachedResultSet.classNotFoundDeserialize", new Object[]{columnIndex}),
              e);
        } catch (IOException e) {
          throw new SQLException(Messages.get("CachedResultSet.ioExceptionDeserialize", new Object[]{columnIndex}), e);
        }
      }
      return rowData[columnIndex - 1];
    }
  }

  /**
   * A restricted ObjectInputStream that only allows deserialization of known-safe classes.
   * This prevents Remote Code Execution via cache poisoning attacks where an attacker
   * injects malicious serialized objects (gadget chains) into the remote cache.
   *
   */
  private static class SafeObjectInputStream extends ObjectInputStream {
    private static final Set<String> ALLOWED_CLASSES;

    static {
      Set<String> allowed = new HashSet<>();

      // Internal cache serialization classes
      allowed.add("software.amazon.jdbc.plugin.cache.CachedResultSetMetaData");
      allowed.add("software.amazon.jdbc.plugin.cache.CachedResultSetMetaData$Field");
      allowed.add("[Lsoftware.amazon.jdbc.plugin.cache.CachedResultSetMetaData$Field;");
      allowed.add("software.amazon.jdbc.plugin.cache.CachedSQLXML");

      // Types with no useful base class for isAssignableFrom
      allowed.add("java.lang.String");
      allowed.add("java.lang.Boolean");
      allowed.add("java.lang.Character");
      allowed.add("java.util.UUID");
      allowed.add("java.net.URL");
      allowed.add("java.net.URI");
      // Package-private JVM serialization proxy for java.time types; cannot be referenced by class literal
      allowed.add("java.time.Ser");

      // Primitive arrays
      allowed.add("[B");
      allowed.add("[I");
      allowed.add("[J");
      allowed.add("[D");
      allowed.add("[F");
      allowed.add("[S");
      allowed.add("[C");
      allowed.add("[Z");

      // Object arrays
      allowed.add("[Ljava.lang.Object;");
      allowed.add("[Ljava.lang.String;");
      allowed.add("[Ljava.lang.Integer;");
      allowed.add("[Ljava.lang.Long;");
      allowed.add("[Ljava.lang.Double;");
      allowed.add("[Ljava.lang.Float;");
      allowed.add("[Ljava.lang.Short;");
      allowed.add("[Ljava.lang.Byte;");
      allowed.add("[Ljava.lang.Boolean;");
      allowed.add("[Ljava.math.BigDecimal;");

      ALLOWED_CLASSES = Collections.unmodifiableSet(allowed);
    }

    SafeObjectInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {
      String className = desc.getName();
      if (ALLOWED_CLASSES.contains(className)) {
        return super.resolveClass(desc);
      }
      // Only load classes from standard Java/javax packages for dynamic hierarchy checks.
      // Never call super.resolveClass for third-party or application classes — loading them
      // before throwing gives the JVM a window to invoke readObject() on a partially
      // constructed object in some implementations.
      if (className.startsWith("java.") || className.startsWith("javax.")) {
        Class<?> cls = super.resolveClass(desc);
        // Supported data types for deserialization. To support additional JDBC types
        // (e.g. CLOB, BLOB, Array), add the corresponding base class or interface here.
        if (Number.class.isAssignableFrom(cls)
            || Collection.class.isAssignableFrom(cls)
            || Map.class.isAssignableFrom(cls)
            || java.util.Date.class.isAssignableFrom(cls)
            || RowId.class.isAssignableFrom(cls)
            || SQLXML.class.isAssignableFrom(cls)
            || Temporal.class.isAssignableFrom(cls)
            || TemporalAmount.class.isAssignableFrom(cls)
            || ZoneId.class.isAssignableFrom(cls)) {
          return cls;
        }
      }
      // Allow user-registered third-party classes and packages (via Driver.skipWrappingForType
      // or Driver.skipWrappingForPackage). See security note in UsingTheJdbcDriver.md.
      if (WrapperUtils.skipWrappingForClasses.stream().anyMatch(c -> c.getName().equals(className))
          || WrapperUtils.skipWrappingForPackages.stream().anyMatch(p -> className.startsWith(p + "."))) {
        return super.resolveClass(desc);
      }
      throw new ClassNotFoundException(
          Messages.get("CachedResultSet.blockedDeserialization", new Object[]{className}));
    }
  }

  protected ArrayList<CachedRow> rows;
  protected int currentRow;
  protected boolean wasNullFlag;
  private final CachedResultSetMetaData metadata;
  protected static final ZoneId defaultTimeZoneId = ZoneId.systemDefault();
  protected static final TimeZone defaultTimeZone = TimeZone.getDefault();
  private final HashMap<String, Integer> columnNames;
  private volatile boolean closed;

  /**
   * Create a CachedResultSet out of the original ResultSet queried from the database.
   *
   * @param resultSet The ResultSet queried from the underlying database (not a CachedResultSet).
   * @throws SQLException if an error occurs while reading the ResultSet metadata or rows
   */
  public CachedResultSet(final ResultSet resultSet) throws SQLException {
    ResultSetMetaData srcMetadata = resultSet.getMetaData();
    final int numColumns = srcMetadata.getColumnCount();
    CachedResultSetMetaData.Field[] fields = new CachedResultSetMetaData.Field[numColumns];
    for (int i = 0; i < numColumns; i++) {
      fields[i] = new CachedResultSetMetaData.Field(srcMetadata, i + 1);
    }
    metadata = new CachedResultSetMetaData(fields);
    rows = new ArrayList<>();
    this.columnNames = new HashMap<>();
    for (int i = 1; i <= numColumns; i++) {
      this.columnNames.put(srcMetadata.getColumnLabel(i), i);
    }
    while (resultSet.next()) {
      final CachedRow row = new CachedRow(numColumns);
      for (int i = 1; i <= numColumns; ++i) {
        Object rowObj = resultSet.getObject(i);
        // For SQLXML object, convert into CachedSQLXML object that is serializable
        if (rowObj instanceof SQLXML) {
          rowObj = new CachedSQLXML(((SQLXML) rowObj).getString());
        }
        row.put(i, rowObj);
      }
      rows.add(row);
    }
    currentRow = -1;
    closed = false;
    wasNullFlag = false;
  }

  private CachedResultSet(final CachedResultSetMetaData md, final ArrayList<CachedRow> resultRows) throws SQLException {
    int numColumns = md.getColumnCount();
    this.columnNames = new HashMap<>();
    for (int i = 1; i <= numColumns; i++) {
      this.columnNames.put(md.getColumnLabel(i), i);
    }
    currentRow = -1;
    rows = resultRows;
    metadata = md;
    closed = false;
    wasNullFlag = false;
  }

  // Serialize the content of metadata and data rows for the current CachedResultSet into a byte array
  public byte[] serializeIntoByteArray() throws SQLException {
    // Serialize the metadata and then the rows
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream output = new ObjectOutputStream(baos)) {
      output.writeObject(metadata);
      output.writeInt(rows.size());
      int numColumns = metadata.getColumnCount();
      while (this.next()) {
        // serialize individual column fields in each row
        CachedRow row = rows.get(currentRow);
        for (int i = 0; i < numColumns; i++) {
          try (ByteArrayOutputStream objBytes = new ByteArrayOutputStream();
               ObjectOutputStream objStream = new ObjectOutputStream(objBytes)) {
            objStream.writeObject(row.get(i + 1));
            objStream.flush();
            byte[] dataByteArray = objBytes.toByteArray();
            int serializedLength = dataByteArray.length;
            output.writeInt(serializedLength);
            output.write(dataByteArray, 0, serializedLength);
          }
        }
      }
      output.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SQLException(Messages.get("CachedResultSet.serializationError"), e);
    }
  }

  /**
   * Form a ResultSet from the raw data from the cache server. Each of the column objects are stored as
   * raw bytes and the actual de-serialization into Java objects will happen lazily upon access later on.
   *
   * @param data the serialized byte array to deserialize
   * @return a ResultSet reconstructed from the byte array
   * @throws SQLException if deserialization fails
   */
  public static ResultSet deserializeFromByteArray(byte[] data) throws SQLException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
         ObjectInputStream ois = new SafeObjectInputStream(bis)) {
      CachedResultSetMetaData metadata = (CachedResultSetMetaData) ois.readObject();
      int numRows = ois.readInt();
      int numColumns = metadata.getColumnCount();
      ArrayList<CachedRow> resultRows = new ArrayList<>(numRows);
      for (int i = 0; i < numRows; i++) {
        // Store the raw bytes for each column object in CachedRow
        final CachedRow row = new CachedRow(numColumns);
        for (int j = 0; j < numColumns; j++) {
          int nextObjSize = ois.readInt(); // The size of the next serialized object in its raw bytes form
          byte[] objData = new byte[nextObjSize];
          int lengthRead = 0;
          while (lengthRead < nextObjSize) {
            int bytesRead = ois.read(objData, lengthRead, nextObjSize - lengthRead);
            if (bytesRead == -1) {
              throw new SQLException(Messages.get("CachedResultSet.endOfStream"));
            }
            lengthRead += bytesRead;
          }
          row.putRaw(j + 1, objData);
        }
        resultRows.add(row);
      }
      return new CachedResultSet(metadata, resultRows);
    } catch (ClassNotFoundException | ClassCastException e) {
      throw new SQLException(Messages.get("CachedResultSet.classNotFoundDeserializeResultSet"), e);
    } catch (IOException e) {
      throw new SQLException(Messages.get("CachedResultSet.ioExceptionDeserializeResultSet"), e);
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (rows.isEmpty()) {
      return false;
    }
    if (this.currentRow >= rows.size() - 1) {
      afterLast();
      return false;
    }
    currentRow++;
    return true;
  }

  @Override
  public void close() throws SQLException {
    currentRow = rows.size() - 1;
    closed = true;
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (isClosed()) {
      throw new SQLException(Messages.get("CachedResultSet.resultSetClosed"));
    }
    return this.wasNullFlag;
  }

  @Override
  public @Nullable String getString(final int columnIndex) throws SQLException {
    Object value = checkAndGetColumnValue(columnIndex);
    if (value == null) {
      return null;
    }
    return value.toString();
  }

  @Override
  public @Nullable String getString(final String columnLabel) throws SQLException {
    return getString(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return false;
    }
    if (val instanceof Boolean) {
      return (Boolean) val;
    }
    if (val instanceof Number) {
      return ((Number) val).intValue() != 0;
    }
    return Boolean.parseBoolean(val.toString());
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException {
    return getBoolean(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public byte getByte(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Byte) {
      return (Byte) val;
    }
    if (val instanceof Number) {
      return ((Number) val).byteValue();
    }
    return Byte.parseByte(val.toString());
  }

  @Override
  public byte getByte(final String columnLabel) throws SQLException {
    return getByte(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Short) {
      return (Short) val;
    }
    if (val instanceof Number) {
      return ((Number) val).shortValue();
    }
    return Short.parseShort(val.toString());
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException {
    return getShort(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException {
    return getInt(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Integer) {
      return (Integer) val;
    }
    if (val instanceof Number) {
      return ((Number) val).intValue();
    }
    return Integer.parseInt(val.toString());
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Long) {
      return (Long) val;
    }
    if (val instanceof Number) {
      return ((Number) val).longValue();
    }
    return Long.parseLong(val.toString());
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException {
    return getLong(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public float getFloat(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Float) {
      return (Float) val;
    }
    if (val instanceof Number) {
      return ((Number) val).floatValue();
    }
    return Float.parseFloat(val.toString());
  }

  @Override
  public float getFloat(final String columnLabel) throws SQLException {
    return getFloat(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public double getDouble(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return 0;
    }
    if (val instanceof Double) {
      return (Double) val;
    }
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    }
    return Double.parseDouble(val.toString());
  }

  @Override
  public double getDouble(final String columnLabel) throws SQLException {
    return getDouble(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  @Deprecated
  // "return": delegates to the possibly-null int-index getBigDecimal; the deprecated
  // getBigDecimal(String, int) contract is annotated non-null in the JDBC stubs.
  @SuppressWarnings({"deprecation", "return"})
  public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
    return getBigDecimal(checkAndGetColumnIndex(columnLabel), scale);
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public @Nullable BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    if (val instanceof Number) {
      return new BigDecimal(((Number) val).doubleValue()).setScale(scale, RoundingMode.HALF_UP);
    }
    return new BigDecimal(Double.parseDouble(val.toString())).setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public @Nullable BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    if (val instanceof Number) {
      return BigDecimal.valueOf(((Number) val).doubleValue());
    }
    return new BigDecimal(val.toString());
  }

  @Override
  public @Nullable BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    return getBigDecimal(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public byte @Nullable [] getBytes(final int columnIndex) throws SQLException {
    final Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof byte[]) {
      return (byte[]) val;
    }
    // Convert non-byte data to string, then to bytes (standard JDBC behavior)
    return val.toString().getBytes();
  }

  @Override
  public byte @Nullable [] getBytes(final String columnLabel) throws SQLException {
    return getBytes(checkAndGetColumnIndex(columnLabel));
  }

  private @Nullable Date convertToDate(final @Nullable Object dateObj, final @Nullable Calendar cal)
      throws SQLException {
    if (dateObj == null) {
      return null;
    }
    if (dateObj instanceof Date) {
      return (Date) dateObj;
    }
    if (dateObj instanceof Number) {
      return new Date(((Number) dateObj).longValue());
    }
    if (dateObj instanceof LocalDate) {
      // Convert the LocalDate for the specified time zone into Date representing
      // the same instant of time for the default time zone.
      LocalDate localDate = (LocalDate) dateObj;
      if (cal == null) {
        return Date.valueOf(localDate);
      }
      LocalDateTime localDateTime = localDate.atStartOfDay();
      ZonedDateTime originalZonedDateTime = localDateTime.atZone(cal.getTimeZone().toZoneId());
      ZonedDateTime targetZonedDateTime = originalZonedDateTime.withZoneSameInstant(defaultTimeZoneId);
      return Date.valueOf(targetZonedDateTime.toLocalDate());
    }
    if (dateObj instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) dateObj;
      long millis = timestamp.getTime();
      if (cal == null) {
        return new Date(millis);
      }
      long adjustedMillis = millis - cal.getTimeZone().getOffset(millis)
          + defaultTimeZone.getOffset(millis);
      return new Date(adjustedMillis);
    }

    // Note: normally the user should properly store the Date object in the DB column and
    // the underlying PG/MySQL/MariaDB driver would convert it into Date already in getObject()
    // prior to reaching this point in our caching logic. This is mainly to handle the case when the user
    // stores a generic string in the DB column and wants to convert this into Date. We try to do a
    // best-effort string parsing into Date with standard format "YYYY-MM-DD". The user is then
    // expected to handle parsing failure and implement custom logic to fetch this as String.
    return Date.valueOf(dateObj.toString());
  }

  @Override
  public @Nullable Date getDate(final int columnIndex) throws SQLException {
    // The value cached is the string representation of epoch time in milliseconds
    return convertToDate(checkAndGetColumnValue(columnIndex), null);
  }

  @Override
  public @Nullable Date getDate(final String columnLabel) throws SQLException {
    return getDate(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public @Nullable Date getDate(final int columnIndex, final @Nullable Calendar cal) throws SQLException {
    return convertToDate(checkAndGetColumnValue(columnIndex), cal);
  }

  @Override
  public @Nullable Date getDate(final String columnLabel, final @Nullable Calendar cal) throws SQLException {
    return getDate(checkAndGetColumnIndex(columnLabel), cal);
  }

  private @Nullable Time convertToTime(final @Nullable Object timeObj, final @Nullable Calendar cal)
      throws SQLException {
    if (timeObj == null) {
      return null;
    }
    if (timeObj instanceof Time) {
      return (Time) timeObj;
    }
    if (timeObj instanceof Number) {
      return new Time(((Number) timeObj).longValue());
    }
    if (timeObj instanceof LocalTime) {
      // Convert the LocalTime for the specified time zone into Time representing
      // the same instant of time for the default time zone.
      LocalTime localTime = (LocalTime) timeObj;
      if (cal == null) {
        return Time.valueOf(localTime);
      }
      LocalDateTime localDateTime = LocalDateTime.of(LocalDate.now(), localTime);
      ZonedDateTime originalZonedDateTime = localDateTime.atZone(cal.getTimeZone().toZoneId());
      ZonedDateTime targetZonedDateTime = originalZonedDateTime.withZoneSameInstant(defaultTimeZoneId);
      return Time.valueOf(targetZonedDateTime.toLocalTime());
    }
    if (timeObj instanceof OffsetTime) {
      OffsetTime offsetTime = (OffsetTime) timeObj;
      if (cal == null) {
        // Convert to default timezone using ZonedDateTime conversion
        ZonedDateTime zonedDateTime = offsetTime.atDate(LocalDate.now())
            .atZoneSameInstant(defaultTimeZoneId);
        return Time.valueOf(zonedDateTime.toLocalTime());
      } else {
        // Convert to specified calendar timezone
        ZonedDateTime zonedDateTime = offsetTime.atDate(LocalDate.now())
            .atZoneSameInstant(cal.getTimeZone().toZoneId());
        return Time.valueOf(zonedDateTime.toLocalTime());
      }
    }
    if (timeObj instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) timeObj;
      long millis = timestamp.getTime();
      if (cal == null) {
        return new Time(millis);
      }
      long adjustedMillis = millis - cal.getTimeZone().getOffset(millis)
          + defaultTimeZone.getOffset(millis);
      return new Time(adjustedMillis);
    }

    // Note: normally the user should properly store the Time object in the DB column and
    // the underlying PG/MySQL/MariaDB driver would convert it into Time already in getObject()
    // prior to reaching this point in our caching logic. This is mainly to handle the case when the user
    // stores a generic string in the DB column and wants to convert this into Time. We try to do a
    // best-effort string parsing into Time with standard format "HH:MM:SS". The user is then
    // expected to handle parsing failure and implement custom logic to fetch this as String.
    return Time.valueOf(timeObj.toString());
  }

  @Override
  public @Nullable Time getTime(final int columnIndex) throws SQLException {
    return convertToTime(checkAndGetColumnValue(columnIndex), null);
  }

  @Override
  public @Nullable Time getTime(final String columnLabel) throws SQLException {
    return getTime(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public @Nullable Time getTime(final int columnIndex, final @Nullable Calendar cal) throws SQLException {
    return convertToTime(checkAndGetColumnValue(columnIndex), cal);
  }

  @Override
  public @Nullable Time getTime(final String columnLabel, final @Nullable Calendar cal) throws SQLException {
    return getTime(checkAndGetColumnIndex(columnLabel), cal);
  }

  private @Nullable Timestamp convertToTimestamp(final @Nullable Object timestampObj,
      final @Nullable Calendar calendar) {
    if (timestampObj == null) {
      return null;
    }
    if (timestampObj instanceof Timestamp) {
      return (Timestamp) timestampObj;
    }
    if (timestampObj instanceof Number) {
      return new Timestamp(((Number) timestampObj).longValue());
    }
    if (timestampObj instanceof LocalDateTime) {
      // Convert LocalDateTime based on the specified calendar time zone info into a
      // Timestamp based on the JVM's default time zone representing the same instant
      long epochTimeInMillis;
      LocalDateTime localTime = (LocalDateTime) timestampObj;
      if (calendar != null) {
        epochTimeInMillis = localTime.atZone(calendar.getTimeZone().toZoneId()).toInstant().toEpochMilli();
      } else {
        epochTimeInMillis = localTime.atZone(defaultTimeZoneId).toInstant().toEpochMilli();
      }
      return new Timestamp(epochTimeInMillis);
    }
    if (timestampObj instanceof OffsetDateTime) {
      return Timestamp.from(((OffsetDateTime) timestampObj).toInstant());
    }
    if (timestampObj instanceof ZonedDateTime) {
      return Timestamp.from(((ZonedDateTime) timestampObj).toInstant());
    }

    // Note: normally the user should properly store the Timestamp/DateTime object in the DB column and
    // the underlying PG/MySQL/MariaDB driver would convert it into Timestamp already in getObject()
    // prior to reaching this point in our caching logic. This is mainly to handle the case when the user
    // stores a generic string in the DB column and wants to convert this into Timestamp. We try to do a
    // best-effort string parsing into Timestamp with standard format "YYYY-MM-DD HH:MM:SS". The user is
    // then expected to handle parsing failure and implement custom logic to fetch this as String.
    return Timestamp.valueOf(timestampObj.toString());
  }

  @Override
  public @Nullable Timestamp getTimestamp(final int columnIndex) throws SQLException {
    return convertToTimestamp(checkAndGetColumnValue(columnIndex), null);
  }

  @Override
  public @Nullable Timestamp getTimestamp(final String columnLabel) throws SQLException {
    return getTimestamp(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public @Nullable Timestamp getTimestamp(final int columnIndex, final @Nullable Calendar cal) throws SQLException {
    return convertToTimestamp(checkAndGetColumnValue(columnIndex), cal);
  }

  @Override
  public @Nullable Timestamp getTimestamp(final String columnLabel, final @Nullable Calendar cal)
      throws SQLException {
    return getTimestamp(checkAndGetColumnIndex(columnLabel), cal);
  }

  @Override
  public InputStream getAsciiStream(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getAsciiStream(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream getBinaryStream(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public @Nullable SQLWarning getWarnings() throws SQLException {
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
      throw new SQLException(Messages.get("CachedResultSet.rowIndexOutOfRange", new Object[]{this.currentRow}));
    }
  }

  @Override
  public @Nullable Object getObject(final int columnIndex) throws SQLException {
    checkCurrentRow();
    return checkAndGetColumnValue(columnIndex);
  }

  @Override
  public @Nullable Object getObject(final String columnLabel) throws SQLException {
    checkCurrentRow();
    return checkAndGetColumnValue(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public Object getObject(final int columnIndex, final Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getObject(final String columnLabel, final Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  // "return": the cached column value may be null; the generic return type T cannot be annotated
  // @Nullable, so Class.cast of a possibly-null value is suppressed here.
  @SuppressWarnings("return")
  public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
    return type.cast(getObject(columnIndex));
  }

  @Override
  // "return": the cached column value may be null; the generic return type T cannot be annotated
  // @Nullable, so Class.cast of a possibly-null value is suppressed here.
  @SuppressWarnings("return")
  public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
    return type.cast(getObject(columnLabel));
  }

  // Check the column index passed in is proper, and return the value of the column from the current row
  private @Nullable Object checkAndGetColumnValue(final int columnIndex) throws SQLException {
    if (columnIndex == 0 || columnIndex > this.columnNames.size()) {
      throw new SQLException(Messages.get("CachedResultSet.columnOutOfBounds"));
    }
    final CachedRow row = this.rows.get(this.currentRow);
    final Object val = row.get(columnIndex);
    this.wasNullFlag = (val == null);
    return val;
  }

  // Check column label exists and returns the column index corresponding to the column name
  private int checkAndGetColumnIndex(final String columnLabel) throws SQLException {
    final Integer colIndex = columnNames.get(columnLabel);
    if (colIndex == null) {
      throw new SQLException(Messages.get("CachedResultSet.columnNotFound", new Object[]{columnLabel}));
    }
    return colIndex;
  }

  @Override
  public int findColumn(final String columnLabel) throws SQLException {
    final Integer colIndex = columnNames.get(columnLabel);
    if (colIndex == null) {
      throw new SQLException(Messages.get("CachedResultSet.columnNotFoundInResultSet", new Object[]{columnLabel}));
    }
    return colIndex;
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
    if (this.currentRow >= 0 && this.currentRow < this.rows.size()) {
      return this.currentRow + 1;
    }
    return 0;
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
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNull(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(final int columnIndex, final byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateByte(final String columnLabel, final byte x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(final int columnIndex, final short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateShort(final String columnLabel, final short x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(final int columnIndex, final int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateInt(final String columnLabel, final int x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(final int columnIndex, final long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateLong(final String columnLabel, final long x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(final int columnIndex, final float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateFloat(final String columnLabel, final float x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(final int columnIndex, final double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDouble(final String columnLabel, final double x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(final int columnIndex, final @Nullable BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBigDecimal(final String columnLabel, final @Nullable BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(final int columnIndex, final @Nullable String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateString(final String columnLabel, final @Nullable String x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(final int columnIndex, final byte @Nullable [] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBytes(final String columnLabel, final byte @Nullable [] x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(final int columnIndex, final @Nullable Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDate(final String columnLabel, final @Nullable Date x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(final int columnIndex, final @Nullable Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTime(final String columnLabel, final @Nullable Time x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(final int columnIndex, final @Nullable Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateTimestamp(final String columnLabel, final @Nullable Timestamp x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final @Nullable InputStream x, final int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final @Nullable InputStream x, final int length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final @Nullable InputStream x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final @Nullable InputStream x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final int columnIndex, final @Nullable InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAsciiStream(final String columnLabel, final @Nullable InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final @Nullable InputStream x, final int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final @Nullable InputStream x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final @Nullable InputStream x, final int length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final @Nullable InputStream x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final String columnLabel, final @Nullable InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBinaryStream(final int columnIndex, final @Nullable InputStream x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final @Nullable Reader x, final int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final @Nullable Reader x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final @Nullable Reader reader, final int length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final int columnIndex, final @Nullable Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCharacterStream(final String columnLabel, final @Nullable Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(final int columnIndex, final @Nullable Object x, final int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(final int columnIndex, final @Nullable Object x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(final String columnLabel, final @Nullable Object x, final int scaleOrLength)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateObject(final String columnLabel, final @Nullable Object x) throws SQLException {
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
  public Ref getRef(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Ref getRef(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob getBlob(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob getClob(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(final int columnIndex) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Array getArray(final String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public @Nullable URL getURL(final int columnIndex) throws SQLException {
    Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof URL) {
      return (URL) val;
    }
    try {
      return new URL(val.toString());
    } catch (MalformedURLException e) {
      throw new SQLException(Messages.get("CachedResultSet.cannotExtractUrl", new Object[]{val}), e);
    }
  }

  @Override
  public @Nullable URL getURL(final String columnLabel) throws SQLException {
    return getURL(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public void updateRef(final int columnIndex, final @Nullable Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRef(final String columnLabel, final @Nullable Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final int columnIndex, final @Nullable Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final String columnLabel, final @Nullable Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final int columnIndex, final @Nullable InputStream inputStream, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final String columnLabel, final @Nullable InputStream inputStream, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final int columnIndex, final @Nullable InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateBlob(final String columnLabel, final @Nullable InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final int columnIndex, final @Nullable Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final String columnLabel, final @Nullable Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final int columnIndex, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final String columnLabel, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final int columnIndex, final @Nullable Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateClob(final String columnLabel, final @Nullable Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(final int columnIndex, final @Nullable Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateArray(final String columnLabel, final @Nullable Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public @Nullable RowId getRowId(final int columnIndex) throws SQLException {
    Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof RowId) {
      return (RowId) val;
    }
    throw new SQLException(Messages.get("CachedResultSet.cannotExtractRowId", new Object[]{val}));
  }

  @Override
  public @Nullable RowId getRowId(final String columnLabel) throws SQLException {
    return getRowId(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public void updateRowId(final int columnIndex, final @Nullable RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateRowId(final String columnLabel, final @Nullable RowId x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
  public void updateNString(final int columnIndex, final @Nullable String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
  public void updateNString(final String columnLabel, final @Nullable String nString) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
  public void updateNClob(final int columnIndex, final @Nullable NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings({"checkstyle:MethodName", "checkstyle:ParameterName"})
  public void updateNClob(final String columnLabel, final @Nullable NClob nClob) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(final int columnIndex, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(final String columnLabel, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(final int columnIndex, final @Nullable Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNClob(final String columnLabel, final @Nullable Reader reader) throws SQLException {
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
  public @Nullable SQLXML getSQLXML(final int columnIndex) throws SQLException {
    Object val = checkAndGetColumnValue(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof SQLXML) {
      return (SQLXML) val;
    }
    return new CachedSQLXML(val.toString());
  }

  @Override
  public @Nullable SQLXML getSQLXML(final String columnLabel) throws SQLException {
    return getSQLXML(checkAndGetColumnIndex(columnLabel));
  }

  @Override
  public void updateSQLXML(final int columnIndex, final @Nullable SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSQLXML(final String columnLabel, final @Nullable SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public @Nullable String getNString(final int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  @Override
  public @Nullable String getNString(final String columnLabel) throws SQLException {
    return getString(columnLabel);
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
  public void updateNCharacterStream(final int columnIndex, final @Nullable Reader x, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final @Nullable Reader reader, final long length)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(final int columnIndex, final @Nullable Reader x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateNCharacterStream(final String columnLabel, final @Nullable Reader reader) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(this.getClass())) {
      return iface.cast(this);
    } else {
      throw new SQLException(Messages.get("CachedResultSet.cannotUnwrap", new Object[]{iface.getName()}));
    }
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface != null && iface.isAssignableFrom(this.getClass());
  }
}
