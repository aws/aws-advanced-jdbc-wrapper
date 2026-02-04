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

package software.amazon.jdbc.plugin.encryption.wrapper;

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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.parser.EncryptionAnnotationParser;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;

/**
 * A PreparedStatement wrapper that automatically encrypts parameter values for columns configured
 * for encryption. Uses delegation pattern for non-encrypted operations.
 */
public class EncryptingPreparedStatement implements PreparedStatement {

  private static final Logger LOGGER =
      Logger.getLogger(EncryptingPreparedStatement.class.getName());

  private final PreparedStatement delegate;
  private final MetadataManager metadataManager;
  private final EncryptionService encryptionService;
  private final KeyManager keyManager;
  private final SqlAnalysisService sqlAnalysisService;
  private final String sql;
  private final String cleanSql;

  // Cache for parameter index to column name mapping
  private final Map<Integer, String> parameterColumnMapping = new ConcurrentHashMap<>();
  private String tableName;
  private boolean mappingInitialized = false;

  public EncryptingPreparedStatement(
      PreparedStatement delegate,
      MetadataManager metadataManager,
      EncryptionService encryptionService,
      KeyManager keyManager,
      SqlAnalysisService sqlAnalysisService,
      String sql) {
    LOGGER.finest(() -> String.format("EncryptingPreparedStatement created for SQL: %s", sql));

    // Parse annotations from original SQL, then strip them
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);
    this.cleanSql = EncryptionAnnotationParser.stripAnnotations(sql);

    this.delegate = delegate;
    this.metadataManager = metadataManager;
    this.encryptionService = encryptionService;
    this.keyManager = keyManager;
    this.sqlAnalysisService = sqlAnalysisService;
    this.sql = cleanSql;

    // Initialize parameter mapping from SQL analysis first
    parseSqlForEncryptedColumns();

    // Override with explicit annotations (highest priority)
    parameterColumnMapping.putAll(annotations);

    LOGGER.finest(() -> String.format("Parameter mapping initialized: %s", parameterColumnMapping));
  }

  /** Initializes parameter mapping using SQL analysis service. */
  private void parseSqlForEncryptedColumns() {
    LOGGER.finest(() -> String.format("initializeParameterMapping called for SQL: %s", sql));
    try {
      // Use SqlAnalysisService to analyze SQL and extract table information
      SqlAnalysisService.SqlAnalysisResult analysisResult = SqlAnalysisService.analyzeSql(sql);
      LOGGER.finest(
          () -> String.format("Analysis result tables: %s", analysisResult.getAffectedTables()));

      // Get the first table from analysis results
      if (!analysisResult.getAffectedTables().isEmpty()) {
        this.tableName = analysisResult.getAffectedTables().iterator().next();
        LOGGER.finest(() -> String.format("Table name set to: %s", tableName));

        // Use SqlAnalysisService to get parameter mapping
        Map<Integer, String> mapping = sqlAnalysisService.getColumnParameterMapping(sql);
        LOGGER.finest(() -> String.format("Column parameter mapping from service: %s", mapping));
        parameterColumnMapping.putAll(mapping);

        LOGGER.finest(() -> String.format("Final parameter mapping: %s", parameterColumnMapping));
      }

      mappingInitialized = true;
      LOGGER.finest(
          () ->
              String.format("Parameter mapping initialization complete for table: %s", tableName));

    } catch (Exception e) {
      LOGGER.finest(
          () -> String.format("Failed to initialize parameter mapping: %s", e.getMessage()));
      LOGGER.finest(() -> String.format("Exception details %s", e));
      mappingInitialized = false;
    }
  }

  /** Gets the column name for a parameter index. */
  private String getColumnNameForParameter(int parameterIndex) {
    return parameterColumnMapping.get(parameterIndex);
  }

  /** Checks if a parameter should be encrypted and encrypts it if necessary. */
  private Object encryptParameterIfNeeded(int parameterIndex, Object value) throws SQLException {
    LOGGER.finest(
        () ->
            String.format(
                "encryptParameterIfNeeded called: param=%s, value=%s", parameterIndex, value));
    LOGGER.finest(
        () -> String.format("mappingInitialized=%s, tableName=%s", mappingInitialized, tableName));

    if (!mappingInitialized || tableName == null || value == null) {
      LOGGER.finest(() -> "Skipping encryption - early exit");
      return value;
    }

    try {
      String columnName = getColumnNameForParameter(parameterIndex);
      LOGGER.finest(
          () -> String.format("Parameter %s maps to column: %s", parameterIndex, columnName));
      LOGGER.finest(() -> String.format("Parameter mapping: %s", parameterColumnMapping));

      if (columnName == null) {
        return value;
      }

      // Check if column is configured for encryption
      boolean isEncrypted = metadataManager.isColumnEncrypted(tableName, columnName);
      LOGGER.finest(
          () -> String.format("Column %s.%s encrypted: %s", tableName, columnName, isEncrypted));

      // Debug metadata manager state
      try {
        LOGGER.finest(() -> String.format("Checking metadata manager for table: %s", tableName));
        LOGGER.finest(
            () -> String.format("MetadataManager class: %s", metadataManager.getClass().getName()));

        // Force refresh metadata to pick up any new configurations
        LOGGER.finest(() -> String.format("Forcing metadata refresh..."));
        metadataManager.refreshMetadata();
        LOGGER.finest(() -> String.format("Metadata refresh completed"));

        // Try to get config directly after refresh
        ColumnEncryptionConfig config = metadataManager.getColumnConfig(tableName, columnName);
        LOGGER.finest(
            () ->
                String.format(
                    "Column config for %s.%s after refresh: %s", tableName, columnName, config));

        // Check encryption status after refresh
        boolean isEncryptedAfterRefresh = metadataManager.isColumnEncrypted(tableName, columnName);
        LOGGER.finest(
            () ->
                String.format(
                    "Column %s.%s encrypted after refresh: %s",
                    tableName, columnName, isEncryptedAfterRefresh));

      } catch (Exception e) {
        LOGGER.finest(() -> String.format("Error getting column config: %s", e.getMessage()));
        LOGGER.finest(() -> String.format("Exception details", e));
      }

      if (!isEncrypted) {
        return value;
      }

      // Get encryption configuration
      ColumnEncryptionConfig config = metadataManager.getColumnConfig(tableName, columnName);
      if (config == null) {
        LOGGER.warning(
            () ->
                String.format(
                    "No encryption config found for column %s.%s", tableName, columnName));
        return value;
      }

      // Get data key for encryption
      byte[] dataKey =
          keyManager.decryptDataKey(
              config.getKeyMetadata().getEncryptedDataKey(),
              config.getKeyMetadata().getMasterKeyArn());

      // Get HMAC key
      byte[] hmacKey = config.getKeyMetadata().getHmacKey();

      LOGGER.info(
          () ->
              String.format(
                  "EncryptingPreparedStatement: param=%d, column=%s.%s, hmacKey=%s",
                  parameterIndex,
                  tableName,
                  columnName,
                  java.util.Base64.getEncoder().encodeToString(hmacKey)));

      // Encrypt the value
      byte[] encryptedValue =
          encryptionService.encrypt(value, dataKey, hmacKey, config.getAlgorithm());

      // Clear the data key from memory
      java.util.Arrays.fill(dataKey, (byte) 0);

      LOGGER.fine(
          () ->
              String.format(
                  "Encrypted parameter %s for column %s.%s",
                  parameterIndex, tableName, columnName));
      return encryptedValue;

    } catch (Exception e) {
      // TODO move this into the subscriber
      String errorMsg =
          String.format(
              "Failed to encrypt parameter %d for column %s.%s",
              parameterIndex, tableName, getColumnNameForParameter(parameterIndex));
      LOGGER.severe(() -> String.format(errorMsg));
      throw new SQLException(errorMsg, e);
    }
  }

  // Override setXXX methods to add encryption logic

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setObject(parameterIndex, new EncryptedData((byte[]) encryptedValue));
    } else {
      delegate.setString(parameterIndex, (String) encryptedValue);
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setInt(parameterIndex, (Integer) encryptedValue);
    }
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setLong(parameterIndex, (Long) encryptedValue);
    }
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setDouble(parameterIndex, (Double) encryptedValue);
    }
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setFloat(parameterIndex, (Float) encryptedValue);
    }
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setBoolean(parameterIndex, (Boolean) encryptedValue);
    }
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setShort(parameterIndex, (Short) encryptedValue);
    }
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setByte(parameterIndex, (Byte) encryptedValue);
    }
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setBigDecimal(parameterIndex, (BigDecimal) encryptedValue);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setDate(parameterIndex, (Date) encryptedValue);
    }
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setDate(parameterIndex, (Date) encryptedValue, cal);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setTime(parameterIndex, (Time) encryptedValue);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setTime(parameterIndex, (Time) encryptedValue, cal);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setTimestamp(parameterIndex, (Timestamp) encryptedValue);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setTimestamp(parameterIndex, (Timestamp) encryptedValue, cal);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[] && !(x instanceof byte[])) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setObject(parameterIndex, encryptedValue);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[] && !(x instanceof byte[])) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setObject(parameterIndex, encryptedValue, targetSqlType);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, x);
    if (encryptedValue instanceof byte[] && !(x instanceof byte[])) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setObject(parameterIndex, encryptedValue, targetSqlType, scaleOrLength);
    }
  }

  // Null setters - no encryption needed
  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    delegate.setNull(parameterIndex, sqlType);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    delegate.setNull(parameterIndex, sqlType, typeName);
  }

  // Stream and reader setters - delegate directly (encryption not supported for streams)
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    delegate.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    delegate.setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    delegate.setBinaryStream(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    delegate.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    delegate.setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    delegate.setAsciiStream(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    delegate.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    delegate.setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    delegate.setCharacterStream(parameterIndex, reader);
  }

  // Other specialized setters - delegate directly
  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    delegate.setURL(parameterIndex, x);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    delegate.setRef(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    delegate.setBlob(parameterIndex, x);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    delegate.setBlob(parameterIndex, inputStream, length);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    delegate.setBlob(parameterIndex, inputStream);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    delegate.setClob(parameterIndex, x);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    delegate.setClob(parameterIndex, reader, length);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    delegate.setClob(parameterIndex, reader);
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    delegate.setArray(parameterIndex, x);
  }

  // Deprecated methods - delegate directly
  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    delegate.setUnicodeStream(parameterIndex, x, length);
  }

  // JDBC 4.0+ methods
  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    delegate.setRowId(parameterIndex, x);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    Object encryptedValue = encryptParameterIfNeeded(parameterIndex, value);
    if (encryptedValue instanceof byte[]) {
      delegate.setBytes(parameterIndex, (byte[]) encryptedValue);
    } else {
      delegate.setNString(parameterIndex, (String) encryptedValue);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    delegate.setNCharacterStream(parameterIndex, value, length);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    delegate.setNCharacterStream(parameterIndex, value);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    delegate.setNClob(parameterIndex, value);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    delegate.setNClob(parameterIndex, reader, length);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    delegate.setNClob(parameterIndex, reader);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    delegate.setSQLXML(parameterIndex, xmlObject);
  }

  // All other PreparedStatement methods delegate directly to the wrapped statement

  @Override
  public ResultSet executeQuery() throws SQLException {
    return delegate.executeQuery();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return delegate.executeQuery(sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    return delegate.executeUpdate();
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    return delegate.executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return delegate.executeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return delegate.executeUpdate(sql, columnIndexes);
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return delegate.executeUpdate(sql, columnNames);
  }

  @Override
  public boolean execute() throws SQLException {
    return delegate.execute();
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    return delegate.execute(sql);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return delegate.execute(sql, autoGeneratedKeys);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return delegate.execute(sql, columnIndexes);
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return delegate.execute(sql, columnNames);
  }

  @Override
  public void addBatch() throws SQLException {
    delegate.addBatch();
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    delegate.addBatch(sql);
  }

  @Override
  public void clearParameters() throws SQLException {
    delegate.clearParameters();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return delegate.getMetaData();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return delegate.getParameterMetaData();
  }

  // Statement methods - delegate to wrapped statement

  @Override
  public void close() throws SQLException {
    delegate.close();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return delegate.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    delegate.setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() throws SQLException {
    return delegate.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    delegate.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    delegate.setEscapeProcessing(enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return delegate.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    delegate.setQueryTimeout(seconds);
  }

  @Override
  public void cancel() throws SQLException {
    delegate.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return delegate.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    delegate.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    delegate.setCursorName(name);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return delegate.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return delegate.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return delegate.getMoreResults();
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return delegate.getMoreResults(current);
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    delegate.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return delegate.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    delegate.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return delegate.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return delegate.getResultSetConcurrency();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return delegate.getResultSetType();
  }

  @Override
  public void clearBatch() throws SQLException {
    delegate.clearBatch();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    return delegate.executeBatch();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return delegate.getConnection();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return delegate.getGeneratedKeys();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return delegate.getResultSetHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return delegate.isClosed();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    delegate.setPoolable(poolable);
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return delegate.isPoolable();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    delegate.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return delegate.isCloseOnCompletion();
  }

  // Wrapper interface methods

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass()) || delegate.isWrapperFor(iface);
  }
}
