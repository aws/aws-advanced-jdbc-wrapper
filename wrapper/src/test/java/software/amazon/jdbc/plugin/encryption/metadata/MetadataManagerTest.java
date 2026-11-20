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

package software.amazon.jdbc.plugin.encryption.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.EncryptionSql;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class MetadataManagerTest {

  private static final Instant METADATA_CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant METADATA_UPDATED_AT = Instant.parse("2026-01-02T00:00:00Z");
  private static final Instant KEY_CREATED_AT = Instant.parse("2026-01-03T00:00:00Z");
  private static final Instant KEY_LAST_USED_AT = Instant.parse("2026-01-04T00:00:00Z");
  private static final byte[] HMAC_KEY = {1, 2, 3, 4, (byte) 0xfe, (byte) 0xff};

  private AutoCloseable closeable;
  private MetadataManager manager;

  @Mock Connection mockConnection;
  @Mock PreparedStatement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock DatabaseMetaData mockMetaData;
  @Mock Statement mockPlainStatement;
  @Mock PluginService mockPluginService;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (manager != null) {
      manager.shutdown();
    }
    closeable.close();
  }

  private MetadataManager createManager(boolean cacheEnabled) {
    manager = new MetadataManager(mockConnection, config(cacheEnabled));
    return manager;
  }

  private static EncryptionConfig config(boolean cacheEnabled) {
    return EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .cacheEnabled(cacheEnabled)
        .metadataRefreshInterval(Duration.ZERO)
        .build();
  }

  private MetadataManager createPluginServiceManager(TargetDriverDialect dialect) throws SQLException {
    when(mockPluginService.getTargetDriverDialect()).thenReturn(dialect);
    when(mockPluginService.forceConnect(any(), any())).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockPlainStatement);
    manager = new MetadataManager(mockPluginService, config(false));
    return manager;
  }

  private void reportProduct(String productName) throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getDatabaseProductName()).thenReturn(productName);
  }

  /** Runs every query that has a comparison, and returns the SQL that was prepared. */
  private List<String> runComparingQueries(MetadataManager mm) throws Exception {
    mm.loadEncryptionMetadata();
    mm.isColumnEncrypted("users", "ssn");
    mm.hasEncryptedColumns("users");
    mm.getColumnConfig("users", "ssn");
    final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection, times(4)).prepareStatement(sql.capture());
    return sql.getAllValues();
  }

  /** One joined encryption_metadata and key_storage row, keyed by result set column label. */
  private static Map<String, Object> columnConfigRow() {
    final Map<String, Object> row = new HashMap<>();
    row.put("table_name", "users");
    row.put("column_name", "ssn");
    row.put("encryption_algorithm", "AES-256-GCM");
    row.put("key_id", 7);
    row.put("created_at", Timestamp.from(METADATA_CREATED_AT));
    row.put("updated_at", Timestamp.from(METADATA_UPDATED_AT));
    row.put("name", "users-ssn-key");
    row.put("master_key_arn", "arn:aws:kms:us-east-1:123456789012:key/abc");
    row.put("encrypted_data_key", "ZW5jcnlwdGVkLWtleQ==");
    row.put("hmac_key", HMAC_KEY.clone());
    row.put("key_spec", "AES_256");
    row.put("key_created_at", Timestamp.from(KEY_CREATED_AT));
    row.put("last_used_at", Timestamp.from(KEY_LAST_USED_AT));
    return row;
  }

  /**
   * Makes each prepared statement return a result set holding {@code row}, readable only through
   * the column labels its SQL selects. Reading any other label throws, as a real driver does.
   */
  private void answerQueriesWith(final Map<String, Object> row) throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenAnswer(invocation -> {
      final Set<String> labels = selectedLabels(invocation.getArgument(0));
      final PreparedStatement stmt = mock(PreparedStatement.class);
      when(stmt.executeQuery()).thenAnswer(query -> singleRowResultSet(row, labels));
      return stmt;
    });
  }

  /** The result set column labels of a {@code SELECT a.x, b.y as z FROM ...} statement. */
  private static Set<String> selectedLabels(final String sql) {
    final String selectList = sql.substring("SELECT ".length(), sql.indexOf(" FROM "));
    final Set<String> labels = new HashSet<>();
    for (String item : selectList.split(",")) {
      final String column = item.trim();
      final int alias = column.toLowerCase(Locale.ROOT).lastIndexOf(" as ");
      labels.add(alias >= 0
          ? column.substring(alias + " as ".length()).trim()
          : column.substring(column.indexOf('.') + 1));
    }
    return labels;
  }

  private static ResultSet singleRowResultSet(final Map<String, Object> row, final Set<String> labels) {
    final boolean[] beforeRow = {true};
    return (ResultSet) Proxy.newProxyInstance(
        MetadataManagerTest.class.getClassLoader(),
        new Class<?>[] {ResultSet.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "next":
              final boolean hasRow = beforeRow[0];
              beforeRow[0] = false;
              return hasRow;
            case "close":
              return null;
            case "toString":
              return "ResultSet" + labels;
            case "getString":
            case "getInt":
            case "getBytes":
            case "getTimestamp":
              final String label = (String) args[0];
              if (!labels.contains(label)) {
                throw new SQLException("The column name " + label + " was not found in this ResultSet.");
              }
              return row.get(label);
            default:
              throw new UnsupportedOperationException(method.getName());
          }
        });
  }

  private static void assertColumnConfigFromRow(final @Nullable ColumnEncryptionConfig config) {
    assertNotNull(config);
    assertEquals("users", config.getTableName());
    assertEquals("ssn", config.getColumnName());
    assertEquals("AES-256-GCM", config.getAlgorithm());
    assertEquals(7, config.getKeyId());
    assertEquals(METADATA_CREATED_AT, config.getCreatedAt());
    assertEquals(METADATA_UPDATED_AT, config.getUpdatedAt());

    final KeyMetadata key = config.getKeyMetadata();
    assertEquals(7, key.getKeyId());
    assertEquals("users-ssn-key", key.getKeyName());
    assertEquals("arn:aws:kms:us-east-1:123456789012:key/abc", key.getMasterKeyArn());
    assertEquals("ZW5jcnlwdGVkLWtleQ==", key.getEncryptedDataKey());
    assertArrayEquals(HMAC_KEY, key.getHmacKey());
    assertEquals("AES_256", key.getKeySpec());
    assertEquals(KEY_CREATED_AT, key.getCreatedAt());
    assertEquals(KEY_LAST_USED_AT, key.getLastUsedAt());
  }

  @Test
  void cachedAndDirectLookups_returnTheSameColumnConfig() throws Exception {
    answerQueriesWith(columnConfigRow());

    final MetadataManager cached = createManager(true);
    cached.initialize();
    final ColumnEncryptionConfig fromCache = cached.getColumnConfig("users", "ssn");

    final MetadataManager direct = new MetadataManager(mockConnection, config(false));
    final ColumnEncryptionConfig fromDatabase;
    try {
      fromDatabase = direct.getColumnConfig("users", "ssn");
    } finally {
      direct.shutdown();
    }

    // One query loads the cache and one answers the direct lookup.
    final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection, times(2)).prepareStatement(sql.capture());
    assertTrue(sql.getAllValues().get(0).endsWith("ORDER BY em.table_name, em.column_name"));
    assertTrue(sql.getAllValues().get(1).endsWith("WHERE em.table_name = ? AND em.column_name = ?"));

    // Compared field by field: ColumnEncryptionConfig.equals and KeyMetadata.equals skip the key
    // name and HMAC key.
    assertColumnConfigFromRow(fromCache);
    assertColumnConfigFromRow(fromDatabase);
  }

  @Test
  void staleCache_looksUpTheSameColumnConfigFromTheDatabase() throws Exception {
    answerQueriesWith(columnConfigRow());
    // A zero expiration leaves the cache stale as soon as it is loaded.
    manager = new MetadataManager(mockConnection, EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .cacheEnabled(true)
        .cacheExpirationMinutes(0)
        .metadataRefreshInterval(Duration.ZERO)
        .build());
    manager.initialize();
    assertEquals(1, manager.getCacheSize());

    final ColumnEncryptionConfig config = manager.getColumnConfig("users", "ssn");

    final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(mockConnection, times(2)).prepareStatement(sql.capture());
    assertTrue(sql.getAllValues().get(1).endsWith("WHERE em.table_name = ? AND em.column_name = ?"));
    assertColumnConfigFromRow(config);
  }

  @Test
  void hasEncryptedColumns_nullTable_returnsFalseWithoutQuery() throws Exception {
    MetadataManager mm = createManager(false);

    assertFalse(mm.hasEncryptedColumns(null));
    verify(mockConnection, times(0)).prepareStatement(anyString());
  }

  @Test
  void hasEncryptedColumns_database_trueWhenRowExists() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(true);

    assertTrue(mm.hasEncryptedColumns("users"));
    verify(mockStatement).setString(1, "users");
  }

  @Test
  void hasEncryptedColumns_database_falseWhenNoRow() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(false);

    assertFalse(mm.hasEncryptedColumns("orders"));
    verify(mockStatement).setString(1, "orders");
  }

  @Test
  void hasEncryptedColumns_database_sqlExceptionBecomesMetadataException() throws Exception {
    MetadataManager mm = createManager(false);
    SQLException cause = new SQLException("boom");
    when(mockStatement.executeQuery()).thenThrow(cause);

    MetadataException e = assertThrows(MetadataException.class,
        () -> mm.hasEncryptedColumns("users"));
    assertSame(cause, e.getCause());
    assertTrue(e.getMessage().contains("users"));
  }

  @Test
  void getEncryptedTableNames_database_returnsRows() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("users", "orders");

    assertEquals(new HashSet<>(Arrays.asList("users", "orders")),
        mm.getEncryptedTableNames());
  }

  @Test
  void getEncryptedTableNames_database_sqlExceptionBecomesMetadataException() throws Exception {
    MetadataManager mm = createManager(false);
    SQLException cause = new SQLException("boom");
    when(mockConnection.prepareStatement(anyString())).thenThrow(cause);

    MetadataException e = assertThrows(MetadataException.class, mm::getEncryptedTableNames);
    assertSame(cause, e.getCause());
  }

  @Test
  void cacheEnabled_answersFromCacheWithoutFurtherQueries() throws Exception {
    final MetadataManager mm = createManager(true);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("table_name")).thenReturn("users");
    when(mockResultSet.getString("column_name")).thenReturn("ssn");
    when(mockResultSet.getString("encryption_algorithm")).thenReturn("AES-256-GCM");
    when(mockResultSet.getString("name")).thenReturn("test-key");
    when(mockResultSet.getString("master_key_arn")).thenReturn("arn:aws:kms:us-east-1:123:key/abc");
    when(mockResultSet.getString("encrypted_data_key")).thenReturn("encrypted-key");
    when(mockResultSet.getString("key_spec")).thenReturn("AES_256");

    mm.initialize();
    verify(mockConnection, times(1)).prepareStatement(anyString());

    assertTrue(mm.hasEncryptedColumns("users"));
    assertFalse(mm.hasEncryptedColumns("orders"));
    assertFalse(mm.hasEncryptedColumns("use"));
    Set<String> tableNames = mm.getEncryptedTableNames();
    assertEquals(Collections.singleton("users"), tableNames);

    verify(mockConnection, times(1)).prepareStatement(anyString());
  }

  @Test
  void postgresql_everyComparisonUsesTheBuiltInOperator() throws Exception {
    reportProduct("PostgreSQL");
    final List<String> statements = runComparingQueries(createManager(false));

    for (String sql : statements) {
      assertTrue(sql.contains(EncryptionSql.PG_EQUALS), sql);
      assertFalse(sql.contains(" = "), sql);
    }
  }

  @Test
  void mysql_sqlIsUnchanged() throws Exception {
    reportProduct("MySQL");
    final List<String> statements = runComparingQueries(createManager(false));

    final String columns = "SELECT em.table_name, em.column_name, em.encryption_algorithm, em.key_id, "
        + "       em.created_at, em.updated_at, "
        + "       ks.name, ks.master_key_arn, ks.encrypted_data_key, ks.hmac_key, ks.key_spec, "
        + "       ks.created_at as key_created_at, ks.last_used_at "
        + "FROM encrypt.encryption_metadata em "
        + "JOIN encrypt.key_storage ks ON em.key_id = ks.id ";
    assertEquals(Arrays.asList(
        columns + "ORDER BY em.table_name, em.column_name",
        "SELECT 1 FROM encrypt.encryption_metadata WHERE table_name = ? AND column_name = ?",
        "SELECT 1 FROM encrypt.encryption_metadata WHERE table_name = ?",
        columns + "WHERE em.table_name = ? AND em.column_name = ?"), statements);
  }

  @Test
  void connectionWithoutProductName_usesThePlainOperator() throws Exception {
    final MetadataManager mm = createManager(false);

    mm.hasEncryptedColumns("users");

    verify(mockConnection).prepareStatement("SELECT 1 FROM encrypt.encryption_metadata WHERE table_name = ?");
  }

  @Test
  void providedConnection_searchPathIsLeftAlone() throws Exception {
    reportProduct("PostgreSQL");
    runComparingQueries(createManager(false));

    verify(mockConnection, never()).createStatement();
  }

  @Test
  void pluginServicePostgresql_pinsSearchPathBeforeEachQuery() throws Exception {
    final MetadataManager mm = createPluginServiceManager(new PgTargetDriverDialect());
    final String sql =
        "SELECT 1 FROM encrypt.encryption_metadata WHERE table_name " + EncryptionSql.PG_EQUALS + " ?";

    mm.hasEncryptedColumns("users");
    mm.hasEncryptedColumns("orders");

    // Each lookup opens its own connection, pins it, queries and closes it.
    final InOrder order = inOrder(mockPlainStatement, mockConnection);
    for (int i = 0; i < 2; i++) {
      order.verify(mockPlainStatement).execute(EncryptionSql.PIN_PG_SEARCH_PATH_SQL);
      order.verify(mockConnection).prepareStatement(sql);
      order.verify(mockConnection).close();
    }
    // The dialect is known from the plugin service, so the connection is not asked.
    verify(mockConnection, never()).getMetaData();
  }

  @Test
  void pluginServiceMysql_doesNotTouchSearchPath() throws Exception {
    final MetadataManager mm = createPluginServiceManager(mock(TargetDriverDialect.class));

    mm.hasEncryptedColumns("users");

    verify(mockConnection, never()).createStatement();
    verify(mockConnection).prepareStatement("SELECT 1 FROM encrypt.encryption_metadata WHERE table_name = ?");
  }

  @Test
  void pluginServicePostgresql_failedPinClosesTheConnection() throws Exception {
    final MetadataManager mm = createPluginServiceManager(new PgTargetDriverDialect());
    final SQLException cause = new SQLException("boom");
    when(mockPlainStatement.execute(anyString())).thenThrow(cause);

    final MetadataException e = assertThrows(MetadataException.class, () -> mm.hasEncryptedColumns("users"));

    assertSame(cause, e.getCause());
    verify(mockConnection, never()).prepareStatement(anyString());
    verify(mockConnection).close();
  }
}
