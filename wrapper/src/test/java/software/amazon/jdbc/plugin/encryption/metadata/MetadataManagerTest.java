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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.EncryptionSql;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class MetadataManagerTest {

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
