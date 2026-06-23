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

package software.amazon.jdbc.plugin.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;
import software.amazon.jdbc.targetdriverdialect.GenericTargetDriverDialect;

public class KmsEncryptionConnectionPluginTest {

  private AutoCloseable closeable;
  private KmsEncryptionConnectionPlugin plugin;

  @Mock PluginService mockPluginService;
  @Mock KmsEncryptionUtility mockUtility;
  @Mock MetadataManager mockMetadataManager;
  @Mock KeyManager mockKeyManager;
  @Mock EncryptionService mockEncryptionService;
  @Mock SqlAnalysisService mockSqlAnalysisService;
  @Mock PreparedStatement mockPreparedStatement;
  @Mock ResultSet mockResultSet;
  @Mock ResultSetMetaData mockResultSetMetaData;
  @Mock Connection mockConnection;

  private final byte[] testDataKey = new byte[32];
  private final byte[] testHmacKey = new byte[32];
  private final byte[] testEncrypted = new byte[]{1, 2, 3, 4, 5};

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockUtility.getMetadataManager()).thenReturn(mockMetadataManager);
    when(mockUtility.getKeyManager()).thenReturn(mockKeyManager);
    when(mockUtility.getEncryptionService()).thenReturn(mockEncryptionService);
    when(mockUtility.getSqlAnalysisService()).thenReturn(mockSqlAnalysisService);
    when(mockPluginService.getTargetDriverDialect())
        .thenReturn(new GenericTargetDriverDialect());

    plugin = new KmsEncryptionConnectionPlugin(mockPluginService, mockUtility);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_prepareStatement_tracksContext() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    JdbcCallable<PreparedStatement, SQLException> callable = () -> mockPreparedStatement;

    PreparedStatement result = plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", callable,
        "INSERT INTO users (name, ssn) VALUES (?, ?)");

    assertEquals(mockPreparedStatement, result);
  }

  private ColumnEncryptionConfig createTestConfig() {
    KeyMetadata keyMetadata = KeyMetadata.builder()
        .keyName("test-key")
        .encryptedDataKey("encrypted-key")
        .masterKeyArn("arn:aws:kms:us-east-1:123:key/abc")
        .hmacKey(testHmacKey)
        .keySpec("AES_256")
        .build();
    return ColumnEncryptionConfig.builder()
        .tableName("users").columnName("ssn").keyId(1).keyMetadata(keyMetadata).build();
  }

  @Test
  void test_setString_encryptsForEncryptedColumn() throws Exception {
    // Set up statement context by calling prepareStatement first
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);
    when(mockSqlAnalysisService.getColumnParameterMapping(anyString()))
        .thenReturn(java.util.Collections.singletonMap(2, "ssn"));

    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO users (name, ssn) VALUES (?, ?)");

    ColumnEncryptionConfig config = createTestConfig();

    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(config);
    when(mockKeyManager.decryptDataKey("encrypted-key", "arn:aws:kms:us-east-1:123:key/abc"))
        .thenReturn(testDataKey);
    when(mockEncryptionService.encrypt(eq("123-45-6789"), any(), any(), anyString()))
        .thenReturn(testEncrypted);

    JdbcCallable<Void, SQLException> callable = () -> null;

    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", callable,
        2, "123-45-6789");

    // Should have set encrypted bytes via the strategy
    verify(mockPreparedStatement).setBytes(eq(2), any());
  }

  @Test
  void test_setString_passesThrough_forNonEncryptedColumn() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);
    when(mockSqlAnalysisService.getColumnParameterMapping(anyString()))
        .thenReturn(java.util.Collections.singletonMap(1, "name"));

    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO users (name) VALUES (?)");

    when(mockMetadataManager.isColumnEncrypted("users", "name")).thenReturn(false);

    JdbcCallable<Void, SQLException> callable = () -> null;
    boolean[] called = {false};
    JdbcCallable<Void, SQLException> trackingCallable = () -> {
      called[0] = true;
      return null;
    };

    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", trackingCallable,
        1, "Alice");

    assertTrue(called[0], "Original callable should be invoked for non-encrypted column");
  }

  @Test
  void test_setString_passesThrough_forUntrackedStatement() throws Exception {
    // Don't call prepareStatement first — no context tracked
    boolean[] called = {false};
    JdbcCallable<Void, SQLException> callable = () -> {
      called[0] = true;
      return null;
    };

    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", callable,
        1, "value");

    assertTrue(called[0], "Original callable should be invoked for untracked statement");
  }

  @Test
  void test_getString_decryptsForEncryptedColumn() throws Exception {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("ssn");
    when(mockResultSetMetaData.getTableName(1)).thenReturn("users");

    ColumnEncryptionConfig config = createTestConfig(); //

    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(config);
    when(mockKeyManager.decryptDataKey("encrypted-key", "arn:aws:kms:us-east-1:123:key/abc"))
        .thenReturn(testDataKey);
    when(mockResultSet.getObject(1)).thenReturn(testEncrypted);
    when(mockResultSet.getBytes(1)).thenReturn(testEncrypted);
    when(mockEncryptionService.decrypt(any(), any(), any(), anyString(), eq(String.class)))
        .thenReturn("123-45-6789");

    JdbcCallable<String, SQLException> callable = () -> "raw-value";

    String result = plugin.execute(
        String.class, SQLException.class, mockResultSet,
        "ResultSet.getString", callable,
        1);

    assertEquals("123-45-6789", result);
  }

  @Test
  void test_getString_passesThrough_forNonEncryptedColumn() throws Exception {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("name");
    when(mockResultSetMetaData.getTableName(1)).thenReturn("users");
    when(mockMetadataManager.isColumnEncrypted("users", "name")).thenReturn(false);

    boolean[] called = {false};
    JdbcCallable<String, SQLException> callable = () -> {
      called[0] = true;
      return "Alice";
    };

    String result = plugin.execute(
        String.class, SQLException.class, mockResultSet,
        "ResultSet.getString", callable,
        1);

    assertTrue(called[0]);
    assertEquals("Alice", result);
  }

  @Test
  void test_getString_byLabel_resolvesTablePerColumn() throws Exception {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSet.findColumn("ssn")).thenReturn(2);
    when(mockResultSetMetaData.getTableName(2)).thenReturn("users");

    ColumnEncryptionConfig config = createTestConfig(); //

    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(config);
    when(mockKeyManager.decryptDataKey(anyString(), anyString())).thenReturn(testDataKey);
    when(mockResultSet.getObject("ssn")).thenReturn(testEncrypted);
    when(mockResultSet.getBytes("ssn")).thenReturn(testEncrypted);
    when(mockEncryptionService.decrypt(any(), any(), any(), anyString(), eq(String.class)))
        .thenReturn("123-45-6789");

    String result = plugin.execute(
        String.class, SQLException.class, mockResultSet,
        "ResultSet.getString", () -> "raw", "ssn");

    assertEquals("123-45-6789", result);
  }

  @Test
  void test_multiTableQuery_resolvesTablePerColumn() throws Exception {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    // Column 1 is from "orders" table, column 2 is from "users" table
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("order_id");
    when(mockResultSetMetaData.getTableName(1)).thenReturn("orders");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("ssn");
    when(mockResultSetMetaData.getTableName(2)).thenReturn("users");

    when(mockMetadataManager.isColumnEncrypted("orders", "order_id")).thenReturn(false);

    boolean[] called = {false};
    JdbcCallable<Integer, SQLException> callable = () -> {
      called[0] = true;
      return 42;
    };

    // Column 1 (orders.order_id) — not encrypted, passes through
    Integer result = plugin.execute(
        Integer.class, SQLException.class, mockResultSet,
        "ResultSet.getInt", callable, 1);

    assertTrue(called[0]);
    assertEquals(42, result);
  }

  @Test
  void test_multiTableQuery_encryptedColumnsFromDifferentTables() throws Exception {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    // Column 1: orders.credit_card (encrypted), Column 2: users.ssn (encrypted)
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("credit_card");
    when(mockResultSetMetaData.getTableName(1)).thenReturn("orders");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("ssn");
    when(mockResultSetMetaData.getTableName(2)).thenReturn("users");

    ColumnEncryptionConfig usersConfig = createTestConfig();
    ColumnEncryptionConfig ordersConfig = createTestConfig();

    // Both columns encrypted but in different tables
    when(mockMetadataManager.isColumnEncrypted("orders", "credit_card")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("orders", "credit_card")).thenReturn(ordersConfig);
    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(usersConfig);

    when(mockKeyManager.decryptDataKey(anyString(), anyString())).thenReturn(testDataKey);
    byte[] encBytes1 = {10, 20, 30};
    byte[] encBytes2 = {40, 50, 60};
    when(mockResultSet.getObject(1)).thenReturn(encBytes1);
    when(mockResultSet.getBytes(1)).thenReturn(encBytes1);
    when(mockResultSet.getObject(2)).thenReturn(encBytes2);
    when(mockResultSet.getBytes(2)).thenReturn(encBytes2);

    when(mockEncryptionService.decrypt(eq(encBytes1), any(), any(), anyString(), eq(String.class)))
        .thenReturn("4111-1111-1111-1111");
    when(mockEncryptionService.decrypt(eq(encBytes2), any(), any(), anyString(), eq(String.class)))
        .thenReturn("123-45-6789");

    // Decrypt column 1 (orders.credit_card)
    String cc = plugin.execute(
        String.class, SQLException.class, mockResultSet,
        "ResultSet.getString", () -> "raw", 1);
    assertEquals("4111-1111-1111-1111", cc);

    // Decrypt column 2 (users.ssn) — different table, same ResultSet
    String ssn = plugin.execute(
        String.class, SQLException.class, mockResultSet,
        "ResultSet.getString", () -> "raw", 2);
    assertEquals("123-45-6789", ssn);

    // Verify the correct table was used for each column
    verify(mockMetadataManager).isColumnEncrypted("orders", "credit_card");
    verify(mockMetadataManager).isColumnEncrypted("users", "ssn");
  }

  @Test
  void test_connectionClose_clearsContexts() throws Exception {
    // Track a statement
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);
    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "SELECT * FROM users");

    // Close connection
    boolean[] called = {false};
    plugin.execute(
        Void.class, SQLException.class, mockConnection,
        "Connection.close", () -> {
          called[0] = true;
          return null;
        }
    );

    assertTrue(called[0], "Connection.close callable should be invoked");

    // After close, setString on the tracked statement should pass through
    boolean[] setStringCalled = {false};
    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString",
        () -> {
          setStringCalled[0] = true;
          return null;
        },
        1, "value");

    assertTrue(setStringCalled[0], "Should pass through after connection close");
  }

  @Test
  void test_preparedStatementClose_removesContext() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);
    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO users (ssn) VALUES (?)");

    // Close the prepared statement
    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.close", () -> null);

    // After close, setString should pass through
    boolean[] called = {false};
    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString",
        () -> {
          called[0] = true;
          return null;
        },
        1, "value");

    assertTrue(called[0], "Should pass through after statement close");
  }

  @Test
  void test_subscribedMethods_containsExpectedMethods() {
    java.util.Set<String> methods = plugin.getSubscribedMethods();

    assertTrue(methods.contains("Connection.prepareStatement"));
    assertTrue(methods.contains("Connection.close"));
    assertTrue(methods.contains("PreparedStatement.close"));
    assertTrue(methods.contains("PreparedStatement.setString"));
    assertTrue(methods.contains("PreparedStatement.setInt"));
    assertTrue(methods.contains("ResultSet.getString"));
    assertTrue(methods.contains("ResultSet.getInt"));
    assertTrue(methods.contains("ResultSet.getObject"));
  }
}
