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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.sqlparser.SqlParserConnectionPlugin;
import software.amazon.jdbc.targetdriverdialect.GenericTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;

public class KmsEncryptionConnectionPluginTest {

  private AutoCloseable closeable;
  private KmsEncryptionConnectionPlugin plugin;
  private PluginCallContext callContext;

  @Mock PluginService mockPluginService;
  @Mock KmsEncryptionUtility mockUtility;
  @Mock MetadataManager mockMetadataManager;
  @Mock KeyManager mockKeyManager;
  @Mock EncryptionService mockEncryptionService;
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
    callContext = new PluginCallContext();

    when(mockUtility.getMetadataManager()).thenReturn(mockMetadataManager);
    when(mockUtility.getKeyManager()).thenReturn(mockKeyManager);
    when(mockUtility.getEncryptionService()).thenReturn(mockEncryptionService);
    when(mockPluginService.getTargetDriverDialect())
        .thenReturn(new GenericTargetDriverDialect());
    when(mockPluginService.getCallContext())
        .thenReturn(callContext);
    when(mockUtility.isFailOnUnmappedParameter()).thenReturn(true);

    plugin = new KmsEncryptionConnectionPlugin(mockPluginService, mockUtility);
  }

  @AfterEach
  void cleanUp() throws Exception {
    callContext.reset();
    closeable.close();
  }

  private void populateContext(String tableName, java.util.Map<Integer, String> paramMapping) {
    java.util.Set<String> tables = new java.util.HashSet<>();
    if (tableName != null) {
      tables.add(tableName);
    }
    callContext.setAttribute(SqlContextKeys.TABLES, tables);
    callContext.setAttribute(SqlContextKeys.PARAM_MAPPING,
        paramMapping != null ? paramMapping : java.util.Collections.emptyMap());
    callContext.setAttribute(SqlContextKeys.QUERY_TYPE, software.amazon.jdbc.parser.QueryType.INSERT);
    callContext.setAttribute(SqlContextKeys.CLEAN_SQL, "");
    callContext.setAttribute(SqlContextKeys.ANNOTATIONS, java.util.Collections.emptyMap());
  }

  @Test
  void test_prepareStatement_tracksContext() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    JdbcCallable<PreparedStatement, SQLException> callable = () -> mockPreparedStatement;

    populateContext("users", java.util.Collections.emptyMap());
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

    populateContext("users", java.util.Collections.singletonMap(2, "ssn"));
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

  /**
   * An /*@encrypt:table.column*&#47; annotation puts a qualified identifier into the parameter
   * mapping, overwriting the bare column name the positional mapping would have supplied. Since
   * MetadataManager composes its lookup key as {@code tableName + "." + columnName}, the qualified
   * value has to be split before the lookup, or the key becomes {@code users.users.ssn} and
   * encryption is silently skipped for the annotated parameter.
   */
  @Test
  void test_setString_encryptsForAnnotationQualifiedColumn() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    populateContext("users", java.util.Collections.singletonMap(2, "users.ssn"));
    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO users (name, ssn) VALUES (?, /*@encrypt:users.ssn*/ ?)");

    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(createTestConfig());
    when(mockKeyManager.decryptDataKey("encrypted-key", "arn:aws:kms:us-east-1:123:key/abc"))
        .thenReturn(testDataKey);
    when(mockEncryptionService.encrypt(eq("123-45-6789"), any(), any(), anyString()))
        .thenReturn(testEncrypted);

    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", () -> null,
        2, "123-45-6789");

    verify(mockPreparedStatement).setBytes(eq(2), any());
    // The table prefix must not be applied twice.
    verify(mockMetadataManager, never()).isColumnEncrypted("users", "users.ssn");
  }

  /**
   * A schema-qualified identifier must split on the last dot, so {@code public.users.ssn} resolves
   * to table {@code public.users} and column {@code ssn}.
   */
  @Test
  void test_setString_splitsSchemaQualifiedColumnOnLastDot() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    populateContext("users", java.util.Collections.singletonMap(1, "public.users.ssn"));
    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO public.users (ssn) VALUES (/*@encrypt:public.users.ssn*/ ?)");

    when(mockMetadataManager.isColumnEncrypted("public.users", "ssn")).thenReturn(false);

    boolean[] called = {false};
    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", () -> {
          called[0] = true;
          return null;
        },
        1, "123-45-6789");

    assertTrue(called[0], "Original callable should be invoked for non-encrypted column");
    verify(mockMetadataManager).isColumnEncrypted("public.users", "ssn");
  }

  /**
   * A bare column name keeps using the statement's parsed table, so the positional mapping path is
   * unchanged by the qualified-identifier handling.
   */
  @Test
  void test_setString_bareColumnStillUsesParsedTable() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    populateContext("users", java.util.Collections.singletonMap(1, "ssn"));
    plugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement", () -> mockPreparedStatement,
        "INSERT INTO users (ssn) VALUES (?)");

    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(false);

    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setString", () -> null,
        1, "123-45-6789");

    verify(mockMetadataManager).isColumnEncrypted("users", "ssn");
  }

  @Test
  void test_setString_passesThrough_forNonEncryptedColumn() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);

    populateContext("users", java.util.Collections.singletonMap(1, "name"));
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
    populateContext("users", java.util.Collections.emptyMap());
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
    populateContext("users", java.util.Collections.emptyMap());
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

  // ---------------------------------------------------------------------------------------------
  // Chained sqlParser + kmsEncryption tests. Only users.ssn is encrypted.
  // ---------------------------------------------------------------------------------------------

  private static final String ANNOTATION = "/*@encrypt:users.ssn*/";

  private void stubEncryptedUsersSsn() throws Exception {
    when(mockPreparedStatement.getConnection()).thenReturn(mockConnection);
    when(mockMetadataManager.isColumnEncrypted("users", "ssn")).thenReturn(true);
    when(mockMetadataManager.hasEncryptedColumns("users")).thenReturn(true);
    when(mockMetadataManager.getEncryptedTableNames()).thenReturn(Collections.singleton("users"));
    when(mockMetadataManager.getColumnConfig("users", "ssn")).thenReturn(createTestConfig());
    when(mockKeyManager.decryptDataKey(anyString(), anyString())).thenReturn(testDataKey);
    when(mockEncryptionService.encrypt(any(), any(), any(), anyString())).thenReturn(testEncrypted);
  }

  private PreparedStatement prepareChained(String sql) throws SQLException {
    SqlParserConnectionPlugin parserPlugin =
        new SqlParserConnectionPlugin(mockPluginService, new Properties());
    return parserPlugin.execute(
        PreparedStatement.class, SQLException.class, mockConnection,
        "Connection.prepareStatement",
        () -> plugin.execute(
            PreparedStatement.class, SQLException.class, mockConnection,
            "Connection.prepareStatement", () -> mockPreparedStatement, sql),
        new Object[] {sql});
  }

  /** Binds parameters 1..n with setString and returns the indexes that were encrypted. */
  private Set<Integer> boundEncrypted(int n) throws SQLException {
    return boundEncrypted(n, "PreparedStatement.setString");
  }

  private Set<Integer> boundEncrypted(int n, String methodName) throws SQLException {
    Set<Integer> encrypted = new TreeSet<>();
    for (int i = 1; i <= n; i++) {
      boolean[] passedThrough = {false};
      plugin.execute(
          Void.class, SQLException.class, mockPreparedStatement, methodName,
          () -> {
            passedThrough[0] = true;
            return null;
          },
          i, "v" + i);
      if (passedThrough[0]) {
        verify(mockPreparedStatement, never()).setBytes(eq(i), any());
      } else {
        verify(mockPreparedStatement).setBytes(eq(i), any());
        encrypted.add(i);
      }
    }
    return encrypted;
  }

  private static Set<Integer> indexes(Integer... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  /** Captures WARNING records of the KMS plugin logger while {@code action} runs. */
  private List<LogRecord> captureWarnings(ThrowingRunnable action) throws Exception {
    Logger logger = Logger.getLogger(KmsEncryptionConnectionPlugin.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          records.add(record);
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    try {
      action.run();
    } finally {
      logger.removeHandler(handler);
    }
    return records;
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  static Stream<Arguments> chainedEncryptionCases() {
    return Stream.of(
        // (a) reporter statements
        Arguments.of("A2", "INSERT INTO users (name, ssn) VALUES ('literal-name', ?)", 1,
            indexes(1)),
        Arguments.of("B2",
            "UPDATE users SET note = 'really?', ssn = " + ANNOTATION + " ? WHERE id = 1", 1,
            indexes(1)),
        // (b) multi-row and upserts
        Arguments.of("A6", "INSERT INTO users (name, ssn) VALUES (?, ?), (?, ?)", 4,
            indexes(2, 4)),
        Arguments.of("A11",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = ?", 3,
            indexes(2, 3)),
        Arguments.of("A12",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON DUPLICATE KEY UPDATE ssn = ?", 3,
            indexes(2, 3)),
        // (g) controls and the remaining starred rows
        Arguments.of("A1", "INSERT INTO users (name, ssn) VALUES (?, ?)", 2, indexes(2)),
        Arguments.of("A3", "UPDATE users SET name = 'x', ssn = ? WHERE id = ?", 2, indexes(1)),
        Arguments.of("A4", "UPDATE users SET ssn = ? WHERE id = ?", 2, indexes(1)),
        Arguments.of("A7c",
            "INSERT INTO users (name, ssn) SELECT 'n', ? FROM staging WHERE id = ?", 2,
            indexes(1)),
        Arguments.of("A9", "INSERT INTO users (name, ssn) VALUES (concat(?, ?), ?)", 3,
            indexes(3)),
        Arguments.of("A16", "INSERT INTO users (\"name\", \"ssn\") VALUES (?, ?)", 2, indexes(2)),
        Arguments.of("A21", "UPDATE users SET ver = ver + 1, ssn = ? WHERE id = ?", 2,
            indexes(1)),
        Arguments.of("A22", "INSERT INTO public.users (name, ssn) VALUES ('n', ?)", 1,
            indexes(1)),
        Arguments.of("B1", "UPDATE users SET note = 'ok', ssn = " + ANNOTATION + " ? WHERE id = 1",
            1, indexes(1)),
        Arguments.of("B6", "-- why?\nUPDATE users SET ssn = " + ANNOTATION + " ? WHERE id = ?", 2,
            indexes(1)),
        Arguments.of("B7", "/* why? */ UPDATE users SET ssn = " + ANNOTATION + " ? WHERE id = ?",
            2, indexes(1)),
        Arguments.of("B11",
            "INSERT INTO users (note, name, ssn) VALUES ('" + ANNOTATION + " ?', ?, ?)", 2,
            indexes(2)),
        Arguments.of("B14",
            "INSERT INTO users (note, ssn) VALUES ('what?', " + ANNOTATION + " ?)", 1,
            indexes(1)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("chainedEncryptionCases")
  void test_chained_encryptsExactlyTheEncryptedColumnParameters(
      String id, String sql, int parameterCount, Set<Integer> expected) throws Exception {
    stubEncryptedUsersSsn();

    List<LogRecord> warnings = captureWarnings(() -> {
      assertEquals(mockPreparedStatement, prepareChained(sql));
      assertEquals(expected, boundEncrypted(parameterCount));
    });

    assertTrue(warnings.isEmpty(), "No WARNING expected for " + id);
    verify(mockPreparedStatement, never()).close();
  }

  @Test
  void test_chained_insertWithoutColumnList_strict_throwsAndClosesStatement() throws Exception {
    stubEncryptedUsersSsn();
    String sql = "INSERT INTO users VALUES (?, ?, ?)";

    SQLException e = assertThrows(SQLException.class, () -> prepareChained(sql));

    verify(mockPreparedStatement).close();
    assertTrue(e.getMessage().contains("users"), e.getMessage());
    assertTrue(e.getMessage().contains("encryptionFailOnUnmappedParameter"), e.getMessage());
    assertFalse(e.getMessage().contains(sql), e.getMessage());
  }

  static Stream<Arguments> chainedRejectedCases() {
    return Stream.of(
        // PARAMS_IN_EXPRESSION on the encrypted column
        Arguments.of("expressionOnEncryptedColumn",
            "INSERT INTO users (name, ssn) VALUES (?, lower(?))", false),
        Arguments.of("scalarSubSelectOnEncryptedColumn",
            "UPDATE users SET ssn = (SELECT s FROM t WHERE id = ?) WHERE id = ?", false),
        // UNKNOWN_TARGET_PARAMS covering the encrypted column
        Arguments.of("rowSubSelect",
            "UPDATE users SET (name, ssn) = (SELECT a, b FROM t WHERE id = ?) WHERE id = ?",
            false),
        // FIXED_INDEX_PARAMETER
        Arguments.of("numberedPlaceholders",
            "INSERT INTO users (name, ssn) VALUES (?1, ?2)", false),
        // UNSUPPORTED_STATEMENT
        Arguments.of("replace", "REPLACE INTO users (name, ssn) VALUES (?, ?)", false),
        Arguments.of("merge",
            "MERGE INTO users u USING src s ON (u.id = s.id)"
                + " WHEN MATCHED THEN UPDATE SET u.ssn = ?",
            false),
        // PLACEHOLDER_COUNT_MISMATCH (backticks are not quotes for PostgreSQL)
        Arguments.of("countMismatch",
            "UPDATE users SET `what?` = 'x', ssn = ? WHERE id = ?", true));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("chainedRejectedCases")
  void test_chained_relevantProblem_strict_throwsAndClosesStatement(
      String id, String sql, boolean postgresDialect) throws Exception {
    stubEncryptedUsersSsn();
    if (postgresDialect) {
      when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());
    }

    SQLException e = assertThrows(SQLException.class, () -> prepareChained(sql));

    verify(mockPreparedStatement).close();
    assertTrue(e.getMessage().contains("users"), e.getMessage());
    assertTrue(e.getMessage().contains("encryptionFailOnUnmappedParameter"), e.getMessage());
    assertFalse(e.getMessage().contains(sql), e.getMessage());
  }

  @Test
  void test_chained_strict_closeFailureIsSuppressed() throws Exception {
    stubEncryptedUsersSsn();
    SQLException closeFailure = new SQLException("close failed");
    doThrow(closeFailure).when(mockPreparedStatement).close();

    SQLException e = assertThrows(SQLException.class,
        () -> prepareChained("INSERT INTO users VALUES (?, ?, ?)"));

    assertEquals(1, e.getSuppressed().length);
    assertSame(closeFailure, e.getSuppressed()[0]);
  }

  @Test
  void test_chained_insertWithoutColumnList_nonStrict_logsOneWarning() throws Exception {
    stubEncryptedUsersSsn();
    when(mockUtility.isFailOnUnmappedParameter()).thenReturn(false);
    String sql = "INSERT INTO users VALUES (?, ?, ?)";

    List<LogRecord> warnings = captureWarnings(() -> {
      assertEquals(mockPreparedStatement, prepareChained(sql));
      assertEquals(Collections.emptySet(), boundEncrypted(3));
    });

    assertEquals(1, warnings.size());
    String message = warnings.get(0).getMessage();
    assertTrue(message.contains("users"), message);
    assertFalse(message.contains(sql), message);
    verify(mockPreparedStatement, never()).close();
  }

  @Test
  void test_chained_problemsOnTableWithoutEncryptedColumns_prepareWithoutWarning()
      throws Exception {
    stubEncryptedUsersSsn();

    List<LogRecord> warnings = captureWarnings(() -> {
      assertEquals(mockPreparedStatement, prepareChained("INSERT INTO other_table VALUES (?, ?)"));
      assertEquals(mockPreparedStatement,
          prepareChained("INSERT INTO other_table (a, b) VALUES (lower(?), ?)"));
      assertEquals(mockPreparedStatement,
          prepareChained("UPDATE other_table SET flag = (attrs ?? 'k'), x = ? WHERE id = ?"));
    });

    assertTrue(warnings.isEmpty());
    verify(mockPreparedStatement, never()).close();
  }

  @Test
  void test_chained_unparsedUpdateOnEncryptedTable_strict_throws() throws Exception {
    stubEncryptedUsersSsn();

    SQLException e = assertThrows(SQLException.class, () -> prepareChained(
        "UPDATE users SET flag = (attrs ?? 'k'), ssn = " + ANNOTATION + " ? WHERE id = ?"));

    verify(mockPreparedStatement).close();
    assertTrue(e.getMessage().contains("users"), e.getMessage());
    assertFalse(e.getMessage().contains("attrs"), e.getMessage());
  }

  @Test
  void test_chained_unparsedUpdateOnEncryptedTable_nonStrict_annotationStillEncrypts()
      throws Exception {
    stubEncryptedUsersSsn();
    when(mockUtility.isFailOnUnmappedParameter()).thenReturn(false);

    List<LogRecord> warnings = captureWarnings(() -> {
      prepareChained(
          "UPDATE users SET flag = (attrs ?? 'k'), ssn = " + ANNOTATION + " ? WHERE id = ?");
      assertEquals(indexes(1), boundEncrypted(2));
    });

    assertEquals(1, warnings.size());
  }

  @Test
  void test_chained_metadataFailureDuringCheck_strict_throws() throws Exception {
    stubEncryptedUsersSsn();
    when(mockMetadataManager.hasEncryptedColumns("users"))
        .thenThrow(new MetadataException("lookup failed"));

    assertThrows(MetadataException.class,
        () -> prepareChained("INSERT INTO users VALUES (?, ?, ?)"));
    verify(mockPreparedStatement).close();
  }

  @Test
  void test_chained_metadataFailureDuringCheck_nonStrict_logsOneWarning() throws Exception {
    stubEncryptedUsersSsn();
    when(mockUtility.isFailOnUnmappedParameter()).thenReturn(false);
    when(mockMetadataManager.hasEncryptedColumns("users"))
        .thenThrow(new MetadataException("lookup failed"));

    List<LogRecord> warnings = captureWarnings(
        () -> prepareChained("INSERT INTO users VALUES (?, ?, ?)"));

    assertEquals(1, warnings.size());
    verify(mockPreparedStatement, never()).close();
  }

  @Test
  void test_chained_setNString_encryptsEncryptedColumn() throws Exception {
    stubEncryptedUsersSsn();
    prepareChained("INSERT INTO users (name, ssn) VALUES (?, ?)");

    assertEquals(indexes(2), boundEncrypted(2, "PreparedStatement.setNString"));
  }

  @Test
  void test_chained_streamSetter_onEncryptedColumn_throws_elsePassesThrough() throws Exception {
    stubEncryptedUsersSsn();
    prepareChained("INSERT INTO users (name, ssn) VALUES (?, ?)");

    SQLException e = assertThrows(SQLException.class, () -> plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setCharacterStream", () -> null,
        2, new StringReader("123-45-6789")));
    assertTrue(e.getMessage().contains("users.ssn"), e.getMessage());
    assertTrue(e.getMessage().contains("PreparedStatement.setCharacterStream"), e.getMessage());
    assertFalse(e.getMessage().contains("123-45-6789"), e.getMessage());

    boolean[] called = {false};
    plugin.execute(
        Void.class, SQLException.class, mockPreparedStatement,
        "PreparedStatement.setCharacterStream", () -> {
          called[0] = true;
          return null;
        },
        1, new StringReader("Alice"));
    assertTrue(called[0], "Stream setter on a non-encrypted column must pass through");
    verify(mockPreparedStatement, never()).setBytes(anyInt(), any());
  }

  @Test
  void test_subscribedMethods_containsNewSetters() {
    Set<String> methods = plugin.getSubscribedMethods();

    for (String setter : Arrays.asList(
        "PreparedStatement.setNString",
        "PreparedStatement.setCharacterStream",
        "PreparedStatement.setNCharacterStream",
        "PreparedStatement.setAsciiStream",
        "PreparedStatement.setBinaryStream",
        "PreparedStatement.setClob",
        "PreparedStatement.setNClob",
        "PreparedStatement.setBlob",
        "PreparedStatement.setUnicodeStream")) {
      assertTrue(methods.contains(setter), setter);
    }
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
