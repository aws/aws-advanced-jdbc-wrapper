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

package software.amazon.jdbc.plugin.encryption.key;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.EncryptionSql;
import software.amazon.jdbc.plugin.encryption.cache.DataKeyCache;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public class KeyManagerTest {

  private static final String LISTED_ARN = "arn:aws:kms:us-east-1:111111111111:key/listed";
  private static final String UNLISTED_ARN = "arn:aws:kms:us-east-1:222222222222:key/unlisted";
  private static final String KEY_UUID = "12345678-aaaa-bbbb-cccc-abcdefabcdef";
  private static final String ROLE_ARN = "arn:aws:iam::333333333333:role/SecretRoleName";
  private static final String FULL_ARN = "arn:aws:kms:us-east-1:444444444444:key/" + KEY_UUID;
  // What KMS puts in an AccessDenied message: the key ARN and the caller's role ARN.
  private static final String KMS_MESSAGE = ROLE_ARN + " is not authorized to perform: kms:GenerateDataKey on "
      + "resource: " + FULL_ARN + " because no identity-based policy allows it";
  private static final String ENCRYPTED_DATA_KEY = Base64.getEncoder().encodeToString(new byte[] {1, 2, 3});

  @Test
  void decryptDataKey_refusesAnUnlistedMasterKey_withoutCallingKms() {
    final KmsClient kmsClient = mock(KmsClient.class);
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN);

    assertUnauthorized(() -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, UNLISTED_ARN));
    verifyNoInteractions(kmsClient);
  }

  @Test
  void decryptDataKey_refusesAnUnlistedMasterKey_onACacheHit() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(new byte[32])).build());
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN);

    // Caches the plaintext data key under the encrypted data key.
    keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, LISTED_ARN);
    assertNotNull(keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, LISTED_ARN));

    // The same encrypted data key, now recorded under another master key, is refused even though
    // its plaintext is cached.
    assertUnauthorized(() -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, UNLISTED_ARN));
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void decryptDataKey_refusesAMissingMasterKey_whenAnAllowListIsConfigured() {
    final KmsClient kmsClient = mock(KmsClient.class);
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN);

    assertUnauthorized(() -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, null));
    assertUnauthorized(() -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, ""));
    verifyNoInteractions(kmsClient);
  }

  @Test
  void refusal_logsAndThrowsTheMasterKeyMasked() {
    final String forged = "arn:aws:kms:us-east-1:222222222222:key/evil\nSEVERE: forged line";
    final Logger logger = Logger.getLogger(KeyManager.class.getName());
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        records.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    logger.addHandler(handler);
    final KeyManagementException exception;
    try {
      final KeyManager keyManager = newKeyManager(mock(KmsClient.class), LISTED_ARN);
      exception = assertThrows(KeyManagementException.class,
          () -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, forged));
    } finally {
      logger.removeHandler(handler);
    }

    final String logged = records.stream()
        .map(LogRecord::getMessage)
        .filter(message -> message.contains("Refusing to use master key"))
        .findFirst()
        .orElseThrow(AssertionError::new);
    assertFalse(logged.contains("222222222222"), logged);
    assertFalse(logged.contains("\n"), logged);
    assertTrue(logged.contains("arn:aws:kms:***:***:key/evil***line"), logged);
    // The exception carries the same masked message as the log.
    assertEquals(logged, exception.getMessage());
    assertEquals(KeyManagementException.UNAUTHORIZED_MASTER_KEY_STATE, exception.getSQLState());
  }

  @Test
  void generateDataKey_logsTheMaskedKeyAndTheErrorSummary_withoutTheKmsMessage() {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.generateDataKey(any(GenerateDataKeyRequest.class))).thenThrow(accessDenied());
    final KeyManager keyManager = newKeyManager(kmsClient, FULL_ARN);

    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final KeyManagementException exception = captureLogs(records,
        () -> assertThrows(KeyManagementException.class, () -> keyManager.generateDataKey(FULL_ARN)));

    final LogRecord severe = findRecord(records, Level.SEVERE, "Failed to generate data key");
    assertSummaryOnly(severe.getMessage());
    assertTrue(severe.getMessage().contains("arn:aws:kms:***:***:key/1234***cdef"), severe.getMessage());
    // The error is kept, as a summary, and no Throwable is attached to the record.
    assertTrue(severe.getMessage().contains("errorCode=AccessDeniedException"), severe.getMessage());
    assertNull(severe.getThrown());
    assertSummaryOnly(exception.getMessage());
  }

  @Test
  void decryptDataKey_logsTheMaskedKeyAndTheErrorSummary_withoutTheKmsMessage() {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.decrypt(any(DecryptRequest.class))).thenThrow(accessDenied());
    final KeyManager keyManager = newKeyManager(kmsClient, FULL_ARN);

    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final KeyManagementException exception = captureLogs(records,
        () -> assertThrows(KeyManagementException.class,
            () -> keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, FULL_ARN)));

    final LogRecord severe = findRecord(records, Level.SEVERE, "Failed to decrypt data key");
    assertSummaryOnly(severe.getMessage());
    assertTrue(severe.getMessage().contains("arn:aws:kms:***:***:key/1234***cdef"), severe.getMessage());
    assertTrue(severe.getMessage().contains("errorCode=AccessDeniedException"), severe.getMessage());
    assertNull(severe.getThrown());
    assertSummaryOnly(exception.getMessage());
  }

  @Test
  void retry_logsTheErrorSummary_withoutTheKmsMessage() {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenThrow((KmsException) KmsException.builder()
            .message(KMS_MESSAGE)
            .statusCode(503)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ServiceUnavailable").build())
            .build());
    final KeyManager keyManager = new KeyManager(kmsClient, mock(Connection.class), false,
        EncryptionConfig.builder().kmsRegion("us-east-1").maxRetries(1)
            .retryBackoffBase(Duration.ofMillis(1)).allowedMasterKeyArns(FULL_ARN).build());

    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    captureLogs(records,
        () -> assertThrows(KeyManagementException.class, () -> keyManager.generateDataKey(FULL_ARN)));

    final LogRecord warning = findRecord(records, Level.WARNING, "retrying");
    assertSummaryOnly(warning.getMessage());
    assertTrue(warning.getMessage().contains("errorCode=ServiceUnavailable, httpStatus=503"),
        warning.getMessage());
  }

  @Test
  void keyArnsAreMaskedAtEveryLevel_whenCreatingAndUsingKeys() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenReturn(GenerateDataKeyResponse.builder()
            .ciphertextBlob(SdkBytes.fromByteArray(new byte[] {9}))
            .plaintext(SdkBytes.fromByteArray(new byte[32]))
            .build());
    when(kmsClient.createKey(any(CreateKeyRequest.class)))
        .thenReturn(CreateKeyResponse.builder()
            .keyMetadata(KeyMetadata.builder().arn(FULL_ARN).build()).build());
    final KeyManager keyManager = newKeyManager(kmsClient, FULL_ARN);

    final Logger logger = Logger.getLogger(KeyManager.class.getName());
    final Level previous = logger.getLevel();
    logger.setLevel(Level.ALL);
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    try {
      captureLogs(records, () -> {
        try {
          keyManager.generateDataKey(FULL_ARN);
          keyManager.createMasterKey("test");
        } catch (KeyManagementException e) {
          throw new AssertionError(e);
        }
        return null;
      });
    } finally {
      logger.setLevel(previous);
    }

    assertFalse(records.isEmpty());
    for (final LogRecord record : records) {
      assertFalse(record.getMessage().contains(KEY_UUID), record.getMessage());
    }
    assertNotNull(findRecord(records, Level.FINEST, "Generating data key using master key"));
    assertNotNull(findRecord(records, Level.INFO, "Successfully created KMS master key"));
  }

  @Test
  void decryptDataKey_usesAListedMasterKey() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    final byte[] plaintextKey = new byte[32];
    Arrays.fill(plaintextKey, (byte) 5);
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(plaintextKey)).build());
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN + ",alias/other");

    assertArrayEquals(plaintextKey, keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, LISTED_ARN));
  }

  @Test
  void decryptDataKey_placesNoRestriction_withoutAnAllowList() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(new byte[32])).build());
    final KeyManager keyManager = newKeyManager(kmsClient, null);

    assertNotNull(keyManager.decryptDataKey(ENCRYPTED_DATA_KEY, UNLISTED_ARN));
  }

  @Test
  void generateDataKey_refusesAnUnlistedMasterKey_withoutCallingKms() {
    final KmsClient kmsClient = mock(KmsClient.class);
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN);

    assertUnauthorized(() -> keyManager.generateDataKey(UNLISTED_ARN));
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
  }

  @Test
  void generateDataKey_usesAListedMasterKey() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenReturn(GenerateDataKeyResponse.builder()
            .ciphertextBlob(SdkBytes.fromByteArray(new byte[] {9}))
            .plaintext(SdkBytes.fromByteArray(new byte[32]))
            .build());
    final KeyManager keyManager = newKeyManager(kmsClient, LISTED_ARN);

    assertEquals(Base64.getEncoder().encodeToString(new byte[] {9}),
        keyManager.generateDataKey(LISTED_ARN).getEncryptedKey());
  }

  @Test
  void decryptDataKey_zeroizesTheKey_whenCachingItFails() throws Exception {
    final byte[] plaintextKey = new byte[32];
    Arrays.fill(plaintextKey, (byte) 7);
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(plaintextKey)).build());

    final KeyManager keyManager = new KeyManager(kmsClient, mock(Connection.class), false,
        EncryptionConfig.builder().kmsRegion("us-east-1").maxRetries(0).build());
    final DataKeyCache failingCache = mock(DataKeyCache.class);
    final AtomicReference<byte[]> keyGivenToCache = new AtomicReference<>();
    doAnswer(invocation -> {
      keyGivenToCache.set(invocation.getArgument(1));
      throw new IllegalStateException("cache failure");
    }).when(failingCache).put(anyString(), any());
    replaceCache(keyManager, failingCache);

    assertThrows(KeyManagementException.class, () -> keyManager.decryptDataKey(
        Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}), "arn:aws:kms:us-east-1:123:key/abc"));

    assertArrayEquals(new byte[32], keyGivenToCache.get());
  }

  @Test
  void postgresql_keyQueriesUseTheBuiltInOperator() {
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), mock(Connection.class), true,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      assertEquals("SELECT id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec, created_at, "
          + "last_used_at FROM encrypt.key_storage WHERE id OPERATOR(pg_catalog.=) ?",
          keyManager.getSelectKeyMetadataSql());
      assertEquals("UPDATE encrypt.key_storage SET last_used_at = ? WHERE id OPERATOR(pg_catalog.=) ?",
          keyManager.getUpdateLastUsedSql());
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void mysql_keyQueriesAreUnchanged() {
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), mock(Connection.class), false,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      assertEquals("SELECT id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec, created_at, "
          + "last_used_at FROM encrypt.key_storage WHERE id = ?", keyManager.getSelectKeyMetadataSql());
      assertEquals("UPDATE encrypt.key_storage SET last_used_at = ? WHERE id = ?",
          keyManager.getUpdateLastUsedSql());
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void pluginServicePostgresql_pinsSearchPathOnItsOwnConnection() throws Exception {
    final Connection conn = queryableConnection();
    final Statement plain = mock(Statement.class);
    when(conn.createStatement()).thenReturn(plain);
    final KeyManager keyManager = pluginServiceKeyManager(new PgTargetDriverDialect(), conn);
    try {
      keyManager.getKeyMetadata(7);

      final InOrder order = inOrder(plain, conn);
      order.verify(plain).execute(EncryptionSql.PIN_PG_SEARCH_PATH_SQL);
      order.verify(conn).prepareStatement(keyManager.getSelectKeyMetadataSql());
      order.verify(conn).close();
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void pluginServicePostgresql_failedPinClosesTheConnection() throws Exception {
    final Connection conn = queryableConnection();
    final Statement plain = mock(Statement.class);
    when(conn.createStatement()).thenReturn(plain);
    when(plain.execute(anyString())).thenThrow(new SQLException("boom"));
    final KeyManager keyManager = pluginServiceKeyManager(new PgTargetDriverDialect(), conn);
    try {
      assertThrows(KeyManagementException.class, () -> keyManager.getKeyMetadata(7));

      verify(conn, never()).prepareStatement(anyString());
      verify(conn).close();
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void pluginServicePostgresql_failedPinAndClose_reportsThePinFailure() throws Exception {
    final Connection conn = queryableConnection();
    final Statement plain = mock(Statement.class);
    when(conn.createStatement()).thenReturn(plain);
    final SQLException pinFailure = new SQLException("pin failed");
    final SQLException closeFailure = new SQLException("close failed");
    when(plain.execute(anyString())).thenThrow(pinFailure);
    doThrow(closeFailure).when(conn).close();
    final KeyManager keyManager = pluginServiceKeyManager(new PgTargetDriverDialect(), conn);
    try {
      final KeyManagementException e =
          assertThrows(KeyManagementException.class, () -> keyManager.getKeyMetadata(7));

      assertTrue(e.getMessage().contains("pin failed"), e.getMessage());
      assertArrayEquals(new Throwable[] {closeFailure}, pinFailure.getSuppressed());
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void pluginServiceMysql_leavesSearchPathAlone() throws Exception {
    final Connection conn = queryableConnection();
    final KeyManager keyManager = pluginServiceKeyManager(mock(TargetDriverDialect.class), conn);
    try {
      keyManager.getKeyMetadata(7);

      verify(conn, never()).createStatement();
      verify(conn).prepareStatement(keyManager.getSelectKeyMetadataSql());
      assertFalse(keyManager.getSelectKeyMetadataSql().contains("OPERATOR("));
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void providedPostgresqlConnection_searchPathIsLeftAlone() throws Exception {
    final Connection conn = queryableConnection();
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), conn, true,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      keyManager.getKeyMetadata(7);

      verify(conn, never()).createStatement();
      verify(conn, never()).close();
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void getKeyMetadata_bindsTheKeyIdAsAnInteger() throws Exception {
    final Connection conn = mock(Connection.class);
    final PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.executeQuery()).thenReturn(mock(ResultSet.class));
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), conn, true,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      assertFalse(keyManager.getKeyMetadata(7).isPresent());
      assertFalse(keyManager.getKeyMetadata(" 8 ").isPresent());

      verify(stmt).setInt(1, 7);
      verify(stmt).setInt(1, 8);
      verify(stmt, never()).setString(anyInt(), anyString());
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void updateLastUsed_bindsTheKeyIdAsAnInteger() throws Exception {
    final Connection conn = mock(Connection.class);
    final PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), conn, true,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      keyManager.updateLastUsed(7);
      keyManager.updateLastUsed("8");

      verify(conn, times(2)).prepareStatement(keyManager.getUpdateLastUsedSql());
      verify(stmt).setInt(2, 7);
      verify(stmt).setInt(2, 8);
      verify(stmt, never()).setString(anyInt(), anyString());
      assertThrows(IllegalArgumentException.class, () -> keyManager.updateLastUsed("key-uuid"));
    } finally {
      keyManager.shutdown();
    }
  }

  @Test
  void getKeyMetadata_refusesANonIntegerKeyId_withoutQuerying() throws Exception {
    final Connection conn = mock(Connection.class);
    final KeyManager keyManager = new KeyManager(mock(KmsClient.class), conn, true,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
    try {
      final IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> keyManager.getKeyMetadata("key-uuid"));
      assertTrue(e.getMessage().contains("key-uuid"), e.getMessage());
      verifyNoInteractions(conn);
    } finally {
      keyManager.shutdown();
    }
  }

  private static Connection queryableConnection() throws SQLException {
    final Connection conn = mock(Connection.class);
    final PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.executeQuery()).thenReturn(mock(ResultSet.class));
    return conn;
  }

  private static KeyManager pluginServiceKeyManager(TargetDriverDialect dialect, Connection conn)
      throws SQLException {
    final PluginService pluginService = mock(PluginService.class);
    when(pluginService.getTargetDriverDialect()).thenReturn(dialect);
    when(pluginService.forceConnect(any(), any())).thenReturn(conn);
    return new KeyManager(mock(KmsClient.class), pluginService,
        EncryptionConfig.builder().kmsRegion("us-east-1").build());
  }

  private static KeyManager newKeyManager(KmsClient kmsClient, String allowedMasterKeyArns) {
    return new KeyManager(kmsClient, mock(Connection.class), false,
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .maxRetries(0)
            .allowedMasterKeyArns(allowedMasterKeyArns)
            .build());
  }

  private static void assertUnauthorized(org.junit.jupiter.api.function.Executable executable) {
    final KeyManagementException e = assertThrows(KeyManagementException.class, executable);
    assertEquals(KeyManagementException.UNAUTHORIZED_MASTER_KEY_STATE, e.getSQLState());
  }

  private static void replaceCache(KeyManager keyManager, DataKeyCache cache) throws Exception {
    final Field field = KeyManager.class.getDeclaredField("dataKeyCache");
    field.setAccessible(true);
    ((DataKeyCache) field.get(keyManager)).shutdown();
    field.set(keyManager, cache);
  }

  private static KmsException accessDenied() {
    return (KmsException) KmsException.builder()
        .message(KMS_MESSAGE)
        .statusCode(400)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDeniedException").build())
        .build();
  }

  private static void assertSummaryOnly(final String text) {
    assertFalse(text.contains(KEY_UUID), text);
    assertFalse(text.contains("444444444444"), text);
    assertFalse(text.contains("333333333333"), text);
    assertFalse(text.contains("SecretRoleName"), text);
  }

  private static LogRecord findRecord(final List<LogRecord> records, final Level level, final String text) {
    return records.stream()
        .filter(r -> r.getLevel().equals(level) && r.getMessage().contains(text))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No " + level + " record containing '" + text + "' in " + records));
  }

  private static <T> T captureLogs(final List<LogRecord> records, final java.util.function.Supplier<T> action) {
    final Logger logger = Logger.getLogger(KeyManager.class.getName());
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        records.add(record);
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
      return action.get();
    } finally {
      logger.removeHandler(handler);
    }
  }
}
