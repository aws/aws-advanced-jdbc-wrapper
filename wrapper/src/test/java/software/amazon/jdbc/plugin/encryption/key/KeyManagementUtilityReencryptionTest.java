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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.ColumnEncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;

public class KeyManagementUtilityReencryptionTest {

  private static final String ALGORITHM = EncryptionAlgorithm.AES_256_GCM;
  private static final String MASTER_KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/test-master-key";
  private static final String TABLE = "users";
  private static final String COLUMN = "ssn";
  private static final String PK = "id";

  private AutoCloseable closeable;
  private final Map<String, byte[]> kmsPlaintexts = new HashMap<>();
  private final EncryptionService encryptionService = new EncryptionService();
  private FakeReencryptionDatabase db;
  private KeyManager keyManager;
  private KeyManagementUtility utility;

  @Mock KmsClient kmsClient;
  @Mock MetadataManager metadataManager;

  /** A key_storage row as the test knows it, with its plaintext keys. */
  private static final class TestKey {
    final int id;
    final String encryptedDataKey;
    final byte[] dataKey;
    final byte[] hmacKey;

    TestKey(int id, String encryptedDataKey, byte[] dataKey, byte[] hmacKey) {
      this.id = id;
      this.encryptedDataKey = encryptedDataKey;
      this.dataKey = dataKey;
      this.hmacKey = hmacKey;
    }
  }

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(kmsClient.generateDataKey(any(GenerateDataKeyRequest.class))).thenAnswer(invocation -> {
      final byte[] blob = randomBytes(16);
      final byte[] plaintext = randomBytes(32);
      kmsPlaintexts.put(base64(blob), plaintext);
      return GenerateDataKeyResponse.builder()
          .ciphertextBlob(SdkBytes.fromByteArray(blob))
          .plaintext(SdkBytes.fromByteArray(plaintext))
          .build();
    });
    when(kmsClient.decrypt(any(DecryptRequest.class))).thenAnswer(invocation -> {
      final DecryptRequest request = invocation.getArgument(0);
      final byte[] plaintext = kmsPlaintexts.get(base64(request.ciphertextBlob().asByteArray()));
      if (plaintext == null) {
        throw KmsException.builder().message("unknown ciphertext").statusCode(400).build();
      }
      return DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(plaintext.clone())).build();
    });
  }

  @AfterEach
  void tearDown() throws Exception {
    if (keyManager != null) {
      keyManager.shutdown();
    }
    closeable.close();
  }

  private void init(boolean postgres, String tableName) {
    init(postgres, tableName, COLUMN);
  }

  private void init(boolean postgres, String tableName, String columnName) {
    db = new FakeReencryptionDatabase(postgres, tableName, columnName);
    final EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .maxRetries(0)
        .dataKeyCacheEnabled(false)
        .build();
    keyManager = new KeyManager(kmsClient, db.getConnection(), postgres, config);
    utility = new KeyManagementUtility(keyManager, metadataManager, db.getConnection(), kmsClient, config);
  }

  // ---- 1. happy path across several batches --------------------------------------------------

  @Test
  void rotate_reencryptsAllRowsAcrossBatches() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("key-users-ssn-1");
    db.setMetadataKeyId(oldKey.id);
    final Object[] values = {"111-11-1111", 42, new byte[] {9, 8, 7}, "444-44-4444", 5, "666-66-6666", "777"};
    for (int i = 0; i < values.length; i++) {
      db.putRow(i + 1, encrypt(values[i], oldKey));
    }

    final DataKeyReencryptionResult result =
        utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 3);

    assertEquals(7, result.getReencryptedCount());
    assertEquals(0, result.getAlreadyCurrentCount());
    assertEquals(0, result.getUnmatchedCount());
    assertEquals(3, result.getBatchCount());
    assertTrue(result.isComplete());
    assertEquals(Collections.singletonList(oldKey.id), result.getSourceKeyIds());
    assertEquals(db.getMetadataKeyId(), result.getCurrentKeyId());
    assertNotEquals(oldKey.id, result.getCurrentKeyId());

    final TestKey newKey = keyFromDb(result.getCurrentKeyId());
    for (int i = 0; i < values.length; i++) {
      assertDecryptsTo(values[i], db.getRow(i + 1), newKey);
    }
    assertNotNull(db.getKey(oldKey.id), "the old key row must be kept");
    assertEquals(2, db.getKeyCount());
    assertEquals(MASTER_KEY_ARN, db.getKey(newKey.id).masterKeyArn);
    verify(kmsClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
    verify(metadataManager).refreshMetadata();
    assertEquals(4, db.getCommitCount(), "1 switch commit and 3 batch commits");
    assertTrue(db.isAutoCommit(), "auto-commit must be restored");
    assertTrue(db.getSetConfigCalls().isEmpty());
    assertTrue(db.getExecutedSql().stream().anyMatch(sql -> sql.contains("FROM `users`")
        && sql.contains("`ssn` IS NOT NULL") && sql.contains("ORDER BY `id`") && sql.endsWith("FOR UPDATE")));
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("UPDATE `users` SET `ssn` = ? WHERE `id` = ? AND `ssn` = ?")));
  }

  @Test
  void rotate_postgres_setsTriggerCacheOnceAndQuotes() throws Exception {
    init(true, "app.users");
    final TestKey oldKey = createKey("key-users-ssn-1");
    db.setMetadataKeyId(oldKey.id);
    for (long pk = 1; pk <= 4; pk++) {
      db.putRow(pk, encrypt("value-" + pk, oldKey));
    }
    final String newMasterKeyArn = "arn:aws:kms:us-east-1:123456789012:key/new-master-key";

    final DataKeyReencryptionResult result =
        utility.rotateDataKeyAndReencrypt("app.users", COLUMN, PK, newMasterKeyArn, 2);

    assertEquals(4, result.getReencryptedCount());
    assertEquals(2, result.getBatchCount());
    final TestKey newKey = keyFromDb(result.getCurrentKeyId());
    assertEquals(newMasterKeyArn, db.getKey(newKey.id).masterKeyArn);
    for (long pk = 1; pk <= 4; pk++) {
      assertDecryptsTo("value-" + pk, db.getRow(pk), newKey);
    }
    // Three batch transactions (two full pages, one empty page); the trigger cache is set once.
    assertEquals(1, db.getSetConfigCalls().size());
    assertEquals("hmac_key.users.ssn", db.getSetConfigCalls().get(0)[0]);
    assertEquals(hex(newKey.hmacKey), db.getSetConfigCalls().get(0)[1]);
    assertEquals(hex(newKey.hmacKey), db.getSessionSetting("hmac_key.users.ssn"));
    assertTrue(db.getKey(newKey.id).name.startsWith("key-users-ssn-"), db.getKey(newKey.id).name);
    assertTrue(db.getExecutedSql().stream().anyMatch(sql -> sql.contains("FROM \"app\".\"users\"")
        && sql.contains("\"ssn\" IS NOT NULL") && sql.contains("ORDER BY \"id\"")));
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("UPDATE \"app\".\"users\" SET \"ssn\" = ? WHERE \"id\" = ? AND \"ssn\" = ?")));
  }

  // ---- 2. resume after a mid-run failure -----------------------------------------------------

  @Test
  void rotate_failureInSecondBatch_keepsFirstBatch_andRepairCompletes() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("key-users-ssn-1");
    db.setMetadataKeyId(oldKey.id);
    for (long pk = 1; pk <= 7; pk++) {
      db.putRow(pk, encrypt("v" + pk, oldKey));
    }
    db.failOnExecuteBatchNumber(2);

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 3));

    final int newKeyId = db.getMetadataKeyId();
    assertNotEquals(oldKey.id, newKeyId, "the switch is committed before the batches");
    assertTrue(exception.getMessage().contains("old key id(s) [" + oldKey.id + "]"), exception.getMessage());
    assertTrue(exception.getMessage().contains("key id " + newKeyId), exception.getMessage());
    assertTrue(db.getRollbackCount() >= 1);
    assertTrue(db.isAutoCommit());
    final TestKey newKey = keyFromDb(newKeyId);
    for (long pk = 1; pk <= 3; pk++) {
      assertTrue(encryptionService.verifyEncryptedData(db.getRow(pk), newKey.hmacKey), "row " + pk);
    }
    for (long pk = 4; pk <= 7; pk++) {
      assertTrue(encryptionService.verifyEncryptedData(db.getRow(pk), oldKey.hmacKey), "row " + pk);
    }

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(oldKey.id), 3);

    assertEquals(4, result.getReencryptedCount());
    assertEquals(3, result.getAlreadyCurrentCount());
    assertEquals(0, result.getUnmatchedCount());
    assertEquals(newKeyId, result.getCurrentKeyId());
    for (long pk = 1; pk <= 7; pk++) {
      assertDecryptsTo("v" + pk, db.getRow(pk), newKey);
    }
  }

  // ---- 3. rows already under the current key are skipped -------------------------------------

  @Test
  void repair_skipsRowsAlreadyUnderCurrentKey() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    final byte[] row1 = encrypt("a", currentKey);
    final byte[] row2 = encrypt(2, currentKey);
    db.putRow(1, row1);
    db.putRow(2, row2);
    db.putRow(3, encrypt("c", oldKey));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(oldKey.id), 10);

    assertEquals(1, result.getReencryptedCount());
    assertEquals(2, result.getAlreadyCurrentCount());
    assertEquals(Collections.singletonList(3L), db.getRowUpdatePks());
    assertArrayEquals(row1, db.getRow(1));
    assertArrayEquals(row2, db.getRow(2));
    assertDecryptsTo("c", db.getRow(3), currentKey);
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
  }

  @Test
  void repair_noOldRows_noOldKeyKmsCall() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", currentKey));
    db.putRow(2, encrypt("b", currentKey));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(oldKey.id), 10);

    assertEquals(0, result.getReencryptedCount());
    assertEquals(2, result.getAlreadyCurrentCount());
    assertTrue(db.getRowUpdatePks().isEmpty());
    verify(kmsClient, never()).decrypt(argThat((DecryptRequest r) ->
        base64(r.ciphertextBlob().asByteArray()).equals(oldKey.encryptedDataKey)));
  }

  // ---- 4. unmatched rows are reported --------------------------------------------------------

  @Test
  void repair_reportsUnmatchedRows() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    final TestKey unrelatedKey = createKey("unrelated");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", oldKey));
    final byte[] foreign = encrypt("b", unrelatedKey);
    db.putRow(2, foreign);
    db.putRow(3, encrypt("c", currentKey));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(oldKey.id), 10);

    assertEquals(1, result.getReencryptedCount());
    assertEquals(1, result.getAlreadyCurrentCount());
    assertEquals(1, result.getUnmatchedCount());
    assertEquals(Collections.singletonList((Object) 2L), result.getUnmatchedPrimaryKeys());
    assertFalse(result.isComplete());
    assertArrayEquals(foreign, db.getRow(2));
  }

  @Test
  void rotate_precheckFails_whenRowsNotUnderCurrentKey() throws Exception {
    init(false, TABLE);
    final TestKey currentKey = createKey("current");
    final TestKey otherKey = createKey("other");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", currentKey));
    db.putRow(2, encrypt("b", otherKey));
    final Map<Long, byte[]> before = db.getRows();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    assertTrue(exception.getMessage().contains("[2]"), exception.getMessage());
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
    assertEquals(2, db.getKeyCount());
    assertEquals(currentKey.id, db.getMetadataKeyId());
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.startsWith("INSERT")));
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.startsWith("UPDATE")));
    assertRowsEqual(before, db.getRows());
  }

  // ---- 5. repair after the switch and after the legacy rotateDataKey -------------------------

  @Test
  void repair_afterLegacyRotateDataKey_and_staleWriter() throws Exception {
    init(false, TABLE);
    final TestKey keyA = createKey("key-users-ssn-1");
    db.setMetadataKeyId(keyA.id);
    for (long pk = 1; pk <= 4; pk++) {
      db.putRow(pk, encrypt("v" + pk, keyA));
    }
    final Map<Long, byte[]> before = db.getRows();
    when(metadataManager.getColumnConfig(TABLE, COLUMN)).thenReturn(ColumnEncryptionConfig.builder()
        .tableName(TABLE)
        .columnName(COLUMN)
        .algorithm(ALGORITHM)
        .keyId(keyA.id)
        .keyMetadata(KeyMetadata.builder()
            .keyId(keyA.id)
            .keyName("key-users-ssn-1")
            .masterKeyArn(MASTER_KEY_ARN)
            .encryptedDataKey(keyA.encryptedDataKey)
            .hmacKey(keyA.hmacKey)
            .keySpec("AES_256")
            .createdAt(Instant.now())
            .lastUsedAt(Instant.now())
            .build())
        .build());

    // Legacy behavior (unchanged): new key row, metadata repointed, rows not touched.
    utility.rotateDataKey(TABLE, COLUMN, null);

    final int keyBId = db.getMetadataKeyId();
    assertNotEquals(keyA.id, keyBId);
    assertEquals(2, db.getKeyCount());
    assertRowsEqual(before, db.getRows());
    assertTrue(db.getRowUpdatePks().isEmpty());
    verify(metadataManager, never()).refreshMetadata();

    // A stale instance keeps writing under key A after the switch.
    db.putRow(5, encrypt("v5", keyA));
    db.putRow(6, encrypt("v6", keyA));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(keyA.id), 2);

    assertEquals(6, result.getReencryptedCount());
    assertEquals(0, result.getUnmatchedCount());
    assertEquals(keyBId, result.getCurrentKeyId());
    final TestKey keyB = keyFromDb(keyBId);
    for (long pk = 1; pk <= 6; pk++) {
      assertDecryptsTo("v" + pk, db.getRow(pk), keyB);
    }
    assertNotNull(db.getKey(keyA.id));
  }

  @Test
  void repair_multipleOldKeys() throws Exception {
    init(true, TABLE);
    final TestKey keyA = createKey("a");
    final TestKey keyB = createKey("b");
    final TestKey keyC = createKey("c");
    db.setMetadataKeyId(keyC.id);
    db.putRow(1, encrypt("one", keyA));
    db.putRow(2, encrypt("two", keyB));
    db.putRow(3, encrypt("three", keyC));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Arrays.asList(keyA.id, keyB.id), 2);

    assertEquals(2, result.getReencryptedCount());
    assertEquals(1, result.getAlreadyCurrentCount());
    assertEquals(Arrays.asList(keyA.id, keyB.id), result.getSourceKeyIds());
    assertDecryptsTo("one", db.getRow(1), keyC);
    assertDecryptsTo("two", db.getRow(2), keyC);
    assertDecryptsTo("three", db.getRow(3), keyC);
  }

  // ---- 6. validation and rejection -----------------------------------------------------------

  static Stream<String> invalidIdentifiers() {
    final StringBuilder longName = new StringBuilder();
    for (int i = 0; i < 65; i++) {
      longName.append('a');
    }
    return Stream.of("", "1abc", "users;drop", "users--", "na\"me", "na`me", "name ", " name", "a-b",
        longName.toString());
  }

  @ParameterizedTest
  @MethodSource("invalidIdentifiers")
  void invalidIdentifiers_rejectedBeforeAnyCall(String identifier) throws Exception {
    init(false, TABLE);
    final List<Integer> oldIds = Collections.singletonList(1);

    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(identifier, COLUMN, PK, null, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, identifier, PK, null, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, identifier, null, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(identifier, COLUMN, PK, oldIds, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, identifier, PK, oldIds, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, identifier, oldIds, 10));

    verifyNoInteractions(kmsClient);
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.b.c", ".users", "users.", "app..users", "app.users;x"})
  void invalidQualifiedTableNames_rejected(String tableName) {
    init(false, TABLE);

    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(tableName, COLUMN, PK, null, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(tableName, COLUMN, PK, Collections.singletonList(1), 10));
    verifyNoInteractions(kmsClient);
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @Test
  void primaryKeySameAsColumn_rejected() {
    init(false, TABLE);

    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, COLUMN, null, 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, COLUMN, Collections.singletonList(1), 10));
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @Test
  void nullArguments_rejected() {
    init(false, TABLE);
    final List<Integer> oldIds = Collections.singletonList(1);

    assertThrows(NullPointerException.class, () -> utility.rotateDataKeyAndReencrypt(null, COLUMN, PK, null, 10));
    assertThrows(NullPointerException.class, () -> utility.rotateDataKeyAndReencrypt(TABLE, null, PK, null, 10));
    assertThrows(NullPointerException.class, () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, null, null, 10));
    assertThrows(NullPointerException.class, () -> utility.reencryptRemainingRows(null, COLUMN, PK, oldIds, 10));
    assertThrows(NullPointerException.class, () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, null, 10));
    verifyNoInteractions(kmsClient);
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, KeyManagementUtility.MAX_REENCRYPTION_BATCH_SIZE + 1})
  void invalidBatchSize_rejected(int batchSize) {
    init(false, TABLE);

    assertThrows(IllegalArgumentException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, batchSize));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(1), batchSize));
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @Test
  void oldKeyIds_emptyOrNullElement_rejected() {
    init(false, TABLE);

    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.emptyList(), 10));
    assertThrows(IllegalArgumentException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Arrays.asList(1, null), 10));
    assertTrue(db.getExecutedSql().isEmpty());
  }

  @Test
  void pluginConnection_rejected() throws Exception {
    init(false, TABLE);
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", currentKey));
    db.setPluginConnection(true);

    assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));
    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(99), 10));

    verifyNoInteractions(kmsClient);
    assertTrue(db.getExecutedSql().isEmpty());
    assertEquals(currentKey.id, db.getMetadataKeyId());
  }

  @Test
  void repair_oldKeyIsCurrent_rejected() throws Exception {
    init(false, TABLE);
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", currentKey));

    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(currentKey.id), 10));
    assertEquals(0, db.getCommitCount());
    assertTrue(db.getRowUpdatePks().isEmpty());
    assertTrue(db.isAutoCommit());
  }

  @Test
  void repair_unknownOldKey_rejected() throws Exception {
    init(false, TABLE);
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", currentKey));

    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(12345), 10));
    assertEquals(0, db.getCommitCount());
    assertTrue(db.getRowUpdatePks().isEmpty());
  }

  @Test
  void repair_noEncryptionMetadata_rejected() {
    init(false, TABLE);

    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(1), 10));
    verifyNoInteractions(kmsClient);
  }

  @Test
  void rotate_switchConflict_noRowWrites() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    final Map<Long, byte[]> before = db.getRows();
    db.forceSwitchConflict();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    final int newKeyId = db.getLastInsertedKeyId();
    assertEquals(oldKey.id, db.getMetadataKeyId());
    assertEquals(1, db.getKeyCount(), "the unused new key row is deleted");
    assertNotNull(db.getKey(oldKey.id));
    assertTrue(exception.getMessage().contains("new key id " + newKeyId), exception.getMessage());
    assertTrue(exception.getMessage().contains("still uses key id " + oldKey.id), exception.getMessage());
    assertTrue(db.getRowUpdatePks().isEmpty());
    assertTrue(db.getRollbackCount() >= 1);
    assertTrue(db.isAutoCommit());
    assertRowsEqual(before, db.getRows());
  }

  @Test
  void rotate_emptyColumn_switchesKeyWithoutRowWrites() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);

    final DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(
        TABLE, COLUMN, PK, null, KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);

    assertEquals(0, result.getReencryptedCount());
    assertEquals(0, result.getBatchCount());
    assertTrue(result.isComplete());
    assertNotEquals(oldKey.id, db.getMetadataKeyId());
  }

  @Test
  void identifierOf64Characters_accepted() {
    init(false, TABLE);
    final String name64 = repeat('a', 64);

    // Passes the identifier check and reaches the metadata lookup, which finds no such column.
    assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, name64, PK, null, 10));
    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows(name64, COLUMN, PK, Collections.singletonList(1), 10));
    verifyNoInteractions(kmsClient);
  }

  // ---- 7. names: schema prefix, stored spelling, existence check -----------------------------

  @Test
  void rotate_schemaQualifiedName_looksUpBareMetadataName() throws Exception {
    init(true, "app.users");
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));

    final DataKeyReencryptionResult result =
        utility.rotateDataKeyAndReencrypt("app.users", COLUMN, PK, null, 10);

    assertEquals(1, result.getReencryptedCount());
    assertEquals("app.users", result.getTableName());
    assertNotEquals(oldKey.id, db.getMetadataKeyId(), "the switch matches the bare metadata name");
    assertFalse(db.getMetadataLookups().isEmpty());
    for (String[] lookup : db.getMetadataLookups()) {
      assertArrayEquals(new String[] {"users", COLUMN}, lookup);
    }
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("SELECT \"id\", \"ssn\" FROM \"app\".\"users\" WHERE 1=0")));
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("UPDATE \"app\".\"users\" SET \"ssn\" = ? WHERE \"id\" = ? AND \"ssn\" = ?")));
  }

  @Test
  void repair_schemaQualifiedName_mysql() throws Exception {
    init(false, "app.users");
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", oldKey));

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows("app.users", COLUMN, PK, Collections.singletonList(oldKey.id), 10);

    assertEquals(1, result.getReencryptedCount());
    assertDecryptsTo("a", db.getRow(1), currentKey);
    assertArrayEquals(new String[] {"users", COLUMN}, db.getMetadataLookups().get(0));
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("UPDATE `app`.`users` SET `ssn` = ? WHERE `id` = ? AND `ssn` = ?")));
    assertArrayEquals(currentKey.hmacKey, db.getUserVariable("@hmac_key_users_ssn"));
  }

  @Test
  void rotate_usesStoredNameSpelling() throws Exception {
    init(false, TABLE);
    db.setCaseInsensitiveMetadataMatch(true);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    db.putRow(2, encrypt("b", oldKey));

    final DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt("USERS", "SSN", PK, null, 10);

    assertEquals(2, result.getReencryptedCount());
    assertNotEquals(oldKey.id, db.getMetadataKeyId());
    assertTrue(db.getExecutedSql().stream().anyMatch(
        sql -> sql.equals("UPDATE `users` SET `ssn` = ? WHERE `id` = ? AND `ssn` = ?")));
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.contains("`USERS`") || sql.contains("`SSN`")));
    assertEquals(Collections.singletonList("@hmac_key_users_ssn"), db.getUserVariableCalls());
  }

  @Test
  void rotate_postgres_wrongCase_noMetadataMatch_noSideEffects() throws Exception {
    init(true, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    final Map<Long, byte[]> before = db.getRows();

    assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt("Users", COLUMN, PK, null, 10));

    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
    assertEquals(1, db.getKeyCount());
    assertEquals(oldKey.id, db.getMetadataKeyId());
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.startsWith("INSERT") || sql.startsWith("UPDATE")));
    assertRowsEqual(before, db.getRows());
  }

  @Test
  void rotate_missingPrimaryKeyColumn_failsBeforeKeyCreation() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    final Map<Long, byte[]> before = db.getRows();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, "user_id", null, 10));

    assertTrue(exception.getMessage().contains("Cannot read the columns `ssn` and `user_id` of table `users`"),
        exception.getMessage());
    assertTrue(exception.getMessage().contains("Nothing was changed"), exception.getMessage());
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
    assertEquals(1, db.getKeyCount());
    assertEquals(oldKey.id, db.getMetadataKeyId());
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.startsWith("INSERT") || sql.startsWith("UPDATE")));
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.contains(" LIMIT ?")),
        "the probe runs before the read-only pre-check");
    assertRowsEqual(before, db.getRows());
    assertTrue(db.isAutoCommit());
  }

  @Test
  void repair_missingTable_failsBeforeWrites() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    db.putRow(1, encrypt("a", oldKey));
    final Map<Long, byte[]> before = db.getRows();

    assertThrows(KeyManagementException.class,
        () -> utility.reencryptRemainingRows("other.users", COLUMN, PK, Collections.singletonList(oldKey.id), 10));

    assertEquals(0, db.getCommitCount());
    assertTrue(db.getUserVariableCalls().isEmpty());
    assertTrue(db.getRowUpdatePks().isEmpty());
    assertRowsEqual(before, db.getRows());
    assertTrue(db.isAutoCommit());
  }

  @Test
  void rotate_invalidStoredName_rejected() throws Exception {
    init(false, TABLE);
    db.setStoredNames("us-ers", COLUMN);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    assertTrue(exception.getMessage().contains("us-ers"), exception.getMessage());
    verifyNoInteractions(kmsClient);
    assertEquals(1, db.getKeyCount());
  }

  // ---- 8. failures before and after the switch -----------------------------------------------

  @Test
  void rotate_switchUpdateFails_deletesNewKeyRow() throws Exception {
    init(true, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    db.failOnSwitchUpdate();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    final int newKeyId = db.getLastInsertedKeyId();
    assertNotEquals(oldKey.id, newKeyId);
    assertEquals(1, db.getKeyCount());
    assertEquals(oldKey.id, db.getMetadataKeyId());
    assertTrue(exception.getMessage().contains("new key id " + newKeyId), exception.getMessage());
    assertTrue(exception.getMessage().contains("was deleted"), exception.getMessage());
    assertTrue(exception.getMessage().contains("still uses key id " + oldKey.id), exception.getMessage());
    assertTrue(db.getSetConfigCalls().isEmpty());
    assertTrue(db.isAutoCommit());
  }

  @Test
  void rotate_newKeyDeleteFails_messageNamesNewKeyId() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    db.failOnSwitchUpdate();
    db.failOnKeyDelete();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    final int newKeyId = db.getLastInsertedKeyId();
    assertEquals(2, db.getKeyCount());
    assertEquals(oldKey.id, db.getMetadataKeyId());
    assertTrue(exception.getMessage().contains("delete the new key id " + newKeyId), exception.getMessage());
    assertTrue(exception.getMessage().contains("delete key_storage row " + newKeyId), exception.getMessage());
    assertTrue(exception.getMessage().contains("still uses key id " + oldKey.id), exception.getMessage());
    assertTrue(db.isAutoCommit());
  }

  @Test
  void rotate_commitErrorAfterSwitchApplied_keepsNewKey() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    db.failOnCommitNumber(1, true);

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    final int newKeyId = db.getLastInsertedKeyId();
    assertEquals(2, db.getKeyCount(), "the column uses the new key, so it is kept");
    assertEquals(newKeyId, db.getMetadataKeyId());
    assertTrue(exception.getMessage().contains("after the switch to key id " + newKeyId), exception.getMessage());
    assertTrue(exception.getMessage().contains("old key id " + oldKey.id), exception.getMessage());
  }

  @Test
  void rotate_failureAfterSwitch_messageNamesOldKey() throws Exception {
    init(true, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));
    db.failOnSetConfig();

    final KeyManagementException exception = assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 10));

    final int newKeyId = db.getMetadataKeyId();
    assertNotEquals(oldKey.id, newKeyId, "the switch is committed before the trigger cache is set");
    assertEquals(2, db.getKeyCount());
    assertTrue(exception.getMessage().contains("old key id(s) [" + oldKey.id + "]"), exception.getMessage());
    assertTrue(exception.getMessage().contains("failed in batch 1"), exception.getMessage());
    assertTrue(db.getRowUpdatePks().isEmpty());
    assertTrue(db.isAutoCommit());
  }

  // ---- 9. trigger cache and statement reuse --------------------------------------------------

  @Test
  void rotate_postgres_triggerCacheSurvivesBatchRollback() throws Exception {
    init(true, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    for (long pk = 1; pk <= 4; pk++) {
      db.putRow(pk, encrypt("v" + pk, oldKey));
    }
    // The first batch fails and is rolled back; the trigger cache was committed before it.
    db.failOnExecuteBatchNumber(1);

    assertThrows(KeyManagementException.class,
        () -> utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 2));

    final TestKey newKey = keyFromDb(db.getMetadataKeyId());
    assertTrue(db.getRollbackCount() >= 1);
    assertEquals(1, db.getSetConfigCalls().size());
    assertEquals(hex(newKey.hmacKey), db.getSessionSetting("hmac_key.users.ssn"));
  }

  @Test
  void repair_postgres_setsTriggerCacheToCurrentKey() throws Exception {
    init(true, TABLE);
    final TestKey oldKey = createKey("old");
    final TestKey currentKey = createKey("current");
    db.setMetadataKeyId(currentKey.id);
    for (long pk = 1; pk <= 5; pk++) {
      db.putRow(pk, encrypt("v" + pk, oldKey));
    }

    final DataKeyReencryptionResult result =
        utility.reencryptRemainingRows(TABLE, COLUMN, PK, Collections.singletonList(oldKey.id), 2);

    assertEquals(5, result.getReencryptedCount());
    assertEquals(1, db.getSetConfigCalls().size());
    assertEquals("hmac_key.users.ssn", db.getSetConfigCalls().get(0)[0]);
    assertEquals(hex(currentKey.hmacKey), db.getSessionSetting("hmac_key.users.ssn"));
  }

  @Test
  void rotate_mysql_setsTriggerUserVariableOnce() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    for (long pk = 1; pk <= 7; pk++) {
      db.putRow(pk, encrypt("v" + pk, oldKey));
    }

    final DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 3);

    final TestKey newKey = keyFromDb(result.getCurrentKeyId());
    assertEquals(1, db.countPrepared(sql -> sql.equals("SET @hmac_key_users_ssn = ?")));
    assertArrayEquals(newKey.hmacKey, db.getUserVariable("@hmac_key_users_ssn"));
    assertTrue(db.getSetConfigCalls().isEmpty());
    assertEquals(4, db.getCommitCount(), "1 switch commit and 3 batch commits");
  }

  @Test
  void rotate_mysql_longNames_skipTriggerVariable() throws Exception {
    final String longTable = repeat('t', 60);
    init(false, longTable);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    db.putRow(1, encrypt("a", oldKey));

    final DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(longTable, COLUMN, PK, null, 10);

    assertEquals(1, result.getReencryptedCount());
    assertFalse(db.getExecutedSql().stream().anyMatch(sql -> sql.startsWith("SET @")));
    assertTrue(db.getUserVariableCalls().isEmpty());
  }

  @Test
  void rotate_preparesStatementsOnce() throws Exception {
    init(false, TABLE);
    final TestKey oldKey = createKey("old");
    db.setMetadataKeyId(oldKey.id);
    for (long pk = 1; pk <= 7; pk++) {
      db.putRow(pk, encrypt("v" + pk, oldKey));
    }

    final DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(TABLE, COLUMN, PK, null, 3);

    assertEquals(7, result.getReencryptedCount());
    assertEquals(3, result.getBatchCount());
    assertEquals(1, db.countPrepared(sql -> sql.startsWith("UPDATE `users` SET")));
    assertEquals(1, db.countPrepared(
        sql -> sql.contains(" LIMIT ?") && !sql.contains(" > ?") && sql.endsWith(" FOR UPDATE")));
    assertEquals(1, db.countPrepared(
        sql -> sql.contains(" LIMIT ?") && sql.contains(" > ?") && sql.endsWith(" FOR UPDATE")));
    assertEquals(1, db.countPrepared(
        sql -> sql.contains(" LIMIT ?") && !sql.contains(" > ?") && !sql.endsWith(" FOR UPDATE")));
    assertEquals(1, db.countPrepared(
        sql -> sql.contains(" LIMIT ?") && sql.contains(" > ?") && !sql.endsWith(" FOR UPDATE")));
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static String repeat(char c, int count) {
    final StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }

  private TestKey createKey(String name) {
    final byte[] blob = randomBytes(16);
    final byte[] dataKey = randomBytes(32);
    final byte[] hmacKey = randomBytes(32);
    kmsPlaintexts.put(base64(blob), dataKey);
    final int id = db.addKey(name, MASTER_KEY_ARN, base64(blob), hmacKey);
    return new TestKey(id, base64(blob), dataKey, hmacKey);
  }

  private TestKey keyFromDb(int id) {
    final FakeReencryptionDatabase.KeyRow row = db.getKey(id);
    assertNotNull(row);
    final byte[] dataKey = kmsPlaintexts.get(row.encryptedDataKey);
    assertNotNull(dataKey);
    return new TestKey(id, row.encryptedDataKey, dataKey, row.hmacKey);
  }

  private byte[] encrypt(Object value, TestKey key) throws Exception {
    return encryptionService.encrypt(value, key.dataKey, key.hmacKey, ALGORITHM);
  }

  private void assertDecryptsTo(Object expected, byte[] stored, TestKey key) throws Exception {
    final Object actual = encryptionService.decrypt(stored, key.dataKey, key.hmacKey, ALGORITHM, Object.class);
    if (expected instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) actual);
    } else {
      assertEquals(expected, actual);
      assertEquals(expected.getClass(), actual.getClass());
    }
  }

  private static void assertRowsEqual(Map<Long, byte[]> expected, Map<Long, byte[]> actual) {
    assertEquals(expected.keySet(), actual.keySet());
    for (Map.Entry<Long, byte[]> e : expected.entrySet()) {
      assertArrayEquals(e.getValue(), actual.get(e.getKey()), "row " + e.getKey());
    }
  }

  private static byte[] randomBytes(int length) {
    final byte[] bytes = new byte[length];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }

  private static String base64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static String hex(byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
