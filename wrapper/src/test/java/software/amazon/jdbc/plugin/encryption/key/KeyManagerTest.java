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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.jdbc.plugin.encryption.cache.DataKeyCache;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KeyManagerTest {

  private static final String LISTED_ARN = "arn:aws:kms:us-east-1:111111111111:key/listed";
  private static final String UNLISTED_ARN = "arn:aws:kms:us-east-1:222222222222:key/unlisted";
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
    assertTrue(logged.contains("arn:aws:kms:***:***:key/evil?SEVERE: forged line"), logged);
    // The exception carries the same masked message as the log.
    assertEquals(logged, exception.getMessage());
    assertEquals(KeyManagementException.UNAUTHORIZED_MASTER_KEY_STATE, exception.getSQLState());
  }

  @Test
  void sanitizeArn_keepsOnlyTheKeyId() {
    assertEquals("arn:aws:kms:***:***:key/listed", KeyManagementException.sanitizeArn(LISTED_ARN));
    assertEquals("arn:aws:kms:***:***:key/***", KeyManagementException.sanitizeArn("not-an-arn"));
    assertEquals("arn:aws:kms:***:***:key/a?b", KeyManagementException.sanitizeArn("key/a\rb"));
    assertEquals(null, KeyManagementException.sanitizeArn(null));
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
}
