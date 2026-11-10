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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.jdbc.plugin.encryption.cache.DataKeyCache;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KeyManagerTest {

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

  private static void replaceCache(KeyManager keyManager, DataKeyCache cache) throws Exception {
    final Field field = KeyManager.class.getDeclaredField("dataKeyCache");
    field.setAccessible(true);
    ((DataKeyCache) field.get(keyManager)).shutdown();
    field.set(keyManager, cache);
  }
}
