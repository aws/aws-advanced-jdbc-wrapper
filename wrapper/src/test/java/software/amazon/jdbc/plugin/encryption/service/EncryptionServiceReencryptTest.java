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

package software.amazon.jdbc.plugin.encryption.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class EncryptionServiceReencryptTest {

  private static final String ALGORITHM = EncryptionAlgorithm.AES_256_GCM;
  private static final int HMAC_LENGTH = 32;
  private static final int MARKER_INDEX = 32;
  private static final int IV_LENGTH = 12;

  private final EncryptionService service = new EncryptionService();
  private final byte[] oldDataKey = randomBytes(32);
  private final byte[] oldHmacKey = randomBytes(32);
  private final byte[] newDataKey = randomBytes(32);
  private final byte[] newHmacKey = randomBytes(32);

  static Stream<Object> values() {
    return Stream.of(
        "123-45-6789",
        42,
        9_000_000_000L,
        Boolean.TRUE,
        new BigDecimal("12345.6789"),
        new Timestamp(1_700_000_000_000L),
        LocalDate.of(2024, 2, 29),
        new byte[] {1, 2, 3, 4, 5});
  }

  @ParameterizedTest
  @MethodSource("values")
  void reencrypt_preservesTypeMarkerAndValue(Object value) throws Exception {
    final byte[] original = service.encrypt(value, oldDataKey, oldHmacKey, ALGORITHM);

    final byte[] result = service.reencrypt(original, oldDataKey, oldHmacKey, newDataKey, newHmacKey, ALGORITHM);

    assertEquals(original[MARKER_INDEX], result[MARKER_INDEX], "type marker must be copied unchanged");
    assertFalse(Arrays.equals(
        Arrays.copyOfRange(original, MARKER_INDEX + 1, MARKER_INDEX + 1 + IV_LENGTH),
        Arrays.copyOfRange(result, MARKER_INDEX + 1, MARKER_INDEX + 1 + IV_LENGTH)),
        "a fresh IV must be used");
    assertEquals(original.length, result.length);
    assertTrue(service.verifyEncryptedData(result, newHmacKey));
    assertFalse(service.verifyEncryptedData(result, oldHmacKey));

    final Object decrypted = service.decrypt(result, newDataKey, newHmacKey, ALGORITHM, value.getClass());
    if (value instanceof byte[]) {
      assertArrayEquals((byte[]) value, (byte[]) decrypted);
    } else {
      assertEquals(value, decrypted);
    }
  }

  @Test
  void reencrypt_wrongOldHmacKey_throws() throws Exception {
    final byte[] original = service.encrypt("secret", oldDataKey, oldHmacKey, ALGORITHM);

    assertThrows(EncryptionException.class,
        () -> service.reencrypt(original, oldDataKey, randomBytes(32), newDataKey, newHmacKey, ALGORITHM));
  }

  @Test
  void reencrypt_wrongOldDataKey_throws() throws Exception {
    final byte[] original = service.encrypt("secret", oldDataKey, oldHmacKey, ALGORITHM);

    assertThrows(EncryptionException.class,
        () -> service.reencrypt(original, randomBytes(32), oldHmacKey, newDataKey, newHmacKey, ALGORITHM));
  }

  @Test
  void reencrypt_tooShortValue_throws() {
    final byte[] tooShort = new byte[HMAC_LENGTH + 1 + IV_LENGTH + 15];

    assertThrows(EncryptionException.class,
        () -> service.reencrypt(tooShort, oldDataKey, oldHmacKey, newDataKey, newHmacKey, ALGORITHM));
  }

  @Test
  void reencrypt_invalidNewKeyLength_throws() throws Exception {
    final byte[] original = service.encrypt("secret", oldDataKey, oldHmacKey, ALGORITHM);

    assertThrows(EncryptionException.class,
        () -> service.reencrypt(original, oldDataKey, oldHmacKey, new byte[16], newHmacKey, ALGORITHM));
  }

  private static byte[] randomBytes(int length) {
    final byte[] bytes = new byte[length];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }
}
