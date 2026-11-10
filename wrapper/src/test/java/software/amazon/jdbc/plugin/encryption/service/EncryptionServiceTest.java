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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.stream.Stream;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EncryptionServiceTest {

  private static final String ALGORITHM = EncryptionAlgorithm.AES_256_GCM;
  private static final int HMAC_LENGTH = 32;
  private static final String SECRET = "secret-plaintext-123-45-6789";

  private final EncryptionService service = new EncryptionService();
  private final byte[] dataKey = randomBytes(32);
  private final byte[] hmacKey = randomBytes(32);

  static Stream<Arguments> unconvertibleTargets() {
    return Stream.of(
        Arguments.of(byte[].class),
        Arguments.of(Integer.class),
        Arguments.of(Long.class),
        Arguments.of(Double.class),
        Arguments.of(Float.class),
        Arguments.of(BigDecimal.class));
  }

  @ParameterizedTest
  @MethodSource("unconvertibleTargets")
  void decrypt_conversionFailure_doesNotExposePlaintext(Class<?> targetType) throws Exception {
    final byte[] encrypted = service.encrypt(SECRET, dataKey, hmacKey, ALGORITHM);

    final EncryptionException e = assertThrows(EncryptionException.class,
        () -> service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, targetType));

    assertEquals(EncryptionException.TYPE_CONVERSION_FAILED_STATE, e.getSQLState());
    assertDecryptContext(e, "DECRYPT", targetType);
    assertNoPlaintext(e, SECRET);
  }

  static Stream<Arguments> textMarkers() {
    return Stream.of(
        Arguments.of(TypeMarker.BIG_DECIMAL, BigDecimal.class),
        Arguments.of(TypeMarker.LOCAL_DATE, LocalDate.class),
        Arguments.of(TypeMarker.LOCAL_TIME, LocalTime.class),
        Arguments.of(TypeMarker.LOCAL_DATE_TIME, LocalDateTime.class));
  }

  @ParameterizedTest
  @MethodSource("textMarkers")
  void decrypt_deserializationFailure_doesNotExposePlaintext(TypeMarker marker, Class<?> targetType)
      throws Exception {
    final byte[] encrypted = withTypeMarker(service.encrypt(SECRET, dataKey, hmacKey, ALGORITHM), marker);

    final EncryptionException e = assertThrows(EncryptionException.class,
        () -> service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, targetType));

    assertEquals(EncryptionException.TYPE_CONVERSION_FAILED_STATE, e.getSQLState());
    assertDecryptContext(e, "DECRYPT", targetType);
    assertNoPlaintext(e, SECRET);
  }

  @Test
  void decrypt_invalidValueLength_keepsDecryptContext() throws Exception {
    final byte[] encrypted = withTypeMarker(service.encrypt(SECRET, dataKey, hmacKey, ALGORITHM), TypeMarker.INTEGER);

    final EncryptionException e = assertThrows(EncryptionException.class,
        () -> service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, Integer.class));

    assertEquals(EncryptionException.DECRYPTION_FAILED_STATE, e.getSQLState());
    assertEquals(4, e.getErrorContext().get("expectedLength"));
    assertDecryptContext(e, "DECRYPT", Integer.class);
  }

  @Test
  void decrypt_byteArrayToUnsupportedType_failsAsTypeConversion() throws Exception {
    final byte[] encrypted = service.encrypt(new byte[] {1, 2, 3}, dataKey, hmacKey, ALGORITHM);

    final EncryptionException e = assertThrows(EncryptionException.class,
        () -> service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, Integer.class));

    assertEquals(EncryptionException.TYPE_CONVERSION_FAILED_STATE, e.getSQLState());
  }

  @Test
  void decrypt_tamperedValue_reportsHmacFailure() throws Exception {
    final byte[] encrypted = service.encrypt(SECRET, dataKey, hmacKey, ALGORITHM);
    encrypted[encrypted.length - 1] ^= 1;

    final EncryptionException e = assertThrows(EncryptionException.class,
        () -> service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, String.class));

    assertEquals(EncryptionException.DECRYPTION_FAILED_STATE, e.getSQLState());
    assertDecryptContext(e, "VERIFY_HMAC", String.class);
  }

  private static void assertDecryptContext(EncryptionException e, String operation, Class<?> targetType) {
    assertEquals(ALGORITHM, e.getErrorContext().get("algorithm"));
    assertEquals(operation, e.getErrorContext().get("operation"));
    assertEquals(targetType.getSimpleName(), e.getErrorContext().get("dataType"));
  }

  @Test
  void encrypt_leavesCallerByteArrayIntact() throws Exception {
    final byte[] value = {1, 2, 3, 4, 5};

    final byte[] encrypted = service.encrypt(value, dataKey, hmacKey, ALGORITHM);

    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, value);
    assertArrayEquals(value, (byte[]) service.decrypt(encrypted, dataKey, hmacKey, ALGORITHM, byte[].class));
  }

  @Test
  void encrypt_failure_leavesCallerByteArrayIntact() {
    final byte[] value = {1, 2, 3, 4, 5};

    assertThrows(EncryptionException.class, () -> service.encrypt(value, dataKey, null, ALGORITHM));

    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, value);
  }

  /** Fails if the secret appears anywhere in the exception chain: messages, context or stack trace. */
  private static void assertNoPlaintext(Throwable thrown, String secret) {
    final StringWriter trace = new StringWriter();
    thrown.printStackTrace(new PrintWriter(trace));
    assertFalse(trace.toString().contains(secret), () -> "stack trace exposes plaintext:\n" + trace);

    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof EncryptionException) {
        final EncryptionException e = (EncryptionException) t;
        final String detailed = String.valueOf(e.getDetailedMessage());
        assertFalse(detailed.contains(secret), () -> "detailed message exposes plaintext: " + detailed);
        e.getErrorContext().forEach((key, value) -> assertFalse(String.valueOf(value).contains(secret),
            () -> "context entry " + key + " exposes plaintext: " + value));
      }
    }
  }

  /** Replaces the type marker of an encrypted value and re-signs it, as a buggy writer might. */
  private byte[] withTypeMarker(byte[] encrypted, TypeMarker marker) throws Exception {
    final byte[] result = encrypted.clone();
    result[HMAC_LENGTH] = marker.getValue();
    final Mac hmac = Mac.getInstance("HmacSHA256");
    hmac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
    final byte[] tag = hmac.doFinal(Arrays.copyOfRange(result, HMAC_LENGTH, result.length));
    System.arraycopy(tag, 0, result, 0, HMAC_LENGTH);
    return result;
  }

  private static byte[] randomBytes(int length) {
    final byte[] bytes = new byte[length];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }
}
