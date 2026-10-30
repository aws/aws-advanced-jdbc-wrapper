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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Logger;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;

/**
 * Service for encrypting and decrypting data using AES-256-GCM algorithm. Supports multiple data
 * types and provides secure memory handling.
 */
public class EncryptionService {

  private static final Logger LOGGER = Logger.getLogger(EncryptionService.class.getName());

  // Algorithm constants
  private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_IV_LENGTH = 12; // 96 bits
  private static final int GCM_TAG_LENGTH = 16; // 128 bits
  private static final String HMAC_ALGORITHM = "HmacSHA256";
  private static final int HMAC_TAG_LENGTH = 32; // 256 bits

  private final SecureRandom secureRandom;

  /** Creates a new EncryptionService instance. */
  public EncryptionService() {
    this.secureRandom = new SecureRandom();
  }

  /**
   * Encrypts a value using the specified data key and algorithm.
   *
   * @param value the value to encrypt
   * @param dataKey the encryption key
   * @param hmacKey the HMAC verification key
   * @param algorithm the encryption algorithm to use
   * @return the encrypted data as byte array with HMAC prepended
   * @throws EncryptionException if encryption fails
   */
  public byte @Nullable [] encrypt(
      final @Nullable Object value, byte[] dataKey, final byte @Nullable [] hmacKey,
      String algorithm)
      throws EncryptionException {
    if (value == null) {
      return null;
    }

    validateAlgorithm(algorithm);
    validateDataKey(dataKey, algorithm);

    LOGGER.finest(() -> Messages.get("EncryptionService.encrypting"));

    try {
      // Convert value to bytes based on type
      byte[] plaintext = serializeValue(value);

      // Generate random IV
      byte[] iv = new byte[GCM_IV_LENGTH];
      secureRandom.nextBytes(iv);

      // Create cipher
      Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      SecretKeySpec keySpec = new SecretKeySpec(dataKey, "AES");
      GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
      cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);

      // Encrypt the data
      byte[] ciphertext = cipher.doFinal(plaintext);

      // Combine type marker + IV + ciphertext
      ByteBuffer buffer = ByteBuffer.allocate(1 + iv.length + ciphertext.length);
      buffer.put(TypeMarker.fromObject(value).getValue());
      buffer.put(iv);
      buffer.put(ciphertext);
      byte[] encryptedData = buffer.array();

      // Generate HMAC using the separate HMAC key
      Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
      if (hmacKey == null) {
        // Same effect as before: SecretKeySpec rejected a null key with IllegalArgumentException,
        // which the catch block below turns into an EncryptionException.
        throw new IllegalArgumentException(Messages.get("EncryptionService.hmacKeyNull"));
      }
      final byte[] hmacKeyBytes = hmacKey;
      hmac.init(new SecretKeySpec(hmacKeyBytes, HMAC_ALGORITHM));
      byte[] hmacTag = hmac.doFinal(encryptedData);

      LOGGER.finest(
          () -> Messages.get("EncryptionService.encrypted",
              new Object[]{hmacKeyBytes.length, encryptedData.length}));

      // Prepend HMAC tag to encrypted data: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
      ByteBuffer finalBuffer = ByteBuffer.allocate(HMAC_TAG_LENGTH + encryptedData.length);
      finalBuffer.put(hmacTag);
      finalBuffer.put(encryptedData);

      // Clear sensitive data
      Arrays.fill(plaintext, (byte) 0);
      Arrays.fill(iv, (byte) 0);

      return finalBuffer.array();

    } catch (Exception e) {
      LOGGER.severe(
          () -> Messages.get("EncryptionService.encryptionFailed"));
      throw EncryptionException.encryptionFailed(Messages.get("EncryptionService.encryptFailed"), e)
          .withDataType(value.getClass().getSimpleName())
          .withAlgorithm(algorithm)
          .withOperation("ENCRYPT");
    }
  }

  /**
   * Decrypts encrypted data using the specified data key and algorithm.
   *
   * @param encryptedValue the encrypted data with HMAC prepended
   * @param dataKey the decryption key
   * @param hmacKey the HMAC verification key
   * @param algorithm the encryption algorithm used
   * @param targetType the expected type of the decrypted value
   * @return the decrypted value
   * @throws EncryptionException if decryption fails or HMAC verification fails
   */
  public @Nullable Object decrypt(
      final byte @Nullable [] encryptedValue, byte[] dataKey, final byte @Nullable [] hmacKey,
      String algorithm, Class<?> targetType)
      throws EncryptionException {
    if (encryptedValue == null) {
      return null;
    }

    validateAlgorithm(algorithm);
    validateDataKey(dataKey, algorithm);

    LOGGER.finest(() -> Messages.get("EncryptionService.decrypting"));

    if (encryptedValue.length < 32 + 1 + 12 + 16) {
      throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidEncryptedLength"), null)
          .withAlgorithm(algorithm)
          .withDataType(targetType.getSimpleName())
          .withContext("dataLength", encryptedValue.length)
          .withContext("minimumLength", 32 + 1 + 12 + 16);
    }

    try {
      ByteBuffer buffer = ByteBuffer.wrap(encryptedValue);

      // Extract HMAC tag (first 32 bytes)
      byte[] storedHmacTag = new byte[32];
      buffer.get(storedHmacTag);

      // Extract encrypted data (everything after HMAC)
      byte[] encryptedData = new byte[buffer.remaining()];
      buffer.get(encryptedData);

      // Verify HMAC using the separate HMAC key
      LOGGER.finest(
          () -> Messages.get("EncryptionService.decrypted",
              new Object[]{hmacKey != null ? hmacKey.length : 0, encryptedData.length}));

      Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
      if (hmacKey == null) {
        // Same effect as before: SecretKeySpec rejected a null key with IllegalArgumentException,
        // which the catch block below turns into an EncryptionException.
        throw new IllegalArgumentException(Messages.get("EncryptionService.hmacKeyNull"));
      }
      hmac.init(new SecretKeySpec(hmacKey, HMAC_ALGORITHM));
      byte[] calculatedHmacTag = hmac.doFinal(encryptedData);

      LOGGER.finest(() -> Messages.get("EncryptionService.verifyingHmac"));

      if (!MessageDigest.isEqual(storedHmacTag, calculatedHmacTag)) {
        throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.hmacTampered"), null)
            .withAlgorithm(algorithm)
            .withDataType(targetType.getSimpleName())
            .withOperation("VERIFY_HMAC");
      }

      // Now decrypt the verified data
      ByteBuffer dataBuffer = ByteBuffer.wrap(encryptedData);

      // Extract type marker
      final TypeMarker typeMarker = TypeMarker.fromValue(dataBuffer.get());

      // Extract IV
      byte[] iv = new byte[GCM_IV_LENGTH];
      dataBuffer.get(iv);

      // Extract ciphertext
      byte[] ciphertext = new byte[dataBuffer.remaining()];
      dataBuffer.get(ciphertext);

      // Create cipher
      Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      SecretKeySpec keySpec = new SecretKeySpec(dataKey, "AES");
      GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
      cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);

      // Decrypt the data
      byte[] plaintext = cipher.doFinal(ciphertext);

      // Deserialize based on type marker and target type
      Object result = deserializeValue(plaintext, typeMarker, targetType);

      // Clear sensitive data
      Arrays.fill(plaintext, (byte) 0);
      Arrays.fill(iv, (byte) 0);

      return result;

    } catch (Exception e) {
      LOGGER.severe(
          () -> Messages.get("EncryptionService.decryptionFailed"));
      throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.decryptFailed"), e)
          .withDataType(targetType.getSimpleName())
          .withAlgorithm(algorithm)
          .withOperation("DECRYPT");
    }
  }

  /**
   * Returns the default encryption algorithm.
   *
   * @return the default algorithm name
   */
  public String getDefaultAlgorithm() {
    return EncryptionAlgorithm.AES_256_GCM;
  }

  /**
   * Checks if the specified algorithm is supported.
   *
   * @param algorithm the algorithm to check
   * @return true if supported, false otherwise
   */
  public boolean isAlgorithmSupported(String algorithm) {
    return EncryptionAlgorithm.isSupported(algorithm);
  }

  /** Validates that the algorithm is supported. */
  private void validateAlgorithm(String algorithm) throws EncryptionException {
    if (!isAlgorithmSupported(algorithm)) {
      throw EncryptionException.invalidAlgorithm(algorithm);
    }
  }

  /** Validates the data key for the specified algorithm. */
  private void validateDataKey(byte[] dataKey, String algorithm) throws EncryptionException {
    if (dataKey == null) {
      throw EncryptionException.invalidKey(Messages.get("EncryptionService.dataKeyNull")).withAlgorithm(algorithm);
    }

    int expectedKeyLength = getExpectedKeyLength(algorithm);
    if (dataKey.length != expectedKeyLength) {
      throw EncryptionException.invalidKey(
              String.format(
                  "Invalid key length for %s: expected %d bytes, got %d",
                  algorithm, expectedKeyLength, dataKey.length))
          .withAlgorithm(algorithm)
          .withContext("expectedLength", expectedKeyLength)
          .withContext("actualLength", dataKey.length);
    }
  }

  /** Gets the expected key length for the algorithm. */
  private int getExpectedKeyLength(String algorithm) {
    return EncryptionAlgorithm.getKeyLength(algorithm);
  }

  /** Serializes a value to bytes based on its type. */
  private byte[] serializeValue(Object value) throws Exception {
    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof Integer) {
      return ByteBuffer.allocate(4).putInt((Integer) value).array();
    } else if (value instanceof Long) {
      return ByteBuffer.allocate(8).putLong((Long) value).array();
    } else if (value instanceof Double) {
      return ByteBuffer.allocate(8).putDouble((Double) value).array();
    } else if (value instanceof Float) {
      return ByteBuffer.allocate(4).putFloat((Float) value).array();
    } else if (value instanceof Boolean) {
      return new byte[] {(Boolean) value ? (byte) 1 : (byte) 0};
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toString().getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof Date) {
      return ByteBuffer.allocate(8).putLong(((Date) value).getTime()).array();
    } else if (value instanceof Time) {
      return ByteBuffer.allocate(8).putLong(((Time) value).getTime()).array();
    } else if (value instanceof Timestamp) {
      return ByteBuffer.allocate(8).putLong(((Timestamp) value).getTime()).array();
    } else if (value instanceof LocalDate) {
      return ((LocalDate) value).toString().getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof LocalTime) {
      return ((LocalTime) value).toString().getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toString().getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof byte[]) {
      return (byte[]) value;
    } else {
      throw EncryptionException.encryptionFailed(Messages.get("EncryptionService.unhandledType"), null);
    }
  }

  /** Deserializes bytes to the appropriate type. */
  private Object deserializeValue(byte[] data, TypeMarker typeMarker, Class<?> targetType)
      throws Exception {
    switch (typeMarker) {
      case STRING:
        String str = new String(data, StandardCharsets.UTF_8);
        return convertToTargetType(str, targetType);

      case INTEGER:
        if (data.length != 4) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidIntegerLength"), null)
              .withContext("expectedLength", 4)
              .withContext("actualLength", data.length);
        }
        int intVal = ByteBuffer.wrap(data).getInt();
        return convertToTargetType(intVal, targetType);

      case LONG:
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidLongLength"), null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long longVal = ByteBuffer.wrap(data).getLong();
        return convertToTargetType(longVal, targetType);

      case DOUBLE:
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidDoubleLength"), null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        double doubleVal = ByteBuffer.wrap(data).getDouble();
        return convertToTargetType(doubleVal, targetType);

      case FLOAT:
        if (data.length != 4) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidFloatLength"), null)
              .withContext("expectedLength", 4)
              .withContext("actualLength", data.length);
        }
        float floatVal = ByteBuffer.wrap(data).getFloat();
        return convertToTargetType(floatVal, targetType);

      case BOOLEAN:
        if (data.length != 1) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidBooleanLength"), null)
              .withContext("expectedLength", 1)
              .withContext("actualLength", data.length);
        }
        boolean boolVal = data[0] == 1;
        return convertToTargetType(boolVal, targetType);

      case BIG_DECIMAL:
        String decStr = new String(data, StandardCharsets.UTF_8);
        BigDecimal decVal = new BigDecimal(decStr);
        return convertToTargetType(decVal, targetType);

      case DATE:
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidDateLength"), null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long dateTime = ByteBuffer.wrap(data).getLong();
        Date dateVal = new Date(dateTime);
        return convertToTargetType(dateVal, targetType);

      case TIME:
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidTimeLength"), null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long timeTime = ByteBuffer.wrap(data).getLong();
        Time timeVal = new Time(timeTime);
        return convertToTargetType(timeVal, targetType);

      case TIMESTAMP:
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidTimestampLength"), null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long tsTime = ByteBuffer.wrap(data).getLong();
        Timestamp tsVal = new Timestamp(tsTime);
        return convertToTargetType(tsVal, targetType);

      case LOCAL_DATE:
        String ldStr = new String(data, StandardCharsets.UTF_8);
        LocalDate ldVal = LocalDate.parse(ldStr);
        return convertToTargetType(ldVal, targetType);

      case LOCAL_TIME:
        String ltStr = new String(data, StandardCharsets.UTF_8);
        LocalTime ltVal = LocalTime.parse(ltStr);
        return convertToTargetType(ltVal, targetType);

      case LOCAL_DATE_TIME:
        String ldtStr = new String(data, StandardCharsets.UTF_8);
        LocalDateTime ldtVal = LocalDateTime.parse(ldtStr);
        return convertToTargetType(ldtVal, targetType);

      case BYTE_ARRAY:
        return convertToTargetType(data, targetType);

      default:
        throw EncryptionException.decryptionFailed(
            Messages.get("EncryptionService.unknownTypeMarker", new Object[]{typeMarker}), null)
            .withContext("typeMarker", typeMarker);
    }
  }

  /** Converts a value to the target type if possible. */
  private Object convertToTargetType(Object value, Class<?> targetType) throws EncryptionException {
    if (value == null || targetType == null) {
      return value;
    }

    // If already the correct type, return as-is
    if (targetType.isAssignableFrom(value.getClass())) {
      return value;
    }

    // Handle Object.class target type (return as-is)
    if (targetType == Object.class) {
      return value;
    }

    // Handle String conversions
    if (targetType == String.class) {
      return value.toString();
    }

    // Handle byte array conversions
    if (targetType == byte[].class) {
      if (value instanceof String) {
        // Assume base64 encoded string, decode it
        try {
          return Base64.getDecoder().decode((String) value);
        } catch (IllegalArgumentException e) {
          throw EncryptionException.typeConversionFailed("String", "byte[]", e)
              .withContext(
                  "stringValue",
                  value.toString().length() > 50
                      ? value.toString().substring(0, 47) + "..."
                      : value.toString());
        }
      } else if (value instanceof byte[]) {
        return value;
      }
    }

    // Handle numeric conversions
    if (value instanceof Number) {
      Number num = (Number) value;
      if (targetType == Integer.class || targetType == int.class) {
        return num.intValue();
      } else if (targetType == Long.class || targetType == long.class) {
        return num.longValue();
      } else if (targetType == Double.class || targetType == double.class) {
        return num.doubleValue();
      } else if (targetType == Float.class || targetType == float.class) {
        return num.floatValue();
      } else if (targetType == BigDecimal.class) {
        return BigDecimal.valueOf(num.doubleValue());
      }
    }

    // Handle String to numeric conversions
    if (value instanceof String) {
      String str = (String) value;
      try {
        if (targetType == Integer.class || targetType == int.class) {
          return Integer.valueOf(str);
        } else if (targetType == Long.class || targetType == long.class) {
          return Long.valueOf(str);
        } else if (targetType == Double.class || targetType == double.class) {
          return Double.valueOf(str);
        } else if (targetType == Float.class || targetType == float.class) {
          return Float.valueOf(str);
        } else if (targetType == BigDecimal.class) {
          return new BigDecimal(str);
        } else if (targetType == Boolean.class || targetType == boolean.class) {
          return Boolean.valueOf(str);
        }
      } catch (NumberFormatException e) {
        throw EncryptionException.typeConversionFailed("String", targetType.getSimpleName(), e)
            .withContext("stringValue", str.length() > 50 ? str.substring(0, 47) + "..." : str);
      }
    }

    // If no conversion is possible, throw an exception
    throw EncryptionException.typeConversionFailed(
        value.getClass().getSimpleName(), targetType.getSimpleName(), null);
  }

  /**
   * Verifies that encrypted data has not been tampered with, without decrypting it. This method
   * only requires the HMAC key, not the encryption key or decryption permission.
   *
   * @param encryptedValue the encrypted data with HMAC prepended
   * @param hmacKey the HMAC verification key
   * @return true if HMAC verification passes, false otherwise
   */
  public boolean verifyEncryptedData(byte[] encryptedValue, byte[] hmacKey) {
    if (encryptedValue == null || hmacKey == null) {
      return false;
    }

    if (encryptedValue.length < HMAC_TAG_LENGTH + 1 + GCM_IV_LENGTH + GCM_TAG_LENGTH) {
      return false;
    }

    try {
      ByteBuffer buffer = ByteBuffer.wrap(encryptedValue);

      // Extract stored HMAC tag
      byte[] storedHmacTag = new byte[HMAC_TAG_LENGTH];
      buffer.get(storedHmacTag);

      // Extract encrypted data
      byte[] encryptedData = new byte[buffer.remaining()];
      buffer.get(encryptedData);

      // Calculate HMAC using the HMAC key
      Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
      hmac.init(new SecretKeySpec(hmacKey, HMAC_ALGORITHM));
      byte[] calculatedHmacTag = hmac.doFinal(encryptedData);

      // Verify
      return MessageDigest.isEqual(storedHmacTag, calculatedHmacTag);

    } catch (Exception e) {
      LOGGER.warning(() -> Messages.get("EncryptionService.hmacVerificationFailed", new Object[]{e.getMessage()}));
      return false;
    }
  }

  /**
   * Re-encrypts a stored value from one data key to another without deserializing it. Verifies the
   * HMAC with {@code oldHmacKey}, decrypts the AES-GCM payload with {@code oldDataKey}, and
   * encrypts the same plaintext bytes with {@code newDataKey} and a fresh random IV. The type marker
   * byte is copied unchanged (it is not parsed, so the Java type of the value is not needed). The
   * result has the normal layout [HMAC-SHA256 32][type marker 1][IV 12][ciphertext + tag 16] with an
   * HMAC computed with {@code newHmacKey}.
   *
   * @param encryptedValue the stored value, encrypted under the old keys
   * @param oldDataKey the data key the value is encrypted with
   * @param oldHmacKey the HMAC key the value is authenticated with
   * @param newDataKey the data key to encrypt the value with
   * @param newHmacKey the HMAC key to authenticate the result with
   * @param algorithm the encryption algorithm of both keys
   * @return the value encrypted under the new keys
   * @throws EncryptionException if the value is too short, the HMAC does not verify with
   *     {@code oldHmacKey}, GCM authentication fails, or a key or the algorithm is invalid
   */
  public byte[] reencrypt(
      byte[] encryptedValue, byte[] oldDataKey, byte[] oldHmacKey,
      byte[] newDataKey, byte[] newHmacKey, String algorithm)
      throws EncryptionException {
    validateAlgorithm(algorithm);
    validateDataKey(oldDataKey, algorithm);
    validateDataKey(newDataKey, algorithm);
    if (oldHmacKey == null || newHmacKey == null) {
      throw EncryptionException.invalidKey(Messages.get("EncryptionService.hmacKeyNull"))
          .withAlgorithm(algorithm)
          .withOperation("REENCRYPT");
    }

    final int minimumLength = HMAC_TAG_LENGTH + 1 + GCM_IV_LENGTH + GCM_TAG_LENGTH;
    if (encryptedValue == null || encryptedValue.length < minimumLength) {
      throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.invalidEncryptedLength"), null)
          .withAlgorithm(algorithm)
          .withOperation("REENCRYPT")
          .withContext("dataLength", encryptedValue == null ? 0 : encryptedValue.length)
          .withContext("minimumLength", minimumLength);
    }

    byte @Nullable [] plaintext = null;
    final byte[] oldIv = new byte[GCM_IV_LENGTH];
    final byte[] newIv = new byte[GCM_IV_LENGTH];
    try {
      final byte[] storedHmacTag = Arrays.copyOfRange(encryptedValue, 0, HMAC_TAG_LENGTH);
      final byte[] encryptedData = Arrays.copyOfRange(encryptedValue, HMAC_TAG_LENGTH, encryptedValue.length);

      final Mac oldHmac = Mac.getInstance(HMAC_ALGORITHM);
      oldHmac.init(new SecretKeySpec(oldHmacKey, HMAC_ALGORITHM));
      if (!MessageDigest.isEqual(storedHmacTag, oldHmac.doFinal(encryptedData))) {
        throw EncryptionException.decryptionFailed(Messages.get("EncryptionService.hmacTampered"), null)
            .withAlgorithm(algorithm)
            .withOperation("REENCRYPT");
      }

      final byte typeMarker = encryptedData[0];
      System.arraycopy(encryptedData, 1, oldIv, 0, GCM_IV_LENGTH);
      final byte[] oldCiphertext = Arrays.copyOfRange(encryptedData, 1 + GCM_IV_LENGTH, encryptedData.length);

      final Cipher decryptCipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      decryptCipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(oldDataKey, "AES"),
          new GCMParameterSpec(GCM_TAG_LENGTH * 8, oldIv));
      plaintext = decryptCipher.doFinal(oldCiphertext);

      secureRandom.nextBytes(newIv);
      final Cipher encryptCipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      encryptCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(newDataKey, "AES"),
          new GCMParameterSpec(GCM_TAG_LENGTH * 8, newIv));
      final byte[] newCiphertext = encryptCipher.doFinal(plaintext);

      final ByteBuffer dataBuffer = ByteBuffer.allocate(1 + GCM_IV_LENGTH + newCiphertext.length);
      dataBuffer.put(typeMarker);
      dataBuffer.put(newIv);
      dataBuffer.put(newCiphertext);
      final byte[] newEncryptedData = dataBuffer.array();

      final Mac newHmac = Mac.getInstance(HMAC_ALGORITHM);
      newHmac.init(new SecretKeySpec(newHmacKey, HMAC_ALGORITHM));
      final byte[] newHmacTag = newHmac.doFinal(newEncryptedData);

      final ByteBuffer finalBuffer = ByteBuffer.allocate(HMAC_TAG_LENGTH + newEncryptedData.length);
      finalBuffer.put(newHmacTag);
      finalBuffer.put(newEncryptedData);
      return finalBuffer.array();

    } catch (EncryptionException e) {
      throw e;
    } catch (Exception e) {
      throw EncryptionException.encryptionFailed(Messages.get("EncryptionService.reencryptFailed"), e)
          .withAlgorithm(algorithm)
          .withOperation("REENCRYPT");
    } finally {
      if (plaintext != null) {
        Arrays.fill(plaintext, (byte) 0);
      }
      Arrays.fill(oldIv, (byte) 0);
      Arrays.fill(newIv, (byte) 0);
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
