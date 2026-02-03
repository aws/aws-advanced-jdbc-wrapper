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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

/**
 * Service for encrypting and decrypting data using AES-256-GCM algorithm. Supports multiple data
 * types and provides secure memory handling.
 */
public class EncryptionService {

  private static final Logger LOGGER = Logger.getLogger(EncryptionService.class.getName());

  // Algorithm constants
  private static final String DEFAULT_ALGORITHM = "AES-256-GCM";
  private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_IV_LENGTH = 12; // 96 bits
  private static final int GCM_TAG_LENGTH = 16; // 128 bits
  private static final String HMAC_ALGORITHM = "HmacSHA256";
  private static final int HMAC_TAG_LENGTH = 32; // 256 bits

  // Supported algorithms
  private static final String[] SUPPORTED_ALGORITHMS = {"AES-256-GCM", "AES-128-GCM"};

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
  public byte[] encrypt(Object value, byte[] dataKey, byte[] hmacKey, String algorithm)
      throws EncryptionException {
    if (value == null) {
      return null;
    }

    validateAlgorithm(algorithm);
    validateDataKey(dataKey, algorithm);

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
      buffer.put(getTypeMarker(value));
      buffer.put(iv);
      buffer.put(ciphertext);
      byte[] encryptedData = buffer.array();

      // Generate HMAC using the separate HMAC key
      Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
      hmac.init(new SecretKeySpec(hmacKey, HMAC_ALGORITHM));
      byte[] hmacTag = hmac.doFinal(encryptedData);

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
          () ->
              String.format(
                  "Encryption failed for value type: %s %s",
                  value.getClass().getSimpleName(), e.getMessage()));
      throw EncryptionException.encryptionFailed("Failed to encrypt value", e)
          .withDataType(value.getClass().getSimpleName())
          .withAlgorithm(algorithm)
          .withOperation("ENCRYPT");
    }
  }

  /**
   * Encrypts a value using the same key for both encryption and HMAC. This is a convenience method
   * for backward compatibility.
   *
   * @param value the value to encrypt
   * @param dataKey the encryption key (also used for HMAC)
   * @param algorithm the encryption algorithm to use
   * @return the encrypted data as byte array with HMAC prepended
   * @throws EncryptionException if encryption fails
   */
  public byte[] encrypt(Object value, byte[] dataKey, String algorithm) throws EncryptionException {
    return encrypt(value, dataKey, dataKey, algorithm);
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
  public Object decrypt(
      byte[] encryptedValue, byte[] dataKey, byte[] hmacKey, String algorithm, Class<?> targetType)
      throws EncryptionException {
    if (encryptedValue == null) {
      return null;
    }

    validateAlgorithm(algorithm);
    validateDataKey(dataKey, algorithm);

    if (encryptedValue.length < 32 + 1 + 12 + 16) {
      throw EncryptionException.decryptionFailed("Invalid encrypted data length", null)
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
      LOGGER.info(
          () ->
              String.format(
                  "Decrypting: hmacKey length=%d, encryptedData length=%d",
                  hmacKey != null ? hmacKey.length : 0, encryptedData.length));

      Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
      hmac.init(new SecretKeySpec(hmacKey, HMAC_ALGORITHM));
      byte[] calculatedHmacTag = hmac.doFinal(encryptedData);

      LOGGER.info(
          () ->
              String.format(
                  "HMAC comparison: stored=%s, calculated=%s",
                  bytesToHex(storedHmacTag).substring(0, 16),
                  bytesToHex(calculatedHmacTag).substring(0, 16)));

      if (!MessageDigest.isEqual(storedHmacTag, calculatedHmacTag)) {
        throw EncryptionException.decryptionFailed(
                "HMAC verification failed - data may be tampered", null)
            .withAlgorithm(algorithm)
            .withDataType(targetType.getSimpleName())
            .withOperation("VERIFY_HMAC");
      }

      // Now decrypt the verified data
      ByteBuffer dataBuffer = ByteBuffer.wrap(encryptedData);

      // Extract type marker
      final byte typeMarker = dataBuffer.get();

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
          () ->
              String.format(
                  "Decryption failed for target type: %s %s",
                  targetType.getSimpleName(), e.getMessage()));
      throw EncryptionException.decryptionFailed("Failed to decrypt value", e)
          .withDataType(targetType.getSimpleName())
          .withAlgorithm(algorithm)
          .withOperation("DECRYPT");
    }
  }

  /**
   * Decrypts encrypted data using the same key for both decryption and HMAC verification. This is a
   * convenience method for backward compatibility.
   *
   * @param encryptedValue the encrypted data with HMAC prepended
   * @param dataKey the decryption key (also used for HMAC verification)
   * @param algorithm the encryption algorithm used
   * @param targetType the expected type of the decrypted value
   * @return the decrypted value
   * @throws EncryptionException if decryption fails or HMAC verification fails
   */
  public Object decrypt(
      byte[] encryptedValue, byte[] dataKey, String algorithm, Class<?> targetType)
      throws EncryptionException {
    return decrypt(encryptedValue, dataKey, dataKey, algorithm, targetType);
  }

  /**
   * Returns the default encryption algorithm.
   *
   * @return the default algorithm name
   */
  public String getDefaultAlgorithm() {
    return DEFAULT_ALGORITHM;
  }

  /**
   * Checks if the specified algorithm is supported.
   *
   * @param algorithm the algorithm to check
   * @return true if supported, false otherwise
   */
  public boolean isAlgorithmSupported(String algorithm) {
    if (algorithm == null) {
      return false;
    }
    return Arrays.asList(SUPPORTED_ALGORITHMS).contains(algorithm);
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
      throw EncryptionException.invalidKey("Data key cannot be null").withAlgorithm(algorithm);
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
    switch (algorithm) {
      case "AES-256-GCM":
        return 32; // 256 bits
      case "AES-128-GCM":
        return 16; // 128 bits
      default:
        throw new IllegalArgumentException("Unknown algorithm: " + algorithm);
    }
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
      // Fallback to Java serialization for complex objects
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(value);
        return baos.toByteArray();
      }
    }
  }

  /** Gets a type marker byte for the value type. */
  private byte getTypeMarker(Object value) {
    if (value instanceof String) {
      return 1;
    }
    if (value instanceof Integer) {
      return 2;
    }
    if (value instanceof Long) {
      return 3;
    }
    if (value instanceof Double) {
      return 4;
    }
    if (value instanceof Float) {
      return 5;
    }
    if (value instanceof Boolean) {
      return 6;
    }
    if (value instanceof BigDecimal) {
      return 7;
    }
    if (value instanceof Date) {
      return 8;
    }
    if (value instanceof Time) {
      return 9;
    }
    if (value instanceof Timestamp) {
      return 10;
    }
    if (value instanceof LocalDate) {
      return 11;
    }
    if (value instanceof LocalTime) {
      return 12;
    }
    if (value instanceof LocalDateTime) {
      return 13;
    }
    if (value instanceof byte[]) {
      return 14;
    }
    return 99; // Generic object serialization
  }

  /** Deserializes bytes to the appropriate type. */
  private Object deserializeValue(byte[] data, byte typeMarker, Class<?> targetType)
      throws Exception {
    switch (typeMarker) {
      case 1: // String
        String str = new String(data, StandardCharsets.UTF_8);
        return convertToTargetType(str, targetType);

      case 2: // Integer
        if (data.length != 4) {
          throw EncryptionException.decryptionFailed("Invalid Integer data length", null)
              .withContext("expectedLength", 4)
              .withContext("actualLength", data.length);
        }
        int intVal = ByteBuffer.wrap(data).getInt();
        return convertToTargetType(intVal, targetType);

      case 3: // Long
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed("Invalid Long data length", null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long longVal = ByteBuffer.wrap(data).getLong();
        return convertToTargetType(longVal, targetType);

      case 4: // Double
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed("Invalid Double data length", null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        double doubleVal = ByteBuffer.wrap(data).getDouble();
        return convertToTargetType(doubleVal, targetType);

      case 5: // Float
        if (data.length != 4) {
          throw EncryptionException.decryptionFailed("Invalid Float data length", null)
              .withContext("expectedLength", 4)
              .withContext("actualLength", data.length);
        }
        float floatVal = ByteBuffer.wrap(data).getFloat();
        return convertToTargetType(floatVal, targetType);

      case 6: // Boolean
        if (data.length != 1) {
          throw EncryptionException.decryptionFailed("Invalid Boolean data length", null)
              .withContext("expectedLength", 1)
              .withContext("actualLength", data.length);
        }
        boolean boolVal = data[0] == 1;
        return convertToTargetType(boolVal, targetType);

      case 7: // BigDecimal
        String decStr = new String(data, StandardCharsets.UTF_8);
        BigDecimal decVal = new BigDecimal(decStr);
        return convertToTargetType(decVal, targetType);

      case 8: // Date
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed("Invalid Date data length", null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long dateTime = ByteBuffer.wrap(data).getLong();
        Date dateVal = new Date(dateTime);
        return convertToTargetType(dateVal, targetType);

      case 9: // Time
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed("Invalid Time data length", null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long timeTime = ByteBuffer.wrap(data).getLong();
        Time timeVal = new Time(timeTime);
        return convertToTargetType(timeVal, targetType);

      case 10: // Timestamp
        if (data.length != 8) {
          throw EncryptionException.decryptionFailed("Invalid Timestamp data length", null)
              .withContext("expectedLength", 8)
              .withContext("actualLength", data.length);
        }
        long tsTime = ByteBuffer.wrap(data).getLong();
        Timestamp tsVal = new Timestamp(tsTime);
        return convertToTargetType(tsVal, targetType);

      case 11: // LocalDate
        String ldStr = new String(data, StandardCharsets.UTF_8);
        LocalDate ldVal = LocalDate.parse(ldStr);
        return convertToTargetType(ldVal, targetType);

      case 12: // LocalTime
        String ltStr = new String(data, StandardCharsets.UTF_8);
        LocalTime ltVal = LocalTime.parse(ltStr);
        return convertToTargetType(ltVal, targetType);

      case 13: // LocalDateTime
        String ldtStr = new String(data, StandardCharsets.UTF_8);
        LocalDateTime ldtVal = LocalDateTime.parse(ldtStr);
        return convertToTargetType(ldtVal, targetType);

      case 14: // byte[]
        return convertToTargetType(data, targetType);

      case 99: // Generic object
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bais)) {
          Object obj = ois.readObject();
          return convertToTargetType(obj, targetType);
        }

      default:
        throw EncryptionException.decryptionFailed("Unknown type marker: " + typeMarker, null)
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
      LOGGER.warning(() -> "HMAC verification failed: " + e.getMessage());
      return false;
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
