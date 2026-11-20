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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KeyManagementUtilityLoggingTest {

  private static final String KEY_UUID = "12345678-aaaa-bbbb-cccc-abcdefabcdef";
  private static final String FULL_ARN = "arn:aws:kms:us-east-1:444444444444:key/" + KEY_UUID;
  private static final String ROLE_ARN = "arn:aws:iam::333333333333:role/SecretRoleName";
  private static final String KMS_MESSAGE = ROLE_ARN + " is not authorized on resource: " + FULL_ARN;

  @Test
  void validateMasterKey_logsTheMaskedKeyAndTheErrorSummary_withoutTheKmsMessage() {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenThrow(accessDenied());
    final KeyManagementUtility utility = newUtility(kmsClient);

    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final KeyManagementException exception = capture(records,
        () -> assertThrows(KeyManagementException.class, () -> utility.validateMasterKey(FULL_ARN)));

    final LogRecord severe = find(records, Level.SEVERE, "Master key validation failed");
    assertMasked(severe.getMessage());
    assertTrue(severe.getMessage().contains("arn:aws:kms:***:***:key/1234***cdef"), severe.getMessage());
    // The error is kept, as a summary.
    assertTrue(severe.getMessage().contains("errorCode=AccessDeniedException, httpStatus=400"), severe.getMessage());
    assertNull(severe.getThrown());
    assertMasked(exception.getMessage());
  }

  @Test
  void validateMasterKey_masksTheKeyInTheFinestLogs() throws Exception {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(
        software.amazon.awssdk.services.kms.model.DescribeKeyResponse.builder().keyMetadata(
            software.amazon.awssdk.services.kms.model.KeyMetadata.builder()
                .enabled(true)
                .keyState(software.amazon.awssdk.services.kms.model.KeyState.ENABLED)
                .keyUsage(software.amazon.awssdk.services.kms.model.KeyUsageType.ENCRYPT_DECRYPT)
                .build()).build());
    final KeyManagementUtility utility = newUtility(kmsClient);

    final Logger logger = Logger.getLogger(KeyManagementUtility.class.getName());
    final Level previous = logger.getLevel();
    logger.setLevel(Level.ALL);
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    try {
      capture(records, () -> {
        try {
          return utility.validateMasterKey(FULL_ARN);
        } catch (KeyManagementException e) {
          throw new AssertionError(e);
        }
      });
    } finally {
      logger.setLevel(previous);
    }

    assertTrue(records.stream().anyMatch(r -> r.getMessage().startsWith("Validating master key")), records.toString());
    records.forEach(r -> assertMasked(r.getMessage()));
  }

  @Test
  void createMasterKey_logsAndThrowsTheErrorSummary_withoutTheKmsMessage() {
    final KmsClient kmsClient = mock(KmsClient.class);
    when(kmsClient.createKey(any(CreateKeyRequest.class))).thenThrow(accessDenied());
    final KeyManagementUtility utility = newUtility(kmsClient);

    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final KeyManagementException exception = capture(records,
        () -> assertThrows(KeyManagementException.class, () -> utility.createMasterKeyWithPermissions("test", null)));

    final LogRecord severe = find(records, Level.SEVERE, "Failed to create master key");
    assertMasked(severe.getMessage());
    assertTrue(severe.getMessage().contains("errorCode=AccessDeniedException"), severe.getMessage());
    assertMasked(exception.getMessage());
  }

  private static KmsException accessDenied() {
    return (KmsException) KmsException.builder()
        .message(KMS_MESSAGE)
        .statusCode(400)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDeniedException").build())
        .build();
  }

  private static void assertMasked(final String text) {
    assertFalse(text.contains(KEY_UUID), text);
    assertFalse(text.contains("444444444444"), text);
    assertFalse(text.contains("333333333333"), text);
    assertFalse(text.contains("SecretRoleName"), text);
  }

  private static LogRecord find(final List<LogRecord> records, final Level level, final String text) {
    return records.stream()
        .filter(r -> r.getLevel().equals(level) && r.getMessage().contains(text))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No " + level + " record containing '" + text + "' in " + records));
  }

  private static <T> T capture(final List<LogRecord> records, final java.util.function.Supplier<T> action) {
    final Logger logger = Logger.getLogger(KeyManagementUtility.class.getName());
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

  private static KeyManagementUtility newUtility(final KmsClient kmsClient) {
    final EncryptionConfig config = EncryptionConfig.builder().kmsRegion("us-east-1").maxRetries(0).build();
    return new KeyManagementUtility(
        mock(KeyManager.class), mock(MetadataManager.class), mock(Connection.class), kmsClient, config);
  }
}
