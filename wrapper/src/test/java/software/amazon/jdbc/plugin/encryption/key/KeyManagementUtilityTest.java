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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.model.KeyMetadata;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;

public class KeyManagementUtilityTest {

  private Connection conn;
  private PreparedStatement stmt;
  private ResultSet rs;
  private KeyManager keyManager;
  private KmsClient kmsClient;
  private MetadataManager metadataManager;
  private EncryptionConfig config;
  private KeyManagementUtility utility;

  private static final String MASTER_KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/test-master-key";

  @BeforeEach
  void setUp() throws Exception {
    conn = mock(Connection.class);
    stmt = mock(PreparedStatement.class);
    rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.executeQuery()).thenReturn(rs);
    config = EncryptionConfig.builder().kmsRegion("us-east-1").build();
    kmsClient = mock(KmsClient.class);
    metadataManager = mock(MetadataManager.class);
    keyManager = new KeyManager(kmsClient, conn, true, config);
    utility = new KeyManagementUtility(keyManager, metadataManager, conn, kmsClient, config);
  }

  @AfterEach
  void tearDown() {
    keyManager.shutdown();
  }

  @Test
  void getColumnsUsingKey_bindsTheKeyIdAsAnInteger() throws Exception {
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("table_name")).thenReturn("users", "orders");
    when(rs.getString("column_name")).thenReturn("ssn", "card_number");

    assertEquals(Arrays.asList("users.ssn", "orders.card_number"), utility.getColumnsUsingKey(3));

    verify(conn).prepareStatement(
        "SELECT table_name, column_name FROM encrypt.encryption_metadata WHERE key_id OPERATOR(pg_catalog.=) ?");
    verify(stmt).setInt(1, 3);
    verify(stmt, never()).setString(anyInt(), anyString());
  }

  @Test
  void getColumnsUsingKey_parsesAStringKeyId() throws Exception {
    utility.getColumnsUsingKey("12");

    verify(stmt).setInt(1, 12);
  }

  @Test
  void getColumnsUsingKey_refusesANonIntegerKeyId_withoutQuerying() {
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> utility.getColumnsUsingKey("some-key"));

    assertTrue(e.getMessage().contains("some-key"), e.getMessage());
    verifyNoInteractions(conn);
  }

  @ParameterizedTest
  @ValueSource(strings = {EncryptionAlgorithm.AES_128_GCM, "aes-256-gcm", "AES-512-GCM", "AES-256-GCM\n"})
  void initializeEncryptionForColumn_refusesAnyAlgorithmButAes256Gcm_beforeAnyLookupOrKmsCall(String algorithm) {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> utility.initializeEncryptionForColumn("users", "ssn", MASTER_KEY_ARN, algorithm));

    assertTrue(e.getMessage().contains("only AES-256-GCM"), e.getMessage());
    verifyNoInteractions(kmsClient, metadataManager, conn);
  }

  @Test
  void initializeEncryptionForColumn_refusalNamesTheAlgorithm_withoutControlCharacters() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> utility.initializeEncryptionForColumn("users", "ssn", MASTER_KEY_ARN, "AES-128-GCM\nSEVERE: forged"));

    assertTrue(e.getMessage().contains("AES-128-GCM"), e.getMessage());
    assertFalse(e.getMessage().contains("\n"), e.getMessage());
  }

  @Test
  void generateAndStoreDataKey_refusesAes128Gcm_beforeCallingKms() {
    assertThrows(IllegalArgumentException.class,
        () -> utility.generateAndStoreDataKey("users", "ssn", MASTER_KEY_ARN, EncryptionAlgorithm.AES_128_GCM));

    verifyNoInteractions(kmsClient, metadataManager, conn);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", " ", EncryptionAlgorithm.AES_256_GCM})
  void generateAndStoreDataKey_storesAes256Gcm_forTheDefaultOrExplicitAlgorithm(String algorithm) throws Exception {
    final KeyManager mockKeyManager = mock(KeyManager.class);
    when(mockKeyManager.generateDataKey(MASTER_KEY_ARN)).thenReturn(new KeyManager.DataKeyResult("ZW5jcnlwdGVk"));
    when(mockKeyManager.storeKeyMetadata(eq("users"), eq("ssn"), any(KeyMetadata.class))).thenReturn(7);
    when(stmt.executeUpdate()).thenReturn(1);
    final KeyManagementUtility mockedUtility =
        new KeyManagementUtility(mockKeyManager, metadataManager, conn, kmsClient, config);

    mockedUtility.generateAndStoreDataKey("users", "ssn", MASTER_KEY_ARN, algorithm);

    verify(stmt).setString(3, EncryptionAlgorithm.AES_256_GCM);
    verify(stmt).setInt(4, 7);
  }
}
