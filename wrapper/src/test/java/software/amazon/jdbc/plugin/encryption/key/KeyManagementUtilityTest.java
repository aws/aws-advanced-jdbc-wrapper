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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
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
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KeyManagementUtilityTest {

  private Connection conn;
  private PreparedStatement stmt;
  private ResultSet rs;
  private KeyManager keyManager;
  private KeyManagementUtility utility;

  @BeforeEach
  void setUp() throws Exception {
    conn = mock(Connection.class);
    stmt = mock(PreparedStatement.class);
    rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.executeQuery()).thenReturn(rs);
    final EncryptionConfig config = EncryptionConfig.builder().kmsRegion("us-east-1").build();
    final KmsClient kmsClient = mock(KmsClient.class);
    keyManager = new KeyManager(kmsClient, conn, true, config);
    utility = new KeyManagementUtility(keyManager, mock(MetadataManager.class), conn, kmsClient, config);
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
}
