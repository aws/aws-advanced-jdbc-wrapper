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

package software.amazon.jdbc.plugin.encryption.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class MetadataManagerTest {

  private AutoCloseable closeable;
  private MetadataManager manager;

  @Mock Connection mockConnection;
  @Mock PreparedStatement mockStatement;
  @Mock ResultSet mockResultSet;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
    when(mockStatement.executeQuery()).thenReturn(mockResultSet);
  }

  @AfterEach
  void cleanUp() throws Exception {
    if (manager != null) {
      manager.shutdown();
    }
    closeable.close();
  }

  private MetadataManager createManager(boolean cacheEnabled) {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .cacheEnabled(cacheEnabled)
        .metadataRefreshInterval(Duration.ZERO)
        .build();
    manager = new MetadataManager(mockConnection, config);
    return manager;
  }

  @Test
  void hasEncryptedColumns_nullTable_returnsFalseWithoutQuery() throws Exception {
    MetadataManager mm = createManager(false);

    assertFalse(mm.hasEncryptedColumns(null));
    verify(mockConnection, times(0)).prepareStatement(anyString());
  }

  @Test
  void hasEncryptedColumns_database_trueWhenRowExists() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(true);

    assertTrue(mm.hasEncryptedColumns("users"));
    verify(mockStatement).setString(1, "users");
  }

  @Test
  void hasEncryptedColumns_database_falseWhenNoRow() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(false);

    assertFalse(mm.hasEncryptedColumns("orders"));
    verify(mockStatement).setString(1, "orders");
  }

  @Test
  void hasEncryptedColumns_database_sqlExceptionBecomesMetadataException() throws Exception {
    MetadataManager mm = createManager(false);
    SQLException cause = new SQLException("boom");
    when(mockStatement.executeQuery()).thenThrow(cause);

    MetadataException e = assertThrows(MetadataException.class,
        () -> mm.hasEncryptedColumns("users"));
    assertSame(cause, e.getCause());
    assertTrue(e.getMessage().contains("users"));
  }

  @Test
  void getEncryptedTableNames_database_returnsRows() throws Exception {
    MetadataManager mm = createManager(false);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("table_name")).thenReturn("users", "orders");

    assertEquals(new HashSet<>(Arrays.asList("users", "orders")),
        mm.getEncryptedTableNames());
  }

  @Test
  void getEncryptedTableNames_database_sqlExceptionBecomesMetadataException() throws Exception {
    MetadataManager mm = createManager(false);
    SQLException cause = new SQLException("boom");
    when(mockConnection.prepareStatement(anyString())).thenThrow(cause);

    MetadataException e = assertThrows(MetadataException.class, mm::getEncryptedTableNames);
    assertSame(cause, e.getCause());
  }

  @Test
  void cacheEnabled_answersFromCacheWithoutFurtherQueries() throws Exception {
    final MetadataManager mm = createManager(true);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("table_name")).thenReturn("users");
    when(mockResultSet.getString("column_name")).thenReturn("ssn");
    when(mockResultSet.getString("encryption_algorithm")).thenReturn("AES-256-GCM");
    when(mockResultSet.getString("name")).thenReturn("test-key");
    when(mockResultSet.getString("master_key_arn")).thenReturn("arn:aws:kms:us-east-1:123:key/abc");
    when(mockResultSet.getString("encrypted_data_key")).thenReturn("encrypted-key");
    when(mockResultSet.getString("key_spec")).thenReturn("AES_256");

    mm.initialize();
    verify(mockConnection, times(1)).prepareStatement(anyString());

    assertTrue(mm.hasEncryptedColumns("users"));
    assertFalse(mm.hasEncryptedColumns("orders"));
    assertFalse(mm.hasEncryptedColumns("use"));
    Set<String> tableNames = mm.getEncryptedTableNames();
    assertEquals(Collections.singleton("users"), tableNames);

    verify(mockConnection, times(1)).prepareStatement(anyString());
  }
}
