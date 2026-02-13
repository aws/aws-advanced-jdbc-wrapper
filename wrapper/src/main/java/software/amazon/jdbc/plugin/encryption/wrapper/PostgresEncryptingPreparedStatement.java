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

package software.amazon.jdbc.plugin.encryption.wrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;

/**
 * PostgreSQL-specific PreparedStatement wrapper that automatically encrypts values.
 * Uses EncryptedData custom type for encrypted columns.
 */
public class PostgresEncryptingPreparedStatement extends BaseEncryptingPreparedStatement {

  public PostgresEncryptingPreparedStatement(
      PreparedStatement delegate,
      MetadataManager metadataManager,
      EncryptionService encryptionService,
      KeyManager keyManager,
      SqlAnalysisService sqlAnalysisService,
      String sql) {
    super(delegate, metadataManager, encryptionService, keyManager, sqlAnalysisService, sql);
  }

  @Override
  protected void setEncryptedBytes(int parameterIndex, byte[] encryptedBytes)
      throws SQLException {
    // PostgreSQL: use custom type
    EncryptedData encData = new EncryptedData(encryptedBytes);
    delegate.setObject(parameterIndex, encData);
  }
}
