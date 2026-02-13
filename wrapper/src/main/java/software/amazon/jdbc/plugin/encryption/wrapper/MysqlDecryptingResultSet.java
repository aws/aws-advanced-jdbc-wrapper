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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;

/**
 * MySQL-specific ResultSet wrapper that automatically decrypts values.
 * Handles MySQL's VARBINARY columns directly as raw bytes.
 */
public class MysqlDecryptingResultSet extends BaseDecryptingResultSet {

  public MysqlDecryptingResultSet(
      ResultSet delegate,
      MetadataManager metadataManager,
      EncryptionService encryptionService,
      KeyManager keyManager) {
    super(delegate, metadataManager, encryptionService, keyManager);
  }

  @Override
  protected byte[] getEncryptedBytes(int columnIndex) throws SQLException {
    // MySQL: get raw bytes from VARBINARY
    return delegate.getBytes(columnIndex);
  }

  @Override
  protected byte[] getEncryptedBytes(String columnLabel) throws SQLException {
    // MySQL: get raw bytes from VARBINARY
    return delegate.getBytes(columnLabel);
  }
}
