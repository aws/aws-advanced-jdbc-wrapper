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
 * PostgreSQL-specific ResultSet wrapper that automatically decrypts values.
 * Handles PostgreSQL's EncryptedData type and hex-encoded text mode.
 */
public class PostgresDecryptingResultSet extends BaseDecryptingResultSet {

  public PostgresDecryptingResultSet(
      ResultSet delegate,
      MetadataManager metadataManager,
      EncryptionService encryptionService,
      KeyManager keyManager) {
    super(delegate, metadataManager, encryptionService, keyManager);
  }

  /**
   * Converts hex string to bytes.
   *
   * @param hex the hex string to convert
   * @return the byte array
   */
  private static byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte)
              ((Character.digit(hex.charAt(i), 16) << 4) + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  @Override
  protected byte[] getEncryptedBytes(int columnIndex) throws SQLException {
    Object obj = delegate.getObject(columnIndex);

    if (obj instanceof EncryptedData) {
      return ((EncryptedData) obj).getBytes();
    } else {
      // Fallback: get as bytes and decode hex if needed
      byte[] rawBytes = delegate.getBytes(columnIndex);

      if (rawBytes != null && rawBytes.length > 2
          && rawBytes[0] == '\\' && rawBytes[1] == 'x') {
        // PostgreSQL text mode: decode hex
        String hexString = new String(rawBytes, 2, rawBytes.length - 2,
            java.nio.charset.StandardCharsets.US_ASCII);
        return hexToBytes(hexString);
      } else {
        return rawBytes;
      }
    }
  }

  @Override
  protected byte[] getEncryptedBytes(String columnLabel) throws SQLException {
    Object obj = delegate.getObject(columnLabel);

    if (obj instanceof EncryptedData) {
      return ((EncryptedData) obj).getBytes();
    } else {
      // Fallback: get as bytes and decode hex if needed
      byte[] rawBytes = delegate.getBytes(columnLabel);

      if (rawBytes != null && rawBytes.length > 2
          && rawBytes[0] == '\\' && rawBytes[1] == 'x') {
        // PostgreSQL text mode: decode hex
        String hexString = new String(rawBytes, 2, rawBytes.length - 2,
            java.nio.charset.StandardCharsets.US_ASCII);
        return hexToBytes(hexString);
      } else {
        return rawBytes;
      }
    }
  }
}
