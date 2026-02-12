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

import java.sql.SQLException;
import org.postgresql.util.PGBinaryObject;
import org.postgresql.util.PGobject;

/**
 * PostgreSQL-specific wrapper for encrypted_data type that extends PGobject
 * and implements PGBinaryObject for binary transfer mode.
 */
public class EncryptedData extends PGobject implements PGBinaryObject {

  private byte[] bytes;

  public EncryptedData() {
    setType("encrypted_data");
  }

  public EncryptedData(byte[] bytes) {
    this();
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public void setValue(String value) throws SQLException {
    if (value != null && value.startsWith("\\x")) {
      this.bytes = hexToBytes(value.substring(2));
    } else {
      this.bytes = null;
    }
  }

  @Override
  public String getValue() {
    if (bytes == null) {
      return null;
    }
    return "\\x" + bytesToHex(bytes);
  }

  @Override
  public int lengthInBytes() {
    return bytes != null ? bytes.length : 0;
  }

  @Override
  public void setByteValue(byte[] value, int offset) throws SQLException {
    if (value == null) {
      this.bytes = null;
    } else {
      this.bytes = new byte[value.length - offset];
      System.arraycopy(value, offset, this.bytes, 0, this.bytes.length);
    }
  }

  @Override
  public void toBytes(byte[] bytes, int offset) {
    if (this.bytes != null) {
      System.arraycopy(this.bytes, 0, bytes, offset, this.bytes.length);
    }
  }

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

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
