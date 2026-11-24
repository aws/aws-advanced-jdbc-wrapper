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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.PGResultSetMetaData;

/**
 * PostgreSQL-specific encrypted data handling using the EncryptedData custom type.
 * Only loaded when PostgreSQL is the target driver, keeping org.postgresql.* off
 * the classpath for MySQL-only deployments.
 */
public class PgEncryptedDataHelper {

  public void setEncryptedParameter(PreparedStatement ps, int paramIndex, byte[] encrypted)
      throws SQLException {
    EncryptedData encData = new EncryptedData(encrypted);
    ps.setObject(paramIndex, encData);
  }

  public byte @Nullable [] getEncryptedBytes(ResultSet rs, Object columnRef) throws SQLException {
    Object obj;
    if (columnRef instanceof Integer) {
      obj = rs.getObject((Integer) columnRef);
    } else {
      obj = rs.getObject((String) columnRef);
    }
    if (obj instanceof EncryptedData) {
      return ((EncryptedData) obj).getBytes();
    }
    if (columnRef instanceof Integer) {
      return rs.getBytes((Integer) columnRef);
    }
    return rs.getBytes((String) columnRef);
  }

  /**
   * Returns the table column that a result set column reads. pgjdbc's {@code getColumnName} returns
   * the column label, so an aliased column would otherwise be looked up by its alias. The base
   * column name is empty when the column is not a direct reference to a table column.
   */
  public @Nullable String getBaseColumnName(ResultSetMetaData metaData, int column)
      throws SQLException {
    if (metaData.isWrapperFor(PGResultSetMetaData.class)) {
      return metaData.unwrap(PGResultSetMetaData.class).getBaseColumnName(column);
    }
    return metaData.getColumnName(column);
  }
}
