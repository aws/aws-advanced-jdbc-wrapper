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

package software.amazon.jdbc.plugin.encryption.schema;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class EncryptedDataTypeInstaller {

  private static final Logger LOGGER = Logger.getLogger(EncryptedDataTypeInstaller.class.getName());
  private static final String SQL_RESOURCE_PATH = "/sql/encrypted_data_type.sql";

  public static void installEncryptedDataType(Connection connection, String metaDataSchema) throws SQLException {
    LOGGER.info("Installing encrypted_data custom type");

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
      LOGGER.fine("pgcrypto extension enabled");

      // Use DOMAIN-based implementation
      String sql = loadSqlScript();
      sql = sql.replaceFirst("SCHEMA_NAME", metaDataSchema);
      stmt.execute(sql);

      LOGGER.info("encrypted_data type installed successfully (DOMAIN approach)");
    }
  }

  public static boolean isEncryptedDataTypeInstalled(Connection connection) throws SQLException {
    String checkSql = "SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'encrypted_data')";
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(checkSql)) {
      return rs.next() && rs.getBoolean(1);
    }
  }

  private static String loadSqlScript() {
    try (InputStream is = EncryptedDataTypeInstaller.class.getResourceAsStream(SQL_RESOURCE_PATH)) {
      if (is == null) {
        throw new IllegalStateException("SQL script not found: " + SQL_RESOURCE_PATH);
      }

      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load SQL script: " + SQL_RESOURCE_PATH, e);
    }
  }
}
