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
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.plugin.encryption.sql.SqlAnalysisService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

/**
 * Factory for creating database-specific EncryptingPreparedStatement implementations.
 */
public class EncryptingPreparedStatementFactory {

  /**
   * Creates appropriate EncryptingPreparedStatement based on target driver dialect.
   *
   * @param delegate the PreparedStatement to wrap
   * @param metadataManager the metadata manager
   * @param encryptionService the encryption service
   * @param keyManager the key manager
   * @param sqlAnalysisService the SQL analysis service
   * @param sql the SQL statement
   * @param targetDriverDialect the target driver dialect
   * @return database-specific EncryptingPreparedStatement implementation
   */
  public static PreparedStatement create(
      PreparedStatement delegate,
      MetadataManager metadataManager,
      software.amazon.jdbc.plugin.encryption.service.EncryptionService encryptionService,
      software.amazon.jdbc.plugin.encryption.key.KeyManager keyManager,
      SqlAnalysisService sqlAnalysisService,
      String sql,
      TargetDriverDialect targetDriverDialect) {
    
    if (isPostgreSqlDriver(targetDriverDialect)) {
      return new PostgresEncryptingPreparedStatement(
          delegate, metadataManager, encryptionService, keyManager, sqlAnalysisService, sql);
    } else {
      return new MysqlEncryptingPreparedStatement(
          delegate, metadataManager, encryptionService, keyManager, sqlAnalysisService, sql);
    }
  }

  private static boolean isPostgreSqlDriver(TargetDriverDialect targetDriverDialect) {
    if (targetDriverDialect == null) {
      return false;
    }
    return targetDriverDialect instanceof software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
  }
}
