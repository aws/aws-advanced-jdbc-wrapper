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
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.service.EncryptionService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

/**
 * Factory for creating database-specific DecryptingResultSet implementations.
 */
public class DecryptingResultSetFactory {

  /**
   * Creates appropriate DecryptingResultSet based on target driver dialect.
   */
  public static ResultSet create(
      ResultSet delegate,
      MetadataManager metadataManager,
      EncryptionService encryptionService,
      KeyManager keyManager,
      TargetDriverDialect targetDriverDialect) {
    
    if (isPostgreSqlDriver(targetDriverDialect)) {
      return new PostgresDecryptingResultSet(delegate, metadataManager, encryptionService, keyManager);
    } else {
      return new MysqlDecryptingResultSet(delegate, metadataManager, encryptionService, keyManager);
    }
  }

  private static boolean isPostgreSqlDriver(TargetDriverDialect targetDriverDialect) {
    if (targetDriverDialect == null) {
      return false;
    }
    return targetDriverDialect instanceof software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
  }
}
