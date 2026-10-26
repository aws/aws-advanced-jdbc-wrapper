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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of {@link KeyManagementUtility#rotateDataKeyAndReencrypt} and
 * {@link KeyManagementUtility#reencryptRemainingRows}. Holds counts and key ids only, never key
 * material or decrypted values.
 */
public final class DataKeyReencryptionResult {

  /** Maximum number of primary keys of unmatched rows kept in a result. */
  public static final int MAX_REPORTED_UNMATCHED_KEYS = 100;

  private final String tableName;
  private final String columnName;
  private final int currentKeyId;
  private final List<Integer> sourceKeyIds;
  private final long reencryptedCount;
  private final long alreadyCurrentCount;
  private final long unmatchedCount;
  private final int batchCount;
  private final List<Object> unmatchedPrimaryKeys;

  DataKeyReencryptionResult(
      String tableName,
      String columnName,
      int currentKeyId,
      List<Integer> sourceKeyIds,
      long reencryptedCount,
      long alreadyCurrentCount,
      long unmatchedCount,
      int batchCount,
      List<Object> unmatchedPrimaryKeys) {
    this.tableName = tableName;
    this.columnName = columnName;
    this.currentKeyId = currentKeyId;
    this.sourceKeyIds = Collections.unmodifiableList(new ArrayList<>(sourceKeyIds));
    this.reencryptedCount = reencryptedCount;
    this.alreadyCurrentCount = alreadyCurrentCount;
    this.unmatchedCount = unmatchedCount;
    this.batchCount = batchCount;
    this.unmatchedPrimaryKeys = Collections.unmodifiableList(new ArrayList<>(unmatchedPrimaryKeys));
  }

  /**
   * Returns the table name as passed to the utility.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the column name as passed to the utility.
   *
   * @return the column name
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Returns the key id the column uses now. After a rotation this is the new key id.
   *
   * @return the current key id
   */
  public int getCurrentKeyId() {
    return currentKeyId;
  }

  /**
   * Returns the previous key ids that rows were matched against. After a rotation this is the key
   * id the column used before the rotation. Keep these keys and their KMS keys.
   *
   * @return unmodifiable list of old key ids
   */
  public List<Integer> getSourceKeyIds() {
    return sourceKeyIds;
  }

  /**
   * Returns the number of rows rewritten under the current key.
   *
   * @return re-encrypted row count
   */
  public long getReencryptedCount() {
    return reencryptedCount;
  }

  /**
   * Returns the number of rows that were already encrypted under the current key and were skipped.
   *
   * @return already-current row count
   */
  public long getAlreadyCurrentCount() {
    return alreadyCurrentCount;
  }

  /**
   * Returns the number of rows whose value matches neither the current key nor any old key. These
   * rows were left unchanged.
   *
   * @return unmatched row count
   */
  public long getUnmatchedCount() {
    return unmatchedCount;
  }

  /**
   * Returns the number of batches that read at least one row.
   *
   * @return batch count
   */
  public int getBatchCount() {
    return batchCount;
  }

  /**
   * Returns the primary keys of the first {@link #MAX_REPORTED_UNMATCHED_KEYS} unmatched rows.
   *
   * @return unmodifiable list of primary key values
   */
  public List<Object> getUnmatchedPrimaryKeys() {
    return unmatchedPrimaryKeys;
  }

  /**
   * Returns whether every non-null value of the column is now encrypted under the current key.
   *
   * @return true if no unmatched rows were found
   */
  public boolean isComplete() {
    return unmatchedCount == 0;
  }

  @Override
  public String toString() {
    return "DataKeyReencryptionResult{"
        + "tableName=" + tableName
        + ", columnName=" + columnName
        + ", currentKeyId=" + currentKeyId
        + ", sourceKeyIds=" + sourceKeyIds
        + ", reencryptedCount=" + reencryptedCount
        + ", alreadyCurrentCount=" + alreadyCurrentCount
        + ", unmatchedCount=" + unmatchedCount
        + ", batchCount=" + batchCount
        + ", unmatchedPrimaryKeys=" + unmatchedPrimaryKeys
        + "}";
  }
}
