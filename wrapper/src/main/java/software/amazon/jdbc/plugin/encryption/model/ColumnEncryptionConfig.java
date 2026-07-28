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

package software.amazon.jdbc.plugin.encryption.model;

import java.time.Instant;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.encryption.service.EncryptionAlgorithm;

/**
 * Configuration class that represents encryption settings for a specific database column. Contains
 * table/column mapping information and associated encryption metadata.
 */
public class ColumnEncryptionConfig {

  private final String tableName;
  private final String columnName;
  private final String algorithm;
  private final Integer keyId;
  private final KeyMetadata keyMetadata;
  private final Instant createdAt;
  private final Instant updatedAt;

  private ColumnEncryptionConfig(Builder builder) {
    // Explicit null checks (instead of Objects.requireNonNull) so the non-null builder values can
    // be assigned to the non-null fields. Same exception type and message as before.
    final @Nullable String builderTableName = builder.tableName;
    if (builderTableName == null) {
      throw new NullPointerException("tableName cannot be null");
    }
    final @Nullable String builderColumnName = builder.columnName;
    if (builderColumnName == null) {
      throw new NullPointerException("columnName cannot be null");
    }
    final @Nullable String builderAlgorithm = builder.algorithm;
    if (builderAlgorithm == null) {
      throw new NullPointerException("algorithm cannot be null");
    }
    final @Nullable Integer builderKeyId = builder.keyId;
    if (builderKeyId == null) {
      throw new NullPointerException("keyId cannot be null");
    }
    this.tableName = builderTableName;
    this.columnName = builderColumnName;
    this.algorithm = builderAlgorithm;
    this.keyId = builderKeyId;
    this.keyMetadata = builder.keyMetadata;
    this.createdAt = builder.createdAt != null ? builder.createdAt : Instant.now();
    this.updatedAt = builder.updatedAt != null ? builder.updatedAt : Instant.now();
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public Integer getKeyId() {
    return keyId;
  }

  public KeyMetadata getKeyMetadata() {
    return keyMetadata;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  /**
   * Returns a unique identifier for this column configuration. Format: "tableName.columnName"
   *
   * @return Column identifier string
   */
  public String getColumnIdentifier() {
    return tableName + "." + columnName;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnEncryptionConfig that = (ColumnEncryptionConfig) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(columnName, that.columnName)
        && Objects.equals(algorithm, that.algorithm)
        && Objects.equals(keyId, that.keyId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnName, algorithm, keyId);
  }

  @Override
  public String toString() {
    return String.format(
        "ColumnEncryptionConfig{tableName='%s', columnName='%s', algorithm='%s',"
            + " keyId='%s', createdAt=%s, updatedAt=%s}",
        tableName, columnName, algorithm, keyId, createdAt, updatedAt);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private @Nullable String tableName;
    private @Nullable String columnName;
    private @Nullable String algorithm = EncryptionAlgorithm.AES_256_GCM;
    private @Nullable Integer keyId;
    // Left non-null: ColumnEncryptionConfig.getKeyMetadata() is dereferenced by callers.
    private KeyMetadata keyMetadata;
    private @Nullable Instant createdAt;
    private @Nullable Instant updatedAt;

    public Builder tableName(@Nullable String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder columnName(@Nullable String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder algorithm(@Nullable String algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    public Builder keyId(Integer keyId) {
      this.keyId = keyId;
      return this;
    }

    public Builder keyMetadata(KeyMetadata keyMetadata) {
      this.keyMetadata = keyMetadata;
      return this;
    }

    public Builder createdAt(Instant createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder updatedAt(Instant updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public ColumnEncryptionConfig build() {
      return new ColumnEncryptionConfig(this);
    }
  }
}
