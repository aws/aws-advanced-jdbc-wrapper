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

/**
 * Configuration class that represents encryption settings for a specific database column. Contains
 * table/column mapping information and associated encryption metadata.
 */
public class ColumnEncryptionConfig {

  private final String tableName;
  private final String columnName;
  private final String algorithm;
  private final String keyId;
  private final KeyMetadata keyMetadata;
  private final Instant createdAt;
  private final Instant updatedAt;

  private ColumnEncryptionConfig(Builder builder) {
    this.tableName = Objects.requireNonNull(builder.tableName, "tableName cannot be null");
    this.columnName = Objects.requireNonNull(builder.columnName, "columnName cannot be null");
    this.algorithm = Objects.requireNonNull(builder.algorithm, "algorithm cannot be null");
    this.keyId = Objects.requireNonNull(builder.keyId, "keyId cannot be null");
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

  public String getKeyId() {
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
  public boolean equals(Object o) {
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
    return "ColumnEncryptionConfig{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnName='"
        + columnName
        + '\''
        + ", algorithm='"
        + algorithm
        + '\''
        + ", keyId='"
        + keyId
        + '\''
        + ", createdAt="
        + createdAt
        + ", updatedAt="
        + updatedAt
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String tableName;
    private String columnName;
    private String algorithm = "AES-256-GCM"; // Default algorithm
    private String keyId;
    private KeyMetadata keyMetadata;
    private Instant createdAt;
    private Instant updatedAt;

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder columnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder algorithm(String algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    public Builder keyId(String keyId) {
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
