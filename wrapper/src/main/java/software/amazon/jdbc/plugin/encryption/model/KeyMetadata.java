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
 * Metadata class for storing KMS key information including master key ARN, encrypted data key, and
 * usage tracking information.
 */
public class KeyMetadata {

  private final String keyId;
  private final String keyName;
  private final String masterKeyArn;
  private final String encryptedDataKey;
  private final byte[] hmacKey;
  private final String keySpec;
  private final Instant createdAt;
  private final Instant lastUsedAt;

  private KeyMetadata(Builder builder) {
    this.keyId = Objects.requireNonNull(builder.keyId, "keyId cannot be null");
    this.keyName = Objects.requireNonNull(builder.keyName, "keyName cannot be null");
    this.masterKeyArn = Objects.requireNonNull(builder.masterKeyArn, "masterKeyArn cannot be null");
    this.encryptedDataKey =
        Objects.requireNonNull(builder.encryptedDataKey, "encryptedDataKey cannot be null");
    this.hmacKey = builder.hmacKey;
    this.keySpec = Objects.requireNonNull(builder.keySpec, "keySpec cannot be null");
    this.createdAt = builder.createdAt != null ? builder.createdAt : Instant.now();
    this.lastUsedAt = builder.lastUsedAt != null ? builder.lastUsedAt : Instant.now();
  }

  public String getKeyId() {
    return keyId;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getMasterKeyArn() {
    return masterKeyArn;
  }

  public String getEncryptedDataKey() {
    return encryptedDataKey;
  }

  public byte[] getHmacKey() {
    return hmacKey;
  }

  public String getKeySpec() {
    return keySpec;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getLastUsedAt() {
    return lastUsedAt;
  }

  /**
   * Creates a new KeyMetadata instance with updated lastUsedAt timestamp.
   *
   * @return New KeyMetadata with current timestamp
   */
  public KeyMetadata withUpdatedLastUsed() {
    return builder()
        .keyId(this.keyId)
        .masterKeyArn(this.masterKeyArn)
        .encryptedDataKey(this.encryptedDataKey)
        .keySpec(this.keySpec)
        .createdAt(this.createdAt)
        .lastUsedAt(Instant.now())
        .build();
  }

  /**
   * Checks if the key metadata is valid for encryption operations.
   *
   * @return True if metadata is valid, false otherwise
   */
  public boolean isValid() {
    return keyId != null
        && !keyId.trim().isEmpty()
        && masterKeyArn != null
        && !masterKeyArn.trim().isEmpty()
        && encryptedDataKey != null
        && !encryptedDataKey.trim().isEmpty()
        && keySpec != null
        && !keySpec.trim().isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyMetadata that = (KeyMetadata) o;
    return Objects.equals(keyId, that.keyId)
        && Objects.equals(masterKeyArn, that.masterKeyArn)
        && Objects.equals(encryptedDataKey, that.encryptedDataKey)
        && Objects.equals(keySpec, that.keySpec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyId, masterKeyArn, encryptedDataKey, keySpec);
  }

  @Override
  public String toString() {
    return "KeyMetadata{"
        + "keyId='"
        + keyId
        + '\''
        + ", masterKeyArn='"
        + masterKeyArn
        + '\''
        + ", keySpec='"
        + keySpec
        + '\''
        + ", createdAt="
        + createdAt
        + ", lastUsedAt="
        + lastUsedAt
        + ", encryptedDataKey='[REDACTED]'"
        + // Don't expose encrypted key in logs
        '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String keyId;
    private String keyName;
    private String masterKeyArn;
    private String encryptedDataKey;
    private byte[] hmacKey;
    private String keySpec = "AES_256"; // Default key spec
    private Instant createdAt;
    private Instant lastUsedAt;

    public Builder keyId(String keyId) {
      this.keyId = keyId;
      return this;
    }

    public Builder keyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder masterKeyArn(String masterKeyArn) {
      this.masterKeyArn = masterKeyArn;
      return this;
    }

    public Builder encryptedDataKey(String encryptedDataKey) {
      this.encryptedDataKey = encryptedDataKey;
      return this;
    }

    public Builder hmacKey(byte[] hmacKey) {
      this.hmacKey = hmacKey;
      return this;
    }

    public Builder keySpec(String keySpec) {
      this.keySpec = keySpec;
      return this;
    }

    public Builder createdAt(Instant createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder lastUsedAt(Instant lastUsedAt) {
      this.lastUsedAt = lastUsedAt;
      return this;
    }

    public KeyMetadata build() {
      return new KeyMetadata(this);
    }
  }
}
