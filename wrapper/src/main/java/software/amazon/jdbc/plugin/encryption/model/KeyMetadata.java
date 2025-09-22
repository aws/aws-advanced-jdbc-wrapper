package software.amazon.jdbc.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata class for storing KMS key information including master key ARN,
 * encrypted data key, and usage tracking information.
 */
public class KeyMetadata {
    
    private final String keyId;
    private final String masterKeyArn;
    private final String encryptedDataKey;
    private final String keySpec;
    private final Instant createdAt;
    private final Instant lastUsedAt;

    private KeyMetadata(Builder builder) {
        this.keyId = Objects.requireNonNull(builder.keyId, "keyId cannot be null");
        this.masterKeyArn = Objects.requireNonNull(builder.masterKeyArn, "masterKeyArn cannot be null");
        this.encryptedDataKey = Objects.requireNonNull(builder.encryptedDataKey, "encryptedDataKey cannot be null");
        this.keySpec = Objects.requireNonNull(builder.keySpec, "keySpec cannot be null");
        this.createdAt = builder.createdAt != null ? builder.createdAt : Instant.now();
        this.lastUsedAt = builder.lastUsedAt != null ? builder.lastUsedAt : Instant.now();
    }

    public String getKeyId() {
        return keyId;
    }

    public String getMasterKeyArn() {
        return masterKeyArn;
    }

    public String getEncryptedDataKey() {
        return encryptedDataKey;
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
     */
    public boolean isValid() {
        return keyId != null && !keyId.trim().isEmpty() &&
               masterKeyArn != null && !masterKeyArn.trim().isEmpty() &&
               encryptedDataKey != null && !encryptedDataKey.trim().isEmpty() &&
               keySpec != null && !keySpec.trim().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyMetadata that = (KeyMetadata) o;
        return Objects.equals(keyId, that.keyId) &&
               Objects.equals(masterKeyArn, that.masterKeyArn) &&
               Objects.equals(encryptedDataKey, that.encryptedDataKey) &&
               Objects.equals(keySpec, that.keySpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyId, masterKeyArn, encryptedDataKey, keySpec);
    }

    @Override
    public String toString() {
        return "KeyMetadata{" +
               "keyId='" + keyId + '\'' +
               ", masterKeyArn='" + masterKeyArn + '\'' +
               ", keySpec='" + keySpec + '\'' +
               ", createdAt=" + createdAt +
               ", lastUsedAt=" + lastUsedAt +
               ", encryptedDataKey='[REDACTED]'" + // Don't expose encrypted key in logs
               '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String keyId;
        private String masterKeyArn;
        private String encryptedDataKey;
        private String keySpec = "AES_256"; // Default key spec
        private Instant createdAt;
        private Instant lastUsedAt;

        public Builder keyId(String keyId) {
            this.keyId = keyId;
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