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

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;

/**
 * Configuration class for the encryption plugin containing KMS settings, caching options, retry
 * policies, and other operational parameters.
 */
public class EncryptionConfig {

  // Property definitions using AwsWrapperProperty
  public static final AwsWrapperProperty KMS_REGION =
      new AwsWrapperProperty("kms.region", null, "AWS KMS region for encryption operations");

  public static final AwsWrapperProperty KMS_MASTER_KEY_ARN =
      new AwsWrapperProperty("kms.MasterKeyArn", null, "Master key ARN for encryption");

  public static final AwsWrapperProperty KEY_ROTATION_DAYS =
      new AwsWrapperProperty("key.rotationDays", "30", "Number of days for key rotation");

  public static final AwsWrapperProperty METADATA_CACHE_ENABLED =
      new AwsWrapperProperty("metadataCache.enabled", "true", "Enable/disable metadata caching");

  public static final AwsWrapperProperty METADATA_CACHE_EXPIRATION_MINUTES =
      new AwsWrapperProperty(
          "metadataCache.expirationMinutes", "60", "Metadata cache expiration time in minutes");

  public static final AwsWrapperProperty KEY_MANAGEMENT_MAX_RETRIES =
      new AwsWrapperProperty(
          "keyManagement.maxRetries",
          "3",
          "Maximum number of retries for key management operations");

  public static final AwsWrapperProperty KEY_MANAGEMENT_RETRY_BACKOFF_BASE_MS =
      new AwsWrapperProperty(
          "keyManagement.retryBackoffBaseMs",
          "100",
          "Base backoff time in milliseconds for key management retries");

  public static final AwsWrapperProperty AUDIT_LOGGING_ENABLED =
      new AwsWrapperProperty("audit.loggingEnabled", "false", "Enable/disable audit logging");

  public static final AwsWrapperProperty KMS_CONNECTION_TIMEOUT_MS =
      new AwsWrapperProperty(
          "kms.connectionTimeoutMs", "5000", "KMS connection timeout in milliseconds");

  public static final AwsWrapperProperty DATA_KEY_CACHE_ENABLED =
      new AwsWrapperProperty("dataKeyCache.enabled", "true", "Enable/disable data key caching");

  public static final AwsWrapperProperty DATA_KEY_CACHE_MAX_SIZE =
      new AwsWrapperProperty("dataKeyCache.maxSize", "1000", "Maximum size of data key cache");

  public static final AwsWrapperProperty DATA_KEY_CACHE_EXPIRATION_MS =
      new AwsWrapperProperty(
          "dataKeyCache.expirationMs", "3600000", "Data key cache expiration in milliseconds");

  public static final AwsWrapperProperty METADATA_CACHE_REFRESH_INTERVAL_MS =
      new AwsWrapperProperty(
          "metadataCache.refreshIntervalMs",
          "300000",
          "Metadata cache refresh interval in milliseconds");

  public static final AwsWrapperProperty ENCRYPTION_METADATA_SCHEMA =
      new AwsWrapperProperty(
          "encryption.metadataSchema", "aws", "Schema name for encryption metadata tables");

  static {
    PropertyDefinition.registerPluginProperties(EncryptionConfig.class);
  }

  private final String kmsRegion;
  private final String defaultMasterKeyArn;
  private final int keyRotationDays;
  private final boolean cacheEnabled;
  private final int cacheExpirationMinutes;
  private final int maxRetries;
  private final Duration retryBackoffBase;
  private final boolean auditLoggingEnabled;
  private final Duration kmsConnectionTimeout;
  private final boolean dataKeyCacheEnabled;
  private final int dataKeyCacheMaxSize;
  private final Duration dataKeyCacheExpiration;
  private final Duration metadataRefreshInterval;
  private final String encryptionMetadataSchema;

  private EncryptionConfig(Builder builder) {
    this.kmsRegion = Objects.requireNonNull(builder.kmsRegion, "kmsRegion cannot be null");
    this.defaultMasterKeyArn = builder.defaultMasterKeyArn;
    this.keyRotationDays = builder.keyRotationDays;
    this.cacheEnabled = builder.cacheEnabled;
    this.cacheExpirationMinutes = builder.cacheExpirationMinutes;
    this.maxRetries = builder.maxRetries;
    this.retryBackoffBase = builder.retryBackoffBase;
    this.auditLoggingEnabled = builder.auditLoggingEnabled;
    this.kmsConnectionTimeout = builder.kmsConnectionTimeout;
    this.dataKeyCacheEnabled = builder.dataKeyCacheEnabled;
    this.dataKeyCacheMaxSize = builder.dataKeyCacheMaxSize;
    this.dataKeyCacheExpiration = builder.dataKeyCacheExpiration;
    this.metadataRefreshInterval = builder.metadataRefreshInterval;
    this.encryptionMetadataSchema =
        Objects.requireNonNull(
            builder.encryptionMetadataSchema, "encryptionMetadataSchema cannot be null");
  }

  public String getKmsRegion() {
    return kmsRegion;
  }

  public String getDefaultMasterKeyArn() {
    return defaultMasterKeyArn;
  }

  public int getKeyRotationDays() {
    return keyRotationDays;
  }

  public boolean isCacheEnabled() {
    return cacheEnabled;
  }

  public int getCacheExpirationMinutes() {
    return cacheExpirationMinutes;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public Duration getRetryBackoffBase() {
    return retryBackoffBase;
  }

  public boolean isAuditLoggingEnabled() {
    return auditLoggingEnabled;
  }

  public Duration getKmsConnectionTimeout() {
    return kmsConnectionTimeout;
  }

  public boolean isDataKeyCacheEnabled() {
    return dataKeyCacheEnabled;
  }

  public int getDataKeyCacheMaxSize() {
    return dataKeyCacheMaxSize;
  }

  public Duration getDataKeyCacheExpiration() {
    return dataKeyCacheExpiration;
  }

  public Duration getMetadataRefreshInterval() {
    return metadataRefreshInterval;
  }

  public String getEncryptionMetadataSchema() {
    return encryptionMetadataSchema;
  }

  /**
   * Validates the configuration settings.
   *
   * @throws IllegalArgumentException if configuration is invalid
   */
  public void validate() {
    if (kmsRegion == null || kmsRegion.trim().isEmpty()) {
      throw new IllegalArgumentException("KMS region cannot be null or empty");
    }

    if (keyRotationDays < 0) {
      throw new IllegalArgumentException("Key rotation days cannot be negative");
    }

    if (cacheExpirationMinutes < 0) {
      throw new IllegalArgumentException("Cache expiration minutes cannot be negative");
    }

    if (maxRetries < 0) {
      throw new IllegalArgumentException("Max retries cannot be negative");
    }

    if (retryBackoffBase.isNegative()) {
      throw new IllegalArgumentException("Retry backoff base cannot be negative");
    }

    if (kmsConnectionTimeout.isNegative()) {
      throw new IllegalArgumentException("KMS connection timeout cannot be negative");
    }

    if (dataKeyCacheMaxSize <= 0) {
      throw new IllegalArgumentException("Data key cache max size must be positive");
    }

    if (dataKeyCacheExpiration.isNegative()) {
      throw new IllegalArgumentException("Data key cache expiration cannot be negative");
    }

    if (metadataRefreshInterval.isNegative()) {
      throw new IllegalArgumentException("Metrics reporting interval cannot be negative");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncryptionConfig that = (EncryptionConfig) o;
    return keyRotationDays == that.keyRotationDays
        && cacheEnabled == that.cacheEnabled
        && cacheExpirationMinutes == that.cacheExpirationMinutes
        && maxRetries == that.maxRetries
        && auditLoggingEnabled == that.auditLoggingEnabled
        && dataKeyCacheEnabled == that.dataKeyCacheEnabled
        && dataKeyCacheMaxSize == that.dataKeyCacheMaxSize
        && Objects.equals(kmsRegion, that.kmsRegion)
        && Objects.equals(defaultMasterKeyArn, that.defaultMasterKeyArn)
        && Objects.equals(retryBackoffBase, that.retryBackoffBase)
        && Objects.equals(kmsConnectionTimeout, that.kmsConnectionTimeout)
        && Objects.equals(dataKeyCacheExpiration, that.dataKeyCacheExpiration)
        && Objects.equals(metadataRefreshInterval, that.metadataRefreshInterval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kmsRegion,
        defaultMasterKeyArn,
        keyRotationDays,
        cacheEnabled,
        cacheExpirationMinutes,
        maxRetries,
        retryBackoffBase,
        auditLoggingEnabled,
        kmsConnectionTimeout,
        dataKeyCacheEnabled,
        dataKeyCacheMaxSize,
        dataKeyCacheExpiration,
        metadataRefreshInterval);
  }

  @Override
  public String toString() {
    return "EncryptionConfig{"
        + "kmsRegion='"
        + kmsRegion
        + '\''
        + ", defaultMasterKeyArn='"
        + defaultMasterKeyArn
        + '\''
        + ", keyRotationDays="
        + keyRotationDays
        + ", cacheEnabled="
        + cacheEnabled
        + ", cacheExpirationMinutes="
        + cacheExpirationMinutes
        + ", maxRetries="
        + maxRetries
        + ", retryBackoffBase="
        + retryBackoffBase
        + ", auditLoggingEnabled="
        + auditLoggingEnabled
        + ", kmsConnectionTimeout="
        + kmsConnectionTimeout
        + ", dataKeyCacheEnabled="
        + dataKeyCacheEnabled
        + ", dataKeyCacheMaxSize="
        + dataKeyCacheMaxSize
        + ", dataKeyCacheExpiration="
        + dataKeyCacheExpiration
        + ", metadataRefreshInterval="
        + metadataRefreshInterval
        + '}';
  }

  /**
   * Creates an EncryptionConfig from Properties.
   *
   * @param properties Properties containing configuration values
   * @return EncryptionConfig instance
   */
  public static EncryptionConfig fromProperties(Properties properties) {
    Builder builder = builder();

    String region = KMS_REGION.getString(properties);
    if (region != null) {
      builder.kmsRegion(region);
    }

    String masterKeyArn = KMS_MASTER_KEY_ARN.getString(properties);
    if (masterKeyArn != null) {
      builder.defaultMasterKeyArn(masterKeyArn);
    }

    builder.keyRotationDays(KEY_ROTATION_DAYS.getInteger(properties));
    builder.cacheEnabled(METADATA_CACHE_ENABLED.getBoolean(properties));
    builder.cacheExpirationMinutes(METADATA_CACHE_EXPIRATION_MINUTES.getInteger(properties));
    builder.maxRetries(KEY_MANAGEMENT_MAX_RETRIES.getInteger(properties));
    builder.retryBackoffBase(
        Duration.ofMillis(KEY_MANAGEMENT_RETRY_BACKOFF_BASE_MS.getLong(properties)));
    builder.auditLoggingEnabled(AUDIT_LOGGING_ENABLED.getBoolean(properties));
    builder.kmsConnectionTimeout(Duration.ofMillis(KMS_CONNECTION_TIMEOUT_MS.getLong(properties)));
    builder.dataKeyCacheEnabled(DATA_KEY_CACHE_ENABLED.getBoolean(properties));
    builder.dataKeyCacheMaxSize(DATA_KEY_CACHE_MAX_SIZE.getInteger(properties));
    builder.dataKeyCacheExpiration(
        Duration.ofMillis(DATA_KEY_CACHE_EXPIRATION_MS.getLong(properties)));
    builder.metadataRefreshInterval(
        Duration.ofMillis(METADATA_CACHE_REFRESH_INTERVAL_MS.getLong(properties)));
    builder.encryptionMetadataSchema(ENCRYPTION_METADATA_SCHEMA.getString(properties));

    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String kmsRegion;
    private String defaultMasterKeyArn;
    private int keyRotationDays = 90; // Default 90 days
    private boolean cacheEnabled = true;
    private int cacheExpirationMinutes = 60; // Default 1 hour
    private int maxRetries = 5;
    private Duration retryBackoffBase = Duration.ofMillis(100);
    private boolean auditLoggingEnabled = false;
    private Duration kmsConnectionTimeout = Duration.ofSeconds(30);
    private boolean dataKeyCacheEnabled = true;
    private int dataKeyCacheMaxSize = 1000;
    private Duration dataKeyCacheExpiration = Duration.ofMinutes(30);
    private Duration metadataRefreshInterval = Duration.ofMinutes(5);
    private String encryptionMetadataSchema = "encrypt"; // Default schema name

    public Builder kmsRegion(String kmsRegion) {
      this.kmsRegion = kmsRegion;
      return this;
    }

    public Builder defaultMasterKeyArn(String defaultMasterKeyArn) {
      this.defaultMasterKeyArn = defaultMasterKeyArn;
      return this;
    }

    public Builder keyRotationDays(int keyRotationDays) {
      this.keyRotationDays = keyRotationDays;
      return this;
    }

    public Builder cacheEnabled(boolean cacheEnabled) {
      this.cacheEnabled = cacheEnabled;
      return this;
    }

    public Builder cacheExpirationMinutes(int cacheExpirationMinutes) {
      this.cacheExpirationMinutes = cacheExpirationMinutes;
      return this;
    }

    public Builder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder retryBackoffBase(Duration retryBackoffBase) {
      this.retryBackoffBase = retryBackoffBase;
      return this;
    }

    public Builder auditLoggingEnabled(boolean auditLoggingEnabled) {
      this.auditLoggingEnabled = auditLoggingEnabled;
      return this;
    }

    public Builder kmsConnectionTimeout(Duration kmsConnectionTimeout) {
      this.kmsConnectionTimeout = kmsConnectionTimeout;
      return this;
    }

    public Builder dataKeyCacheEnabled(boolean dataKeyCacheEnabled) {
      this.dataKeyCacheEnabled = dataKeyCacheEnabled;
      return this;
    }

    public Builder dataKeyCacheMaxSize(int dataKeyCacheMaxSize) {
      this.dataKeyCacheMaxSize = dataKeyCacheMaxSize;
      return this;
    }

    public Builder dataKeyCacheExpiration(Duration dataKeyCacheExpiration) {
      this.dataKeyCacheExpiration = dataKeyCacheExpiration;
      return this;
    }

    public Builder metadataRefreshInterval(Duration metadataRefreshInterval) {
      this.metadataRefreshInterval = metadataRefreshInterval;
      return this;
    }

    public Builder encryptionMetadataSchema(String encryptionMetadataSchema) {
      this.encryptionMetadataSchema = encryptionMetadataSchema;
      return this;
    }

    public EncryptionConfig build() {
      return new EncryptionConfig(this);
    }
  }
}
