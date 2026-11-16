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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;

/**
 * Configuration class for the encryption plugin containing KMS settings, caching options, retry
 * policies, and other operational parameters.
 */
public class EncryptionConfig {

  // Property definitions using AwsWrapperProperty
  public static final AwsWrapperProperty KMS_REGION =
      new AwsWrapperProperty("kms.region", null, "AWS KMS region for encryption operations");

  public static final AwsWrapperProperty ALLOWED_MASTER_KEY_ARNS =
      new AwsWrapperProperty(
          "encryptionAllowedMasterKeyArns",
          null,
          "Comma-separated list of the KMS master keys the KMS encryption plugin may use. Required by "
              + "the plugin. Each entry must match a key_storage.master_key_arn value exactly; a stored "
              + "data key that names any other master key (or none) is refused before KMS is called.");

  public static final AwsWrapperProperty METADATA_CACHE_ENABLED =
      new AwsWrapperProperty("metadataCacheEnabled", "true", "Enable/disable metadata caching");

  public static final AwsWrapperProperty METADATA_CACHE_EXPIRATION_MINUTES =
      new AwsWrapperProperty(
          "metadataCacheExpirationMinutes", "60", "Metadata cache expiration time in minutes");

  public static final AwsWrapperProperty KEY_MANAGEMENT_MAX_RETRIES =
      new AwsWrapperProperty(
          "keyManagementMaxRetries",
          "3",
          "Maximum number of retries for key management operations");

  public static final AwsWrapperProperty KEY_MANAGEMENT_RETRY_BACKOFF_BASE_MS =
      new AwsWrapperProperty(
          "keyManagementRetryBackoffBaseMs",
          "100",
          "Base backoff time in milliseconds for key management retries");

  public static final AwsWrapperProperty AUDIT_LOGGING_ENABLED =
      new AwsWrapperProperty("auditLoggingEnabled", "false", "Enable/disable audit logging");

  public static final AwsWrapperProperty DATA_KEY_CACHE_ENABLED =
      new AwsWrapperProperty("dataKeyCacheEnabled", "true", "Enable/disable data key caching");

  public static final AwsWrapperProperty DATA_KEY_CACHE_MAX_SIZE =
      new AwsWrapperProperty("dataKeyCacheMaxSize", "1000", "Maximum size of data key cache");

  public static final AwsWrapperProperty DATA_KEY_CACHE_EXPIRATION_MS =
      new AwsWrapperProperty(
          "dataKeyCacheExpirationMs", "3600000", "Data key cache expiration in milliseconds");

  public static final AwsWrapperProperty METADATA_CACHE_REFRESH_INTERVAL_MS =
      new AwsWrapperProperty(
          "metadataCacheRefreshIntervalMs",
          "300000",
          "Metadata cache refresh interval in milliseconds");

  public static final AwsWrapperProperty ENCRYPTION_METADATA_SCHEMA =
      new AwsWrapperProperty(
          "encryptionMetadataSchema", "encrypt", "Schema name for encryption metadata tables");

  public static final AwsWrapperProperty FAIL_ON_UNMAPPED_PARAMETER =
      new AwsWrapperProperty(
          "encryptionFailOnUnmappedParameter",
          "true",
          "Reject statements on tables with encrypted columns whose parameters cannot be mapped "
              + "to columns.");

  static {
    PropertyDefinition.registerPluginProperties(EncryptionConfig.class);
  }

  private final String kmsRegion;
  private final boolean cacheEnabled;
  private final int cacheExpirationMinutes;
  private final int maxRetries;
  private final Duration retryBackoffBase;
  private final boolean auditLoggingEnabled;
  private final boolean dataKeyCacheEnabled;
  private final int dataKeyCacheMaxSize;
  private final Duration dataKeyCacheExpiration;
  private final Duration metadataRefreshInterval;
  private final SchemaName encryptionMetadataSchema;
  private final boolean failOnUnmappedParameter;
  private final Set<String> allowedMasterKeyArns;

  private EncryptionConfig(Builder builder) {
    this.kmsRegion = Objects.requireNonNull(builder.kmsRegion, "kmsRegion cannot be null");
    this.cacheEnabled = builder.cacheEnabled;
    this.cacheExpirationMinutes = builder.cacheExpirationMinutes;
    this.maxRetries = builder.maxRetries;
    this.retryBackoffBase = builder.retryBackoffBase;
    this.auditLoggingEnabled = builder.auditLoggingEnabled;
    this.dataKeyCacheEnabled = builder.dataKeyCacheEnabled;
    this.dataKeyCacheMaxSize = builder.dataKeyCacheMaxSize;
    this.dataKeyCacheExpiration = builder.dataKeyCacheExpiration;
    this.metadataRefreshInterval = builder.metadataRefreshInterval;
    this.encryptionMetadataSchema =
        Objects.requireNonNull(
            builder.encryptionMetadataSchema, "encryptionMetadataSchema cannot be null");
    this.failOnUnmappedParameter = builder.failOnUnmappedParameter;
    this.allowedMasterKeyArns = Collections.unmodifiableSet(new LinkedHashSet<>(builder.allowedMasterKeyArns));
  }

  public String getKmsRegion() {
    return kmsRegion;
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

  public SchemaName getEncryptionMetadataSchema() {
    return encryptionMetadataSchema;
  }

  /**
   * Whether statements on tables with encrypted columns are rejected when their parameters cannot
   * be reliably mapped to columns.
   *
   * @return true to reject such statements (default), false to only log a warning
   */
  public boolean isFailOnUnmappedParameter() {
    return failOnUnmappedParameter;
  }

  /**
   * The master keys data keys may be generated or decrypted under, in the form (ARN, alias, or key
   * id) used when the columns were set up.
   *
   * @return an unmodifiable set, empty when no restriction is configured
   */
  public Set<String> getAllowedMasterKeyArns() {
    return allowedMasterKeyArns;
  }

  /**
   * Whether only the listed master keys may be used. An empty allow-list places no restriction,
   * which is acceptable for the key management utility but refused by the plugin itself.
   *
   * @return true when an allow-list is configured
   */
  public boolean restrictsMasterKeys() {
    return !allowedMasterKeyArns.isEmpty();
  }

  /**
   * Whether a master key, as recorded in key_storage, may be used.
   *
   * @param masterKeyArn the master key ARN, alias, or key id
   * @return true when no restriction is configured, or the key is on the allow-list
   */
  public boolean isMasterKeyAllowed(final @Nullable String masterKeyArn) {
    return !restrictsMasterKeys() || (masterKeyArn != null && allowedMasterKeyArns.contains(masterKeyArn));
  }

  /**
   * Validates the configuration settings.
   *
   * @throws IllegalArgumentException if configuration is invalid
   */
  public void validate() {
    if (kmsRegion == null || kmsRegion.trim().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.kmsRegionEmpty"));
    }

    if (cacheExpirationMinutes < 0) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.cacheExpirationNegative"));
    }

    if (maxRetries < 0) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.maxRetriesNegative"));
    }

    if (retryBackoffBase.isNegative()) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.retryBackoffNegative"));
    }

    if (dataKeyCacheMaxSize <= 0) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.cacheMaxSizePositive"));
    }

    if (dataKeyCacheExpiration.isNegative()) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.cacheExpirationNeg"));
    }

    if (metadataRefreshInterval.isNegative()) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.refreshIntervalNegative"));
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncryptionConfig that = (EncryptionConfig) o;
    return cacheEnabled == that.cacheEnabled
        && cacheExpirationMinutes == that.cacheExpirationMinutes
        && maxRetries == that.maxRetries
        && auditLoggingEnabled == that.auditLoggingEnabled
        && dataKeyCacheEnabled == that.dataKeyCacheEnabled
        && dataKeyCacheMaxSize == that.dataKeyCacheMaxSize
        && failOnUnmappedParameter == that.failOnUnmappedParameter
        && Objects.equals(kmsRegion, that.kmsRegion)
        && Objects.equals(retryBackoffBase, that.retryBackoffBase)
        && Objects.equals(dataKeyCacheExpiration, that.dataKeyCacheExpiration)
        && Objects.equals(metadataRefreshInterval, that.metadataRefreshInterval)
        && Objects.equals(allowedMasterKeyArns, that.allowedMasterKeyArns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kmsRegion,
        cacheEnabled,
        cacheExpirationMinutes,
        maxRetries,
        retryBackoffBase,
        auditLoggingEnabled,
        dataKeyCacheEnabled,
        dataKeyCacheMaxSize,
        dataKeyCacheExpiration,
        metadataRefreshInterval,
        failOnUnmappedParameter,
        allowedMasterKeyArns);
  }

  @Override
  public String toString() {
    return "EncryptionConfig{"
        + "kmsRegion='"
        + kmsRegion
        + '\''
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
        + ", dataKeyCacheEnabled="
        + dataKeyCacheEnabled
        + ", dataKeyCacheMaxSize="
        + dataKeyCacheMaxSize
        + ", dataKeyCacheExpiration="
        + dataKeyCacheExpiration
        + ", metadataRefreshInterval="
        + metadataRefreshInterval
        + ", failOnUnmappedParameter="
        + failOnUnmappedParameter
        // Only the count: the keys are not printed, so a logged config does not reveal them.
        + ", allowedMasterKeyArns=<"
        + allowedMasterKeyArns.size()
        + " keys>"
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

    builder.cacheEnabled(METADATA_CACHE_ENABLED.getBoolean(properties));
    builder.cacheExpirationMinutes(METADATA_CACHE_EXPIRATION_MINUTES.getInteger(properties));
    builder.maxRetries(KEY_MANAGEMENT_MAX_RETRIES.getInteger(properties));
    builder.retryBackoffBase(
        Duration.ofMillis(KEY_MANAGEMENT_RETRY_BACKOFF_BASE_MS.getLong(properties)));
    builder.auditLoggingEnabled(AUDIT_LOGGING_ENABLED.getBoolean(properties));
    builder.dataKeyCacheEnabled(DATA_KEY_CACHE_ENABLED.getBoolean(properties));
    builder.dataKeyCacheMaxSize(DATA_KEY_CACHE_MAX_SIZE.getInteger(properties));
    builder.dataKeyCacheExpiration(
        Duration.ofMillis(DATA_KEY_CACHE_EXPIRATION_MS.getLong(properties)));
    builder.metadataRefreshInterval(
        Duration.ofMillis(METADATA_CACHE_REFRESH_INTERVAL_MS.getLong(properties)));
    builder.encryptionMetadataSchema(ENCRYPTION_METADATA_SCHEMA.getString(properties));
    builder.failOnUnmappedParameter(FAIL_ON_UNMAPPED_PARAMETER.getBoolean(properties));
    builder.allowedMasterKeyArns(ALLOWED_MASTER_KEY_ARNS.getString(properties));

    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String kmsRegion;
    private boolean cacheEnabled = true;
    private int cacheExpirationMinutes = 60; // Default 1 hour
    private int maxRetries = 5;
    private Duration retryBackoffBase = Duration.ofMillis(100);
    private boolean auditLoggingEnabled = false;
    private boolean dataKeyCacheEnabled = true;
    private int dataKeyCacheMaxSize = 1000;
    private Duration dataKeyCacheExpiration = Duration.ofMinutes(30);
    private Duration metadataRefreshInterval = Duration.ofMinutes(5);
    private SchemaName encryptionMetadataSchema = SchemaName.of("encrypt"); // Default schema name
    private boolean failOnUnmappedParameter = true;
    private List<String> allowedMasterKeyArns = new ArrayList<>();

    public Builder kmsRegion(String kmsRegion) {
      this.kmsRegion = kmsRegion;
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

    public Builder encryptionMetadataSchema(@Nullable String encryptionMetadataSchema) {
      this.encryptionMetadataSchema = SchemaName.of(encryptionMetadataSchema);
      return this;
    }

    public Builder failOnUnmappedParameter(boolean failOnUnmappedParameter) {
      this.failOnUnmappedParameter = failOnUnmappedParameter;
      return this;
    }

    /**
     * Sets the permitted master keys from a comma-separated list. Entries are trimmed, and blank
     * entries are dropped.
     *
     * @param allowedMasterKeyArns the comma-separated master keys, or null for none
     * @return this builder
     */
    public Builder allowedMasterKeyArns(final @Nullable String allowedMasterKeyArns) {
      return allowedMasterKeyArns(
          allowedMasterKeyArns == null
              ? Collections.emptyList()
              : Arrays.asList(allowedMasterKeyArns.split(",")));
    }

    /**
     * Sets the permitted master keys. Entries are trimmed, and null or blank entries are dropped.
     *
     * @param allowedMasterKeyArns the master keys, or null for none
     * @return this builder
     */
    public Builder allowedMasterKeyArns(final @Nullable Collection<@Nullable String> allowedMasterKeyArns) {
      final List<String> normalized = new ArrayList<>();
      if (allowedMasterKeyArns != null) {
        for (final @Nullable String arn : allowedMasterKeyArns) {
          if (arn != null && !arn.trim().isEmpty()) {
            normalized.add(arn.trim());
          }
        }
      }
      this.allowedMasterKeyArns = normalized;
      return this;
    }

    public EncryptionConfig build() {
      return new EncryptionConfig(this);
    }
  }
}
