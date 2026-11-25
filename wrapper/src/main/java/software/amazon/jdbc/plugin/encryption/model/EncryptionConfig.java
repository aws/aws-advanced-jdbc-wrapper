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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;

/**
 * Configuration class for the encryption plugin containing KMS settings, caching options, retry
 * policies, and other operational parameters.
 */
public class EncryptionConfig {

  private static final Logger LOGGER = Logger.getLogger(EncryptionConfig.class.getName());

  // The region field of a KMS key or alias ARN, in any partition (aws, aws-cn, aws-us-gov, ...).
  private static final Pattern KMS_ARN_REGION_PATTERN = Pattern.compile("^arn:[^:]+:kms:([^:]*):");

  // The configuration is loaded for every connection, so each region mismatch is warned about once.
  // Keyed by the configured region and the regions of the key ARNs, never by the ARNs themselves.
  private static final Set<String> WARNED_REGION_MISMATCHES = ConcurrentHashMap.newKeySet();

  // Property definitions using AwsWrapperProperty
  public static final AwsWrapperProperty KMS_REGION =
      new AwsWrapperProperty(
          "kms.region",
          null,
          "AWS KMS region for encryption operations. If unset, the region is taken from the key ARNs "
              + "in encryptionAllowedMasterKeyArns.");

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
    builder.kmsRegion(resolveKmsRegion(KMS_REGION.getString(properties), builder.allowedMasterKeyArns));

    return builder.build();
  }

  /**
   * Returns the KMS region to use: {@code kms.region} when it is set, otherwise the region of the
   * key ARNs in the allow-list. Alias names ({@code alias/...}) and bare
   * key ids carry no region and are skipped.
   *
   * @throws IllegalArgumentException if {@code kms.region} is not set and the allow-list has no key
   *     ARN, or has key ARNs in more than one region
   */
  private static String resolveKmsRegion(
      final @Nullable String configuredRegion, final List<String> allowedMasterKeyArns) {
    final Set<String> arnRegions = new TreeSet<>();
    for (final String key : allowedMasterKeyArns) {
      final @Nullable String region = regionFromArn(key);
      if (region != null) {
        arnRegions.add(region);
      }
    }

    if (configuredRegion != null && !configuredRegion.trim().isEmpty()) {
      final String region = configuredRegion.trim();
      // A KMS client serves one region, so calls with a key from another region fail. The entry is
      // only reported, not refused: an unused leftover, or the replica of a multi-region key listed
      // for a deployment in another region, does not stop this one from working.
      if ((arnRegions.size() > 1 || (arnRegions.size() == 1 && !arnRegions.contains(region)))
          && WARNED_REGION_MISMATCHES.add(region + " " + arnRegions)) {
        LOGGER.warning(() -> Messages.get("EncryptionConfig.kmsRegionMismatch",
            new Object[]{KMS_REGION.name, region, ALLOWED_MASTER_KEY_ARNS.name}));
      }
      return region;
    }

    if (arnRegions.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.kmsRegionRequired",
          new Object[]{KMS_REGION.name, ALLOWED_MASTER_KEY_ARNS.name}));
    }
    if (arnRegions.size() > 1) {
      throw new IllegalArgumentException(Messages.get("EncryptionConfig.kmsRegionAmbiguous",
          new Object[]{KMS_REGION.name, ALLOWED_MASTER_KEY_ARNS.name}));
    }
    return arnRegions.iterator().next();
  }

  private static @Nullable String regionFromArn(final String key) {
    final Matcher matcher = KMS_ARN_REGION_PATTERN.matcher(key);
    if (!matcher.find()) {
      return null;
    }
    // The group always participates in a match, so it is never null here.
    final @Nullable String region = matcher.group(1);
    return region == null || region.isEmpty() ? null : region;
  }

  private static String defaultOf(final AwsWrapperProperty property) {
    final @Nullable String defaultValue = property.defaultValue;
    if (defaultValue == null) {
      throw new IllegalStateException(property.name + " has no default");
    }
    return defaultValue;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link EncryptionConfig}. {@code kmsRegion} is required; every other setting defaults
   * to the default of its connection property.
   */
  public static class Builder {
    private String kmsRegion;
    private boolean cacheEnabled = Boolean.parseBoolean(defaultOf(METADATA_CACHE_ENABLED));
    private int cacheExpirationMinutes = Integer.parseInt(defaultOf(METADATA_CACHE_EXPIRATION_MINUTES));
    private int maxRetries = Integer.parseInt(defaultOf(KEY_MANAGEMENT_MAX_RETRIES));
    private Duration retryBackoffBase =
        Duration.ofMillis(Long.parseLong(defaultOf(KEY_MANAGEMENT_RETRY_BACKOFF_BASE_MS)));
    private boolean auditLoggingEnabled = Boolean.parseBoolean(defaultOf(AUDIT_LOGGING_ENABLED));
    private boolean dataKeyCacheEnabled = Boolean.parseBoolean(defaultOf(DATA_KEY_CACHE_ENABLED));
    private int dataKeyCacheMaxSize = Integer.parseInt(defaultOf(DATA_KEY_CACHE_MAX_SIZE));
    private Duration dataKeyCacheExpiration =
        Duration.ofMillis(Long.parseLong(defaultOf(DATA_KEY_CACHE_EXPIRATION_MS)));
    private Duration metadataRefreshInterval =
        Duration.ofMillis(Long.parseLong(defaultOf(METADATA_CACHE_REFRESH_INTERVAL_MS)));
    private SchemaName encryptionMetadataSchema = SchemaName.of(defaultOf(ENCRYPTION_METADATA_SCHEMA));
    private boolean failOnUnmappedParameter = Boolean.parseBoolean(defaultOf(FAIL_ON_UNMAPPED_PARAMETER));
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
