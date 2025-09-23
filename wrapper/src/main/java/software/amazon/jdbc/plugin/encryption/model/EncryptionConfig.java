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

/**
 * Configuration class for the encryption plugin containing KMS settings,
 * caching options, retry policies, and other operational parameters.
 */
public class EncryptionConfig {

    private final String kmsRegion;
    private final String defaultMasterKeyArn;
    private final int keyRotationDays;
    private final boolean cacheEnabled;
    private final int cacheExpirationMinutes;
    private final int maxRetries;
    private final Duration retryBackoffBase;
    private final boolean auditLoggingEnabled;
    private final int kmsConnectionPoolSize;
    private final Duration kmsConnectionTimeout;
    private final boolean dataKeyCacheEnabled;
    private final int dataKeyCacheMaxSize;
    private final Duration dataKeyCacheExpiration;
    private final Duration metadataRefreshInterval;
    private final boolean hotReloadEnabled;
    private final boolean metricsEnabled;
    private final Duration metricsReportingInterval;

    private EncryptionConfig(Builder builder) {
        this.kmsRegion = Objects.requireNonNull(builder.kmsRegion, "kmsRegion cannot be null");
        this.defaultMasterKeyArn = builder.defaultMasterKeyArn;
        this.keyRotationDays = builder.keyRotationDays;
        this.cacheEnabled = builder.cacheEnabled;
        this.cacheExpirationMinutes = builder.cacheExpirationMinutes;
        this.maxRetries = builder.maxRetries;
        this.retryBackoffBase = builder.retryBackoffBase;
        this.auditLoggingEnabled = builder.auditLoggingEnabled;
        this.kmsConnectionPoolSize = builder.kmsConnectionPoolSize;
        this.kmsConnectionTimeout = builder.kmsConnectionTimeout;
        this.dataKeyCacheEnabled = builder.dataKeyCacheEnabled;
        this.dataKeyCacheMaxSize = builder.dataKeyCacheMaxSize;
        this.dataKeyCacheExpiration = builder.dataKeyCacheExpiration;
        this.metadataRefreshInterval = builder.metadataRefreshInterval;
        this.hotReloadEnabled = builder.hotReloadEnabled;
        this.metricsEnabled = builder.metricsEnabled;
        this.metricsReportingInterval = builder.metricsReportingInterval;
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

    public int getKmsConnectionPoolSize() {
        return kmsConnectionPoolSize;
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

    public boolean isHotReloadEnabled() {
        return hotReloadEnabled;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public Duration getMetricsReportingInterval() {
        return metricsReportingInterval;
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

        if (kmsConnectionPoolSize <= 0) {
            throw new IllegalArgumentException("KMS connection pool size must be positive");
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
            throw new IllegalArgumentException("Metadata refresh interval cannot be negative");
        }

        if (metricsReportingInterval.isNegative()) {
            throw new IllegalArgumentException("Metrics reporting interval cannot be negative");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        EncryptionConfig that = (EncryptionConfig) o;
        return keyRotationDays == that.keyRotationDays &&
                cacheEnabled == that.cacheEnabled &&
                cacheExpirationMinutes == that.cacheExpirationMinutes &&
                maxRetries == that.maxRetries &&
                auditLoggingEnabled == that.auditLoggingEnabled &&
                kmsConnectionPoolSize == that.kmsConnectionPoolSize &&
                dataKeyCacheEnabled == that.dataKeyCacheEnabled &&
                dataKeyCacheMaxSize == that.dataKeyCacheMaxSize &&
                hotReloadEnabled == that.hotReloadEnabled &&
                metricsEnabled == that.metricsEnabled &&
                Objects.equals(kmsRegion, that.kmsRegion) &&
                Objects.equals(defaultMasterKeyArn, that.defaultMasterKeyArn) &&
                Objects.equals(retryBackoffBase, that.retryBackoffBase) &&
                Objects.equals(kmsConnectionTimeout, that.kmsConnectionTimeout) &&
                Objects.equals(dataKeyCacheExpiration, that.dataKeyCacheExpiration) &&
                Objects.equals(metadataRefreshInterval, that.metadataRefreshInterval) &&
                Objects.equals(metricsReportingInterval, that.metricsReportingInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kmsRegion, defaultMasterKeyArn, keyRotationDays, cacheEnabled,
                cacheExpirationMinutes, maxRetries, retryBackoffBase, auditLoggingEnabled,
                kmsConnectionPoolSize, kmsConnectionTimeout, dataKeyCacheEnabled, dataKeyCacheMaxSize,
                dataKeyCacheExpiration, metadataRefreshInterval, hotReloadEnabled, metricsEnabled,
                metricsReportingInterval);
    }

    @Override
    public String toString() {
        return "EncryptionConfig{" +
                "kmsRegion='" + kmsRegion + '\'' +
                ", defaultMasterKeyArn='" + defaultMasterKeyArn + '\'' +
                ", keyRotationDays=" + keyRotationDays +
                ", cacheEnabled=" + cacheEnabled +
                ", cacheExpirationMinutes=" + cacheExpirationMinutes +
                ", maxRetries=" + maxRetries +
                ", retryBackoffBase=" + retryBackoffBase +
                ", auditLoggingEnabled=" + auditLoggingEnabled +
                ", kmsConnectionPoolSize=" + kmsConnectionPoolSize +
                ", kmsConnectionTimeout=" + kmsConnectionTimeout +
                ", dataKeyCacheEnabled=" + dataKeyCacheEnabled +
                ", dataKeyCacheMaxSize=" + dataKeyCacheMaxSize +
                ", dataKeyCacheExpiration=" + dataKeyCacheExpiration +
                ", metadataRefreshInterval=" + metadataRefreshInterval +
                ", hotReloadEnabled=" + hotReloadEnabled +
                ", metricsEnabled=" + metricsEnabled +
                ", metricsReportingInterval=" + metricsReportingInterval +
                '}';
    }

    /**
     * Creates an EncryptionConfig from Properties.
     *
     * @param properties Properties containing configuration values
     * @return EncryptionConfig instance
     */
    public static EncryptionConfig fromProperties(Properties properties) {
        Builder builder = builder();

        String region = properties.getProperty("kms.region");
        if (region != null) {
            builder.kmsRegion(region);
        }

        String masterKeyArn = properties.getProperty("kms.defaultMasterKeyArn");
        if (masterKeyArn != null) {
            builder.defaultMasterKeyArn(masterKeyArn);
        }

        String rotationDays = properties.getProperty("key.rotationDays");
        if (rotationDays != null) {
            builder.keyRotationDays(Integer.parseInt(rotationDays));
        }

        String cacheEnabled = properties.getProperty("cache.enabled");
        if (cacheEnabled != null) {
            builder.cacheEnabled(Boolean.parseBoolean(cacheEnabled));
        }

        String cacheExpiration = properties.getProperty("cache.expirationMinutes");
        if (cacheExpiration != null) {
            builder.cacheExpirationMinutes(Integer.parseInt(cacheExpiration));
        }

        String maxRetries = properties.getProperty("retry.maxRetries");
        if (maxRetries != null) {
            builder.maxRetries(Integer.parseInt(maxRetries));
        }

        String backoffMs = properties.getProperty("retry.backoffBaseMs");
        if (backoffMs != null) {
            builder.retryBackoffBase(Duration.ofMillis(Long.parseLong(backoffMs)));
        }

        String auditLogging = properties.getProperty("audit.loggingEnabled");
        if (auditLogging != null) {
            builder.auditLoggingEnabled(Boolean.parseBoolean(auditLogging));
        }

        String poolSize = properties.getProperty("kms.connectionPoolSize");
        if (poolSize != null) {
            builder.kmsConnectionPoolSize(Integer.parseInt(poolSize));
        }

        String timeoutMs = properties.getProperty("kms.connectionTimeoutMs");
        if (timeoutMs != null) {
            builder.kmsConnectionTimeout(Duration.ofMillis(Long.parseLong(timeoutMs)));
        }

        String dataKeyCacheEnabled = properties.getProperty("dataKeyCache.enabled");
        if (dataKeyCacheEnabled != null) {
            builder.dataKeyCacheEnabled(Boolean.parseBoolean(dataKeyCacheEnabled));
        }

        String dataKeyCacheMaxSize = properties.getProperty("dataKeyCache.maxSize");
        if (dataKeyCacheMaxSize != null) {
            builder.dataKeyCacheMaxSize(Integer.parseInt(dataKeyCacheMaxSize));
        }

        String dataKeyCacheExpirationMs = properties.getProperty("dataKeyCache.expirationMs");
        if (dataKeyCacheExpirationMs != null) {
            builder.dataKeyCacheExpiration(Duration.ofMillis(Long.parseLong(dataKeyCacheExpirationMs)));
        }

        String metadataRefreshIntervalMs = properties.getProperty("metadata.refreshIntervalMs");
        if (metadataRefreshIntervalMs != null) {
            builder.metadataRefreshInterval(Duration.ofMillis(Long.parseLong(metadataRefreshIntervalMs)));
        }

        String hotReloadEnabled = properties.getProperty("config.hotReloadEnabled");
        if (hotReloadEnabled != null) {
            builder.hotReloadEnabled(Boolean.parseBoolean(hotReloadEnabled));
        }

        String metricsEnabled = properties.getProperty("metrics.enabled");
        if (metricsEnabled != null) {
            builder.metricsEnabled(Boolean.parseBoolean(metricsEnabled));
        }

        String metricsReportingIntervalMs = properties.getProperty("metrics.reportingIntervalMs");
        if (metricsReportingIntervalMs != null) {
            builder.metricsReportingInterval(Duration.ofMillis(Long.parseLong(metricsReportingIntervalMs)));
        }

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
        private int kmsConnectionPoolSize = 10;
        private Duration kmsConnectionTimeout = Duration.ofSeconds(30);
        private boolean dataKeyCacheEnabled = true;
        private int dataKeyCacheMaxSize = 1000;
        private Duration dataKeyCacheExpiration = Duration.ofMinutes(30);
        private Duration metadataRefreshInterval = Duration.ofMinutes(5);
        private boolean hotReloadEnabled = false;
        private boolean metricsEnabled = false;
        private Duration metricsReportingInterval = Duration.ofMinutes(1);

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

        public Builder kmsConnectionPoolSize(int kmsConnectionPoolSize) {
            this.kmsConnectionPoolSize = kmsConnectionPoolSize;
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

        public Builder hotReloadEnabled(boolean hotReloadEnabled) {
            this.hotReloadEnabled = hotReloadEnabled;
            return this;
        }

        public Builder metricsEnabled(boolean metricsEnabled) {
            this.metricsEnabled = metricsEnabled;
            return this;
        }

        public Builder metricsReportingInterval(Duration metricsReportingInterval) {
            this.metricsReportingInterval = metricsReportingInterval;
            return this;
        }

        public EncryptionConfig build() {
            return new EncryptionConfig(this);
        }
    }
}
