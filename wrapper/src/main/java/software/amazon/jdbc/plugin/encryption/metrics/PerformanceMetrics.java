package software.amazon.jdbc.metrics;

import software.amazon.jdbc.cache.DataKeyCache;
import software.amazon.jdbc.model.EncryptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Collects and reports performance metrics for the encryption plugin.
 * Tracks encryption/decryption operations, KMS calls, cache performance, and timing metrics.
 */
public class PerformanceMetrics {
    
    private static final Logger logger = LoggerFactory.getLogger(PerformanceMetrics.class);
    private static final Logger metricsLogger = LoggerFactory.getLogger("METRICS");
    
    private final EncryptionConfig config;
    private final ScheduledExecutorService reportingExecutor;
    
    // Operation counters
    private final LongAdder encryptionOperations = new LongAdder();
    private final LongAdder decryptionOperations = new LongAdder();
    private final LongAdder kmsGenerateDataKeyOperations = new LongAdder();
    private final LongAdder kmsDecryptOperations = new LongAdder();
    private final LongAdder metadataLookups = new LongAdder();
    
    // Error counters
    private final LongAdder encryptionErrors = new LongAdder();
    private final LongAdder decryptionErrors = new LongAdder();
    private final LongAdder kmsErrors = new LongAdder();
    private final LongAdder metadataErrors = new LongAdder();
    
    // Timing metrics (in nanoseconds)
    private final LongAdder totalEncryptionTime = new LongAdder();
    private final LongAdder totalDecryptionTime = new LongAdder();
    private final LongAdder totalKmsTime = new LongAdder();
    private final LongAdder totalMetadataTime = new LongAdder();
    
    // Peak timing tracking
    private final AtomicLong maxEncryptionTime = new AtomicLong(0);
    private final AtomicLong maxDecryptionTime = new AtomicLong(0);
    private final AtomicLong maxKmsTime = new AtomicLong(0);
    private final AtomicLong maxMetadataTime = new AtomicLong(0);
    
    // Cache reference for metrics
    private volatile DataKeyCache dataKeyCache;
    
    // Reporting state
    private volatile Instant lastReportTime = Instant.now();

    public PerformanceMetrics(EncryptionConfig config) {
        this.config = config;
        this.reportingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PerformanceMetrics-Reporter");
            t.setDaemon(true);
            return t;
        });
        
        if (config.isMetricsEnabled()) {
            startPeriodicReporting();
        }
        
        logger.info("PerformanceMetrics initialized, enabled={}, reportingInterval={}", 
                   config.isMetricsEnabled(), config.getMetricsReportingInterval());
    }

    /**
     * Sets the data key cache reference for cache metrics reporting.
     */
    public void setDataKeyCache(DataKeyCache dataKeyCache) {
        this.dataKeyCache = dataKeyCache;
    }

    /**
     * Records an encryption operation with timing.
     */
    public void recordEncryption(Duration duration) {
        if (!config.isMetricsEnabled()) return;
        
        encryptionOperations.increment();
        long nanos = duration.toNanos();
        totalEncryptionTime.add(nanos);
        updateMaxTime(maxEncryptionTime, nanos);
    }

    /**
     * Records a decryption operation with timing.
     */
    public void recordDecryption(Duration duration) {
        if (!config.isMetricsEnabled()) return;
        
        decryptionOperations.increment();
        long nanos = duration.toNanos();
        totalDecryptionTime.add(nanos);
        updateMaxTime(maxDecryptionTime, nanos);
    }

    /**
     * Records a KMS generate data key operation with timing.
     */
    public void recordKmsGenerateDataKey(Duration duration) {
        if (!config.isMetricsEnabled()) return;
        
        kmsGenerateDataKeyOperations.increment();
        long nanos = duration.toNanos();
        totalKmsTime.add(nanos);
        updateMaxTime(maxKmsTime, nanos);
    }

    /**
     * Records a KMS decrypt operation with timing.
     */
    public void recordKmsDecrypt(Duration duration) {
        if (!config.isMetricsEnabled()) return;
        
        kmsDecryptOperations.increment();
        long nanos = duration.toNanos();
        totalKmsTime.add(nanos);
        updateMaxTime(maxKmsTime, nanos);
    }

    /**
     * Records a metadata lookup operation with timing.
     */
    public void recordMetadataLookup(Duration duration) {
        if (!config.isMetricsEnabled()) return;
        
        metadataLookups.increment();
        long nanos = duration.toNanos();
        totalMetadataTime.add(nanos);
        updateMaxTime(maxMetadataTime, nanos);
    }

    /**
     * Records an encryption error.
     */
    public void recordEncryptionError() {
        if (!config.isMetricsEnabled()) return;
        encryptionErrors.increment();
    }

    /**
     * Records a decryption error.
     */
    public void recordDecryptionError() {
        if (!config.isMetricsEnabled()) return;
        decryptionErrors.increment();
    }

    /**
     * Records a KMS error.
     */
    public void recordKmsError() {
        if (!config.isMetricsEnabled()) return;
        kmsErrors.increment();
    }

    /**
     * Records a metadata error.
     */
    public void recordMetadataError() {
        if (!config.isMetricsEnabled()) return;
        metadataErrors.increment();
    }

    /**
     * Creates a timing context for measuring operation duration.
     */
    public TimingContext startTiming() {
        return new TimingContext();
    }

    /**
     * Returns current metrics snapshot.
     */
    public MetricsSnapshot getSnapshot() {
        return new MetricsSnapshot(
            // Operation counts
            encryptionOperations.sum(),
            decryptionOperations.sum(),
            kmsGenerateDataKeyOperations.sum(),
            kmsDecryptOperations.sum(),
            metadataLookups.sum(),
            
            // Error counts
            encryptionErrors.sum(),
            decryptionErrors.sum(),
            kmsErrors.sum(),
            metadataErrors.sum(),
            
            // Average times (in milliseconds)
            calculateAverageTime(totalEncryptionTime.sum(), encryptionOperations.sum()),
            calculateAverageTime(totalDecryptionTime.sum(), decryptionOperations.sum()),
            calculateAverageTime(totalKmsTime.sum(), kmsGenerateDataKeyOperations.sum() + kmsDecryptOperations.sum()),
            calculateAverageTime(totalMetadataTime.sum(), metadataLookups.sum()),
            
            // Max times (in milliseconds)
            maxEncryptionTime.get() / 1_000_000.0,
            maxDecryptionTime.get() / 1_000_000.0,
            maxKmsTime.get() / 1_000_000.0,
            maxMetadataTime.get() / 1_000_000.0,
            
            // Cache stats
            dataKeyCache != null ? dataKeyCache.getStats() : null,
            
            Instant.now()
        );
    }

    /**
     * Resets all metrics counters.
     */
    public void reset() {
        encryptionOperations.reset();
        decryptionOperations.reset();
        kmsGenerateDataKeyOperations.reset();
        kmsDecryptOperations.reset();
        metadataLookups.reset();
        
        encryptionErrors.reset();
        decryptionErrors.reset();
        kmsErrors.reset();
        metadataErrors.reset();
        
        totalEncryptionTime.reset();
        totalDecryptionTime.reset();
        totalKmsTime.reset();
        totalMetadataTime.reset();
        
        maxEncryptionTime.set(0);
        maxDecryptionTime.set(0);
        maxKmsTime.set(0);
        maxMetadataTime.set(0);
        
        lastReportTime = Instant.now();
        
        logger.info("Metrics reset");
    }

    /**
     * Shuts down the metrics system.
     */
    public void shutdown() {
        logger.info("Shutting down PerformanceMetrics");
        
        reportingExecutor.shutdown();
        try {
            if (!reportingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                reportingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            reportingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts periodic metrics reporting.
     */
    private void startPeriodicReporting() {
        long intervalMs = config.getMetricsReportingInterval().toMillis();
        reportingExecutor.scheduleAtFixedRate(this::reportMetrics, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        logger.info("Started periodic metrics reporting every {}ms", intervalMs);
    }

    /**
     * Reports current metrics to the log.
     */
    private void reportMetrics() {
        try {
            MetricsSnapshot snapshot = getSnapshot();
            Duration reportingPeriod = Duration.between(lastReportTime, snapshot.getTimestamp());
            
            metricsLogger.info("=== Encryption Plugin Metrics ({}s period) ===", reportingPeriod.getSeconds());
            metricsLogger.info("Operations: encrypt={}, decrypt={}, kms_generate={}, kms_decrypt={}, metadata_lookup={}", 
                             snapshot.getEncryptionOperations(),
                             snapshot.getDecryptionOperations(),
                             snapshot.getKmsGenerateDataKeyOperations(),
                             snapshot.getKmsDecryptOperations(),
                             snapshot.getMetadataLookups());
            
            metricsLogger.info("Errors: encrypt={}, decrypt={}, kms={}, metadata={}", 
                             snapshot.getEncryptionErrors(),
                             snapshot.getDecryptionErrors(),
                             snapshot.getKmsErrors(),
                             snapshot.getMetadataErrors());
            
            metricsLogger.info("Avg Times (ms): encrypt={:.2f}, decrypt={:.2f}, kms={:.2f}, metadata={:.2f}", 
                             snapshot.getAvgEncryptionTime(),
                             snapshot.getAvgDecryptionTime(),
                             snapshot.getAvgKmsTime(),
                             snapshot.getAvgMetadataTime());
            
            metricsLogger.info("Max Times (ms): encrypt={:.2f}, decrypt={:.2f}, kms={:.2f}, metadata={:.2f}", 
                             snapshot.getMaxEncryptionTime(),
                             snapshot.getMaxDecryptionTime(),
                             snapshot.getMaxKmsTime(),
                             snapshot.getMaxMetadataTime());
            
            if (snapshot.getCacheStats() != null) {
                metricsLogger.info("Cache: {}", snapshot.getCacheStats());
            }
            
            lastReportTime = snapshot.getTimestamp();
            
        } catch (Exception e) {
            logger.warn("Error reporting metrics", e);
        }
    }

    /**
     * Updates the maximum time if the new time is greater.
     */
    private void updateMaxTime(AtomicLong maxTime, long newTime) {
        long currentMax;
        do {
            currentMax = maxTime.get();
            if (newTime <= currentMax) {
                break;
            }
        } while (!maxTime.compareAndSet(currentMax, newTime));
    }

    /**
     * Calculates average time in milliseconds.
     */
    private double calculateAverageTime(long totalNanos, long count) {
        return count > 0 ? (totalNanos / (double) count) / 1_000_000.0 : 0.0;
    }

    /**
     * Context for measuring operation timing.
     */
    public static class TimingContext {
        private final Instant startTime;

        private TimingContext() {
            this.startTime = Instant.now();
        }

        public Duration elapsed() {
            return Duration.between(startTime, Instant.now());
        }
    }

    /**
     * Immutable snapshot of metrics at a point in time.
     */
    public static class MetricsSnapshot {
        private final long encryptionOperations;
        private final long decryptionOperations;
        private final long kmsGenerateDataKeyOperations;
        private final long kmsDecryptOperations;
        private final long metadataLookups;
        
        private final long encryptionErrors;
        private final long decryptionErrors;
        private final long kmsErrors;
        private final long metadataErrors;
        
        private final double avgEncryptionTime;
        private final double avgDecryptionTime;
        private final double avgKmsTime;
        private final double avgMetadataTime;
        
        private final double maxEncryptionTime;
        private final double maxDecryptionTime;
        private final double maxKmsTime;
        private final double maxMetadataTime;
        
        private final DataKeyCache.CacheStats cacheStats;
        private final Instant timestamp;

        public MetricsSnapshot(long encryptionOperations, long decryptionOperations, 
                             long kmsGenerateDataKeyOperations, long kmsDecryptOperations, long metadataLookups,
                             long encryptionErrors, long decryptionErrors, long kmsErrors, long metadataErrors,
                             double avgEncryptionTime, double avgDecryptionTime, double avgKmsTime, double avgMetadataTime,
                             double maxEncryptionTime, double maxDecryptionTime, double maxKmsTime, double maxMetadataTime,
                             DataKeyCache.CacheStats cacheStats, Instant timestamp) {
            this.encryptionOperations = encryptionOperations;
            this.decryptionOperations = decryptionOperations;
            this.kmsGenerateDataKeyOperations = kmsGenerateDataKeyOperations;
            this.kmsDecryptOperations = kmsDecryptOperations;
            this.metadataLookups = metadataLookups;
            this.encryptionErrors = encryptionErrors;
            this.decryptionErrors = decryptionErrors;
            this.kmsErrors = kmsErrors;
            this.metadataErrors = metadataErrors;
            this.avgEncryptionTime = avgEncryptionTime;
            this.avgDecryptionTime = avgDecryptionTime;
            this.avgKmsTime = avgKmsTime;
            this.avgMetadataTime = avgMetadataTime;
            this.maxEncryptionTime = maxEncryptionTime;
            this.maxDecryptionTime = maxDecryptionTime;
            this.maxKmsTime = maxKmsTime;
            this.maxMetadataTime = maxMetadataTime;
            this.cacheStats = cacheStats;
            this.timestamp = timestamp;
        }

        // Getters
        public long getEncryptionOperations() { return encryptionOperations; }
        public long getDecryptionOperations() { return decryptionOperations; }
        public long getKmsGenerateDataKeyOperations() { return kmsGenerateDataKeyOperations; }
        public long getKmsDecryptOperations() { return kmsDecryptOperations; }
        public long getMetadataLookups() { return metadataLookups; }
        public long getEncryptionErrors() { return encryptionErrors; }
        public long getDecryptionErrors() { return decryptionErrors; }
        public long getKmsErrors() { return kmsErrors; }
        public long getMetadataErrors() { return metadataErrors; }
        public double getAvgEncryptionTime() { return avgEncryptionTime; }
        public double getAvgDecryptionTime() { return avgDecryptionTime; }
        public double getAvgKmsTime() { return avgKmsTime; }
        public double getAvgMetadataTime() { return avgMetadataTime; }
        public double getMaxEncryptionTime() { return maxEncryptionTime; }
        public double getMaxDecryptionTime() { return maxDecryptionTime; }
        public double getMaxKmsTime() { return maxKmsTime; }
        public double getMaxMetadataTime() { return maxMetadataTime; }
        public DataKeyCache.CacheStats getCacheStats() { return cacheStats; }
        public Instant getTimestamp() { return timestamp; }
    }
}