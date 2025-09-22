package software.amazon.jdbc.cache;

import software.amazon.jdbc.model.ConnectionParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe cache for connection parameters with configurable expiration and size limits.
 * Provides metrics for cache performance monitoring and avoids re-extraction of connection parameters.
 */
public class ConnectionParameterCache {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionParameterCache.class);

    // Default configuration values
    private static final int DEFAULT_MAX_SIZE = 100;
    private static final Duration DEFAULT_EXPIRATION = Duration.ofMinutes(30);
    private static final Duration DEFAULT_CLEANUP_INTERVAL = Duration.ofMinutes(5);

    private final Map<String, CacheEntry> cache;
    private final ReadWriteLock cacheLock;
    private final ScheduledExecutorService cleanupExecutor;
    
    // Configuration
    private final int maxSize;
    private final Duration expiration;
    private final boolean enabled;

    // Metrics
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong invalidationCount = new AtomicLong(0);

    /**
     * Creates a new ConnectionParameterCache with default configuration.
     */
    public ConnectionParameterCache() {
        this(DEFAULT_MAX_SIZE, DEFAULT_EXPIRATION, true);
    }

    /**
     * Creates a new ConnectionParameterCache with custom configuration.
     * 
     * @param maxSize the maximum number of entries to cache
     * @param expiration the expiration time for cache entries
     * @param enabled whether caching is enabled
     */
    public ConnectionParameterCache(int maxSize, Duration expiration, boolean enabled) {
        this.maxSize = Math.max(1, maxSize);
        this.expiration = expiration != null ? expiration : DEFAULT_EXPIRATION;
        this.enabled = enabled;
        
        this.cache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ConnectionParameterCache-Cleanup");
            t.setDaemon(true);
            return t;
        });

        if (enabled) {
            // Schedule periodic cleanup of expired entries
            long cleanupIntervalMs = Math.max(this.expiration.toMillis() / 6, DEFAULT_CLEANUP_INTERVAL.toMillis());
            cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredEntries,
                    cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);

            logger.info("ConnectionParameterCache initialized with maxSize={}, expiration={}, cleanupInterval={}ms",
                    this.maxSize, this.expiration, cleanupIntervalMs);
        } else {
            logger.info("ConnectionParameterCache initialized but disabled");
        }
    }

    /**
     * Retrieves connection parameters from the cache using a connection-based key.
     * 
     * @param connection the database connection to use as a key
     * @return cached ConnectionParameters, or null if not found or expired
     */
    public ConnectionParameters get(Connection connection) {
        if (!enabled || connection == null) {
            return null;
        }

        String cacheKey = generateCacheKey(connection);
        return get(cacheKey);
    }

    /**
     * Retrieves connection parameters from the cache using a string key.
     * 
     * @param cacheKey the cache key
     * @return cached ConnectionParameters, or null if not found or expired
     */
    public ConnectionParameters get(String cacheKey) {
        if (!enabled || cacheKey == null) {
            return null;
        }

        cacheLock.readLock().lock();
        try {
            CacheEntry entry = cache.get(cacheKey);
            if (entry == null) {
                missCount.incrementAndGet();
                logger.trace("Cache miss for key: {}", sanitizeKey(cacheKey));
                return null;
            }

            if (entry.isExpired(expiration)) {
                missCount.incrementAndGet();
                logger.trace("Cache entry expired for key: {}", sanitizeKey(cacheKey));
                // Remove expired entry (will be cleaned up by background thread)
                return null;
            }

            hitCount.incrementAndGet();
            logger.trace("Cache hit for key: {}", sanitizeKey(cacheKey));
            return entry.getConnectionParameters();

        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Stores connection parameters in the cache using a connection-based key.
     * 
     * @param connection the database connection to use as a key
     * @param parameters the connection parameters to cache
     */
    public void put(Connection connection, ConnectionParameters parameters) {
        if (!enabled || connection == null || parameters == null) {
            return;
        }

        String cacheKey = generateCacheKey(connection);
        put(cacheKey, parameters);
    }

    /**
     * Stores connection parameters in the cache using a string key.
     * 
     * @param cacheKey the cache key
     * @param parameters the connection parameters to cache
     */
    public void put(String cacheKey, ConnectionParameters parameters) {
        if (!enabled || cacheKey == null || parameters == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            // Check if we need to evict entries to make room
            if (cache.size() >= maxSize) {
                evictOldestEntry();
            }

            CacheEntry entry = new CacheEntry(parameters);
            cache.put(cacheKey, entry);

            logger.trace("Cached connection parameters for key: {}", sanitizeKey(cacheKey));

        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Removes connection parameters from the cache using a connection-based key.
     * 
     * @param connection the database connection to use as a key
     */
    public void remove(Connection connection) {
        if (!enabled || connection == null) {
            return;
        }

        String cacheKey = generateCacheKey(connection);
        remove(cacheKey);
    }

    /**
     * Removes connection parameters from the cache using a string key.
     * 
     * @param cacheKey the cache key to remove
     */
    public void remove(String cacheKey) {
        if (!enabled || cacheKey == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            CacheEntry removed = cache.remove(cacheKey);
            if (removed != null) {
                invalidationCount.incrementAndGet();
                logger.trace("Removed connection parameters from cache for key: {}", sanitizeKey(cacheKey));
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Invalidates cache entries that match the given connection properties.
     * This is useful when connection properties change and cached parameters become stale.
     * 
     * @param jdbcUrl the JDBC URL to match for invalidation
     */
    public void invalidateByJdbcUrl(String jdbcUrl) {
        if (!enabled || jdbcUrl == null) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            int removedCount = 0;
            Iterator<Map.Entry<String, CacheEntry>> iterator = cache.entrySet().iterator();
            
            while (iterator.hasNext()) {
                Map.Entry<String, CacheEntry> entry = iterator.next();
                ConnectionParameters params = entry.getValue().getConnectionParameters();
                
                if (params != null && jdbcUrl.equals(params.getJdbcUrl())) {
                    iterator.remove();
                    removedCount++;
                    invalidationCount.incrementAndGet();
                }
            }

            if (removedCount > 0) {
                logger.debug("Invalidated {} cache entries matching JDBC URL", removedCount);
            }

        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Clears all entries from the cache.
     */
    public void clear() {
        if (!enabled) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            int clearedCount = cache.size();
            cache.clear();
            invalidationCount.addAndGet(clearedCount);
            logger.info("Cache cleared, removed {} entries", clearedCount);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Returns cache statistics.
     * 
     * @return CacheStats object with current metrics
     */
    public CacheStats getStats() {
        if (!enabled) {
            return new CacheStats(0, 0, 0, 0, 0, 0.0, false);
        }

        cacheLock.readLock().lock();
        try {
            return new CacheStats(
                    cache.size(),
                    hitCount.get(),
                    missCount.get(),
                    evictionCount.get(),
                    invalidationCount.get(),
                    calculateHitRate(),
                    enabled);
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    /**
     * Checks if the cache is enabled.
     * 
     * @return true if caching is enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Gets the maximum cache size.
     * 
     * @return the maximum number of entries
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Gets the cache expiration duration.
     * 
     * @return the expiration duration
     */
    public Duration getExpiration() {
        return expiration;
    }

    /**
     * Shuts down the cache and cleans up resources.
     */
    public void shutdown() {
        logger.info("Shutting down ConnectionParameterCache");

        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        clear();
    }

    /**
     * Generates a cache key from a database connection.
     * The key is based on connection properties that uniquely identify the connection configuration.
     * 
     * @param connection the database connection
     * @return a cache key string
     */
    private String generateCacheKey(Connection connection) {
        try {
            // Use connection class and hash code as a basic key
            // This approach works for most connection types but may need refinement
            String className = connection.getClass().getName();
            int hashCode = System.identityHashCode(connection);
            
            // Try to get more specific information if available
            String url = null;
            String username = null;
            
            try {
                DatabaseMetaData metadata = connection.getMetaData();
                url = metadata.getURL();
                username = metadata.getUserName();
            } catch (Exception e) {
                // Ignore metadata extraction errors for key generation
                logger.trace("Could not extract metadata for cache key generation", e);
            }
            
            // Create a composite key that should be unique for each connection configuration
            StringBuilder keyBuilder = new StringBuilder();
            keyBuilder.append(className).append(":");
            
            if (url != null) {
                keyBuilder.append("url=").append(url.hashCode()).append(":");
            }
            if (username != null) {
                keyBuilder.append("user=").append(username.hashCode()).append(":");
            }
            
            keyBuilder.append("hash=").append(hashCode);
            
            return keyBuilder.toString();
            
        } catch (Exception e) {
            // Fallback to simple key if anything fails
            logger.trace("Failed to generate detailed cache key, using fallback", e);
            return connection.getClass().getName() + ":" + System.identityHashCode(connection);
        }
    }

    /**
     * Removes expired entries from the cache.
     */
    private void cleanupExpiredEntries() {
        if (!enabled) {
            return;
        }

        cacheLock.writeLock().lock();
        try {
            int removedCount = 0;

            Iterator<Map.Entry<String, CacheEntry>> iterator = cache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, CacheEntry> entry = iterator.next();
                if (entry.getValue().isExpired(expiration)) {
                    iterator.remove();
                    removedCount++;
                }
            }

            if (removedCount > 0) {
                logger.debug("Cleaned up {} expired cache entries", removedCount);
            }

        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Evicts the oldest entry from the cache to make room for new entries.
     */
    private void evictOldestEntry() {
        if (cache.isEmpty()) {
            return;
        }

        // Find the oldest entry
        String oldestKey = null;
        Instant oldestTime = Instant.MAX;

        for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
            if (entry.getValue().getCreatedAt().isBefore(oldestTime)) {
                oldestTime = entry.getValue().getCreatedAt();
                oldestKey = entry.getKey();
            }
        }

        if (oldestKey != null) {
            cache.remove(oldestKey);
            evictionCount.incrementAndGet();
            logger.trace("Evicted oldest cache entry: {}", sanitizeKey(oldestKey));
        }
    }

    /**
     * Calculates the current cache hit rate.
     */
    private double calculateHitRate() {
        long hits = hitCount.get();
        long misses = missCount.get();
        long total = hits + misses;

        return total > 0 ? (double) hits / total : 0.0;
    }

    /**
     * Sanitizes cache keys for logging by removing sensitive information.
     * 
     * @param key the cache key to sanitize
     * @return sanitized cache key
     */
    private String sanitizeKey(String key) {
        if (key == null) return "null";
        
        // Remove hash codes and other potentially sensitive information
        return key.replaceAll("hash=\\d+", "hash=***")
                 .replaceAll("url=\\d+", "url=***")
                 .replaceAll("user=\\d+", "user=***");
    }

    /**
     * Cache entry wrapper that tracks creation time.
     */
    private static class CacheEntry {
        private final ConnectionParameters connectionParameters;
        private final Instant createdAt;

        public CacheEntry(ConnectionParameters connectionParameters) {
            this.connectionParameters = connectionParameters;
            this.createdAt = Instant.now();
        }

        public ConnectionParameters getConnectionParameters() {
            return connectionParameters;
        }

        public Instant getCreatedAt() {
            return createdAt;
        }

        public boolean isExpired(Duration expiration) {
            return Instant.now().isAfter(createdAt.plus(expiration));
        }
    }

    /**
     * Cache statistics data class.
     */
    public static class CacheStats {
        private final int size;
        private final long hitCount;
        private final long missCount;
        private final long evictionCount;
        private final long invalidationCount;
        private final double hitRate;
        private final boolean enabled;

        public CacheStats(int size, long hitCount, long missCount, long evictionCount, 
                         long invalidationCount, double hitRate, boolean enabled) {
            this.size = size;
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.invalidationCount = invalidationCount;
            this.hitRate = hitRate;
            this.enabled = enabled;
        }

        public int getSize() {
            return size;
        }

        public long getHitCount() {
            return hitCount;
        }

        public long getMissCount() {
            return missCount;
        }

        public long getEvictionCount() {
            return evictionCount;
        }

        public long getInvalidationCount() {
            return invalidationCount;
        }

        public double getHitRate() {
            return hitRate;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public String toString() {
            return String.format("ConnectionParameterCacheStats{enabled=%s, size=%d, hits=%d, misses=%d, " +
                               "evictions=%d, invalidations=%d, hitRate=%.2f%%}",
                    enabled, size, hitCount, missCount, evictionCount, invalidationCount, hitRate * 100);
        }
    }
}