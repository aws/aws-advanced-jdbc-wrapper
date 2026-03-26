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

package software.amazon.jdbc.plugin.encryption.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.util.Messages;

/**
 * Thread-safe cache for data keys with configurable expiration and size limits. Provides metrics
 * for cache performance monitoring.
 */
public class DataKeyCache {

  private static final Logger LOGGER = Logger.getLogger(DataKeyCache.class.getName());

  private final Map<String, CacheEntry> cache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService cleanupExecutor;
  private final EncryptionConfig config;

  // Metrics
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong evictionCount = new AtomicLong(0);

  public DataKeyCache(EncryptionConfig config) {
    this.config = config;
    this.cache = new HashMap<>();
    this.cleanupExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "DataKeyCache-Cleanup");
              t.setDaemon(true);
              return t;
            });

    // Schedule periodic cleanup of expired entries
    long cleanupIntervalMs = Math.max(config.getDataKeyCacheExpiration().toMillis() / 4, 30000);
    cleanupExecutor.scheduleAtFixedRate(
        this::cleanupExpiredEntries, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);

    LOGGER.info(
        () -> Messages.get("DataKeyCache.initialized",
            new Object[]{config.getDataKeyCacheMaxSize(),
                config.getDataKeyCacheExpiration(), cleanupIntervalMs}));
  }

  /**
   * Retrieves a data key from the cache.
   *
   * @param keyId the key identifier
   * @return decrypted data key bytes, or null if not found or expired
   */
  public byte[] get(String keyId) {
    if (!config.isDataKeyCacheEnabled() || keyId == null) {
      return null;
    }

    lock.readLock().lock();
    try {
      CacheEntry entry = cache.get(keyId);
      if (entry == null) {
        missCount.incrementAndGet();
        LOGGER.finest(() -> Messages.get("DataKeyCache.cacheMiss", new Object[]{keyId}));
        return null;
      }

      if (entry.isExpired(config.getDataKeyCacheExpiration())) {
        missCount.incrementAndGet();
        LOGGER.finest(() -> Messages.get("DataKeyCache.cacheExpired", new Object[]{keyId}));
        return null;
      }

      hitCount.incrementAndGet();
      LOGGER.finest(() -> Messages.get("DataKeyCache.cacheHit", new Object[]{keyId}));
      return entry.getDataKey();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Stores a data key in the cache.
   *
   * @param keyId the key identifier
   * @param dataKey the decrypted data key bytes
   */
  public void put(String keyId, byte[] dataKey) {
    if (!config.isDataKeyCacheEnabled() || keyId == null || dataKey == null) {
      return;
    }

    lock.writeLock().lock();
    try {
      // Check if we need to evict entries to make room
      if (cache.size() >= config.getDataKeyCacheMaxSize()) {
        evictOldestEntry();
      }

      CacheEntry entry = new CacheEntry(dataKey.clone());
      cache.put(keyId, entry);

      LOGGER.finest(() -> Messages.get("DataKeyCache.cached", new Object[]{keyId}));
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes a specific key from the cache.
   *
   * @param keyId the key identifier to remove
   */
  public void remove(String keyId) {
    if (keyId == null) {
      return;
    }

    lock.writeLock().lock();
    try {
      CacheEntry removed = cache.remove(keyId);
      if (removed != null) {
        removed.clear();
        LOGGER.finest(() -> Messages.get("DataKeyCache.removed", new Object[]{keyId}));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Clears all entries from the cache. */
  public void clear() {
    lock.writeLock().lock();
    try {
      // Clear sensitive data before removing entries
      cache.values().forEach(CacheEntry::clear);
      cache.clear();
      LOGGER.info(Messages.get("DataKeyCache.cleared"));
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns cache statistics.
   *
   * @return CacheStats object with current metrics
   */
  public CacheStats getStats() {
    lock.readLock().lock();
    try {
      return new CacheStats(
          cache.size(), hitCount.get(), missCount.get(), evictionCount.get(), calculateHitRate());
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Shuts down the cache and cleans up resources. */
  public void shutdown() {
    LOGGER.info(Messages.get("DataKeyCache.shuttingDown"));

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

  /** Removes expired entries from the cache. */
  private void cleanupExpiredEntries() {
    if (!config.isDataKeyCacheEnabled()) {
      return;
    }

    lock.writeLock().lock();
    try {
      Duration expiration = config.getDataKeyCacheExpiration();
      int removedCount = 0;

      Iterator<Map.Entry<String, CacheEntry>> iterator = cache.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, CacheEntry> entry = iterator.next();
        if (entry.getValue().isExpired(expiration)) {
          entry.getValue().clear();
          iterator.remove();
          removedCount++;
        }
      }

      if (removedCount > 0) {
        int finalRemovedCount = removedCount;
        LOGGER.finest(
            () -> Messages.get("DataKeyCache.cleanedUp", new Object[]{finalRemovedCount}));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Evicts the oldest entry from the cache to make room for new entries. Caller must hold write lock. */
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
      CacheEntry removed = cache.remove(oldestKey);
      if (removed != null) {
        removed.clear();
        evictionCount.incrementAndGet();
        String finalOldestKey = oldestKey;
        LOGGER.finest(() -> Messages.get("DataKeyCache.evicted", new Object[]{finalOldestKey}));
      }
    }
  }

  /** Calculates the current cache hit rate. */
  private double calculateHitRate() {
    long hits = hitCount.get();
    long misses = missCount.get();
    long total = hits + misses;

    return total > 0 ? (double) hits / total : 0.0;
  }

  /** Cache entry wrapper that tracks creation time and provides secure cleanup. */
  private static class CacheEntry {
    private final byte[] dataKey;
    private final Instant createdAt;
    private volatile boolean cleared = false;

    public CacheEntry(byte[] dataKey) {
      this.dataKey = dataKey;
      this.createdAt = Instant.now();
    }

    public byte[] getDataKey() {
      if (cleared) {
        return null;
      }
      return dataKey.clone(); // Return copy for security
    }

    public Instant getCreatedAt() {
      return createdAt;
    }

    public boolean isExpired(Duration expiration) {
      return Instant.now().isAfter(createdAt.plus(expiration));
    }

    public void clear() {
      if (!cleared && dataKey != null) {
        Arrays.fill(dataKey, (byte) 0);
        cleared = true;
      }
    }
  }

  /** Cache statistics data class. */
  public static class CacheStats {
    private final int size;
    private final long hitCount;
    private final long missCount;
    private final long evictionCount;
    private final double hitRate;

    public CacheStats(int size, long hitCount, long missCount, long evictionCount, double hitRate) {
      this.size = size;
      this.hitCount = hitCount;
      this.missCount = missCount;
      this.evictionCount = evictionCount;
      this.hitRate = hitRate;
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

    public double getHitRate() {
      return hitRate;
    }

    @Override
    public String toString() {
      return String.format(
          "CacheStats{size=%d, hits=%d, misses=%d, evictions=%d, hitRate=%.2f%%}",
          size, hitCount, missCount, evictionCount, hitRate * 100);
    }
  }
}
