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

package software.amazon.jdbc.plugin.cache;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class DataLocalCacheConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(DataLocalCacheConnectionPlugin.class.getName());

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName
      )));

  public static final AwsWrapperProperty DATA_CACHE_TRIGGER_CONDITION = new AwsWrapperProperty(
      "dataCacheTriggerCondition", null,
      "A regular expression that, if it's matched, allows the plugin to cache SQL results.");

  public static final AwsWrapperProperty DATA_CACHE_TTL_MS = new AwsWrapperProperty(
      "dataCacheTtlMs", "300000",
      "Time in milliseconds that a cached result set stays valid. A cached result is not refreshed "
          + "by writes, so this is the longest an application can observe stale data. Set to 0 to "
          + "keep entries until the cache is cleared, accepting unbounded staleness.");

  public static final AwsWrapperProperty DATA_CACHE_MAX_SIZE = new AwsWrapperProperty(
      "dataCacheMaxSize", "1000",
      "Maximum number of distinct SQL statements whose results are held in the cache. When the "
          + "limit is reached, expired entries are purged; if the cache is still full, further "
          + "results are returned to the caller without being cached.");

  /**
   * Process-wide cache of materialized query results, keyed on the SQL text. Each entry carries its
   * own expiry deadline, computed from the {@code dataCacheTtlMs} of the connection that stored it,
   * so a single static cache can be shared by connections configured differently.
   */
  protected static final Map<String, CacheEntry> dataCache = new ConcurrentHashMap<>();

  protected final @Nullable String dataCacheTriggerCondition;
  protected final long ttlNanos;
  protected final int maxSize;

  static {
    PropertyDefinition.registerPluginProperties(DataLocalCacheConnectionPlugin.class);
  }

  private final TelemetryFactory telemetryFactory;
  private final @Nullable TelemetryCounter hitCounter;
  private final @Nullable TelemetryCounter missCounter;
  private final @Nullable TelemetryCounter totalCallsCounter;
  private final @Nullable TelemetryGauge cacheSizeGauge;

  public DataLocalCacheConnectionPlugin(final PluginService pluginService, final Properties props) {
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.dataCacheTriggerCondition = DATA_CACHE_TRIGGER_CONDITION.getString(props);

    final long ttlMs = DATA_CACHE_TTL_MS.getLong(props);
    this.ttlNanos = ttlMs <= 0 ? CacheEntry.NO_EXPIRY : TimeUnit.MILLISECONDS.toNanos(ttlMs);
    final long configuredMaxSize = DATA_CACHE_MAX_SIZE.getLong(props);
    this.maxSize = configuredMaxSize <= 0
        ? Integer.MAX_VALUE
        : (int) Math.min(configuredMaxSize, Integer.MAX_VALUE);

    this.hitCounter = telemetryFactory.createCounter("dataCache.cache.hit");
    this.missCounter = telemetryFactory.createCounter("dataCache.cache.miss");
    this.totalCallsCounter = telemetryFactory.createCounter("dataCache.cache.totalCalls");
    this.cacheSizeGauge = telemetryFactory.createGauge("dataCache.cache.size", () -> (long) dataCache.size());
  }

  public static void clearCache() {
    dataCache.clear();
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  // "return": the final result is produced by the underlying JDBC call (possibly null) and
  // returned via Class.cast; the generic return type T cannot be annotated @Nullable.
  @SuppressWarnings("return")
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable Object[] jdbcMethodArgs)
      throws E {

    if (StringUtils.isNullOrEmpty(this.dataCacheTriggerCondition) || resultClass != ResultSet.class) {
      return jdbcMethodFunc.call();
    }

    if (this.totalCallsCounter != null) {
      this.totalCallsCounter.inc();
    }

    ResultSet result;
    boolean needToCache = false;
    final String sql = getQuery(jdbcMethodArgs);

    if (!StringUtils.isNullOrEmpty(sql) && sql.matches(this.dataCacheTriggerCondition)) {
      result = getIfFresh(sql);
      if (result == null) {
        needToCache = true;
        if (this.missCounter != null) {
          this.missCounter.inc();
        }
        LOGGER.finest(
            () -> Messages.get(
                "DataLocalCacheConnectionPlugin.queryResultsCached",
                new Object[]{methodName, sql}));
      } else {
        if (this.hitCounter != null) {
          this.hitCounter.inc();
        }
        try {
          result.beforeFirst();
        } catch (final SQLException ex) {
          throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
        }
        return resultClass.cast(result);
      }
    }

    result = (ResultSet) jdbcMethodFunc.call();

    // needToCache is only set to true above after sql was confirmed non-null and matched the
    // trigger condition, so the null guards below never skip caching in practice; they satisfy
    // the nullness checker for the possibly-null JDBC result and the sql cache key.
    if (needToCache) {
      final ResultSet dbResult = result;
      if (dbResult != null && sql != null) {
        final ResultSet cachedResultSet;
        try {
          cachedResultSet = new CachedResultSet(dbResult);
          tryCache(sql, cachedResultSet);
          cachedResultSet.beforeFirst();
          return resultClass.cast(cachedResultSet);
        } catch (final SQLException ex) {
          // ignore exception
        }
      }
    }

    return resultClass.cast(result);
  }

  /**
   * Returns the cached result for the given SQL if it is present and not expired. An expired entry
   * is removed and reported as a miss, so the query runs again and the stale copy is replaced.
   *
   * @param sql the SQL text used as the cache key.
   * @return the cached result set, or null if there is no usable entry.
   */
  protected @Nullable ResultSet getIfFresh(final String sql) {
    final CacheEntry entry = dataCache.get(sql);
    if (entry == null) {
      return null;
    }

    if (entry.isExpired()) {
      // remove(key, value) so a fresher entry stored concurrently by another thread survives.
      dataCache.remove(sql, entry);
      return null;
    }

    return entry.resultSet;
  }

  /**
   * Stores the given result under the given SQL, unless the cache is at its configured size limit.
   * Expired entries are purged first, so a cache that is full only of stale entries makes room for
   * the new one. When no room can be made the result is left uncached: the caller still receives it,
   * and the only consequence is that the next identical query hits the database again.
   *
   * @param sql       the SQL text used as the cache key.
   * @param resultSet the materialized result to store.
   */
  protected void tryCache(final String sql, final ResultSet resultSet) {
    if (dataCache.size() >= this.maxSize && !dataCache.containsKey(sql)) {
      removeExpiredEntries();

      if (dataCache.size() >= this.maxSize && !dataCache.containsKey(sql)) {
        LOGGER.finest(
            () -> Messages.get(
                "DataLocalCacheConnectionPlugin.cacheSizeLimitReached",
                new Object[]{this.maxSize, sql}));
        return;
      }
    }

    dataCache.put(sql, new CacheEntry(resultSet, this.ttlNanos));
  }

  protected static void removeExpiredEntries() {
    dataCache.forEach((key, entry) -> {
      if (entry.isExpired()) {
        dataCache.remove(key, entry);
      }
    });
  }

  protected @Nullable String getQuery(final @Nullable Object[] jdbcMethodArgs) {
    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString();
    }
    return null;
  }

  /** A cached result together with the deadline after which it must not be served. */
  protected static class CacheEntry {

    /** Sentinel time-to-live meaning "never expires". */
    protected static final long NO_EXPIRY = -1L;

    protected final ResultSet resultSet;
    protected final long expiresAtNanos;

    protected CacheEntry(final ResultSet resultSet, final long ttlNanos) {
      this.resultSet = resultSet;
      this.expiresAtNanos = ttlNanos == NO_EXPIRY ? NO_EXPIRY : System.nanoTime() + ttlNanos;
    }

    protected boolean isExpired() {
      // Subtraction rather than a direct comparison so the check stays correct across the
      // System.nanoTime() wraparound.
      return this.expiresAtNanos != NO_EXPIRY && System.nanoTime() - this.expiresAtNanos >= 0;
    }
  }

}
