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

import static software.amazon.jdbc.plugin.cache.CacheAuthorizationTracker.appendAuthorizationStateParts;
import static software.amazon.jdbc.plugin.cache.CacheAuthorizationTracker.appendCacheKeyPart;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class DataLocalCacheConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(DataLocalCacheConnectionPlugin.class.getName());

  private static final Set<String> QUERY_METHODS = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName
      )));

  private static final Set<String> CACHING_SUBSCRIBED_METHODS =
      CacheAuthorizationTracker.withTrackedMethods(QUERY_METHODS);

  // One warning per JVM, because every pooled connection would otherwise repeat it.
  private static final AtomicBoolean unsupportedDriverWarningLogged = new AtomicBoolean(false);

  public static final AwsWrapperProperty DATA_CACHE_TRIGGER_CONDITION = new AwsWrapperProperty(
      "dataCacheTriggerCondition", null,
      "A regular expression that, if it's matched, allows the plugin to cache SQL results.");

  // Both bounds are declared nonNegative() so a negative value fails fast when the property is
  // read, matching how the rest of the driver treats durations and sizes. Without it, a user who
  // wrote -1 expecting "unlimited" would silently get the opposite of what they asked for: the
  // sentinel meaning is carried by 0, not by a negative number.
  public static final AwsWrapperProperty DATA_CACHE_TTL_MS = new AwsWrapperProperty(
      "dataCacheTtlMs", "300000",
      "Time in milliseconds that a cached result set stays valid. A cached result is not refreshed "
          + "by writes, so this is the longest an application can observe stale data. Set to 0 to "
          + "keep entries until the cache is cleared, accepting unbounded staleness.").nonNegative();

  public static final AwsWrapperProperty DATA_CACHE_MAX_SIZE = new AwsWrapperProperty(
      "dataCacheMaxSize", "1000",
      "Maximum number of distinct cache keys whose results are held in the cache. When the "
          + "limit is reached, expired entries are purged; if the cache is still full, further "
          + "results are returned to the caller without being cached. Set to 0 for no limit.")
      .nonNegative();

  /**
   * Process-wide cache of materialized query results. The key combines the SQL text with the
   * server, database, schema, user and authorization session state the query ran under, so that
   * connections with a different database identity do not share entries. Each entry carries its
   * own expiry deadline, computed from the {@code dataCacheTtlMs} of the connection that stored
   * it, so a single static cache can be shared by connections configured differently.
   */
  protected static final Map<String, CacheEntry> dataCache = new ConcurrentHashMap<>();

  protected final @Nullable String dataCacheTriggerCondition;
  protected final long ttlNanos;
  protected final int maxSize;

  static {
    PropertyDefinition.registerPluginProperties(DataLocalCacheConnectionPlugin.class);
  }

  private final PluginService pluginService;
  private final CacheAuthorizationTracker authorizationTracker;
  private final String dbUserName;
  private final TelemetryFactory telemetryFactory;
  private final @Nullable TelemetryCounter hitCounter;
  private final @Nullable TelemetryCounter missCounter;
  private final @Nullable TelemetryCounter totalCallsCounter;
  private final @Nullable TelemetryGauge cacheSizeGauge;

  public DataLocalCacheConnectionPlugin(final PluginService pluginService, final Properties props) {
    this.pluginService = pluginService;
    this.authorizationTracker = new CacheAuthorizationTracker(pluginService, true);
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.dataCacheTriggerCondition = DATA_CACHE_TRIGGER_CONDITION.getString(props);
    final String configuredUser = PropertyDefinition.USER.getString(props);
    this.dbUserName = configuredUser == null ? "" : configuredUser;

    // getLong rejects a negative value, so only 0 (the documented "no limit" / "never expires"
    // opt-out) and positive values reach the checks below.
    final long ttlMs = DATA_CACHE_TTL_MS.getLong(props);
    this.ttlNanos = ttlMs == 0 ? CacheEntry.NO_EXPIRY : TimeUnit.MILLISECONDS.toNanos(ttlMs);
    final long configuredMaxSize = DATA_CACHE_MAX_SIZE.getLong(props);
    this.maxSize = configuredMaxSize == 0
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
    return StringUtils.isNullOrEmpty(this.dataCacheTriggerCondition)
        ? QUERY_METHODS
        : CACHING_SUBSCRIBED_METHODS;
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(
      final EnumSet<NodeChangeOptions> changes) {
    if (StringUtils.isNullOrEmpty(this.dataCacheTriggerCondition)) {
      return OldConnectionSuggestedAction.NO_OPINION;
    }
    if (changes.contains(NodeChangeOptions.INITIAL_CONNECTION)
        && !this.pluginService.getTargetDriverDialect().supportsAuthorizationSessionState()
        && unsupportedDriverWarningLogged.compareAndSet(false, true)) {
      LOGGER.warning(
          () -> Messages.get(
              "DataLocalCacheConnectionPlugin.unsupportedTargetDriver",
              new Object[]{this.pluginService.getTargetDriverDialect().getClass().getSimpleName()}));
    }
    this.authorizationTracker.notifyConnectionChanged(changes);
    return OldConnectionSuggestedAction.NO_OPINION;
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

    final String triggerCondition = this.dataCacheTriggerCondition;
    if (StringUtils.isNullOrEmpty(triggerCondition)) {
      return jdbcMethodFunc.call();
    }

    if (resultClass != ResultSet.class) {
      return this.authorizationTracker.execute(
          methodInvokeOn, methodName, jdbcMethodFunc, jdbcMethodArgs);
    }

    if (this.totalCallsCounter != null) {
      this.totalCallsCounter.inc();
    }

    final String sql = getQuery(jdbcMethodArgs);
    @Nullable String cacheKey = null;
    if (!StringUtils.isNullOrEmpty(sql)
        && sql.matches(triggerCondition)
        && this.authorizationTracker.isCacheableQuery(methodName, sql)) {
      cacheKey = this.getCacheKey(sql);
    }

    long lookupGeneration = -1;
    if (cacheKey != null) {
      lookupGeneration = this.authorizationTracker.getConnectionGeneration();
      final CachedResultSet cached = getIfFresh(cacheKey);
      if (cached != null) {
        if (this.hitCounter != null) {
          this.hitCounter.inc();
        }
        return resultClass.cast(cached.copyWithOwnCursor());
      }
      if (this.missCounter != null) {
        this.missCounter.inc();
      }
      LOGGER.finest(
          () -> Messages.get(
              "DataLocalCacheConnectionPlugin.queryResultsCached",
              new Object[]{methodName, sql}));
    }

    final ResultSet result = (ResultSet) this.authorizationTracker.execute(
        methodInvokeOn, methodName, jdbcMethodFunc, jdbcMethodArgs);

    // A connection change, such as failover, between the lookup and this point means the result
    // came from a connection the key was not built from, so it is returned without being cached.
    if (cacheKey != null
        && result != null
        && lookupGeneration == this.authorizationTracker.getConnectionGeneration()) {
      final CachedResultSet cachedResultSet;
      try {
        cachedResultSet = new CachedResultSet(result);
      } catch (final SQLException ex) {
        // The rows read so far have been consumed, so the partial result cannot be returned.
        LOGGER.log(Level.WARNING, Messages.get("DataLocalCacheConnectionPlugin.errorCachingResult"), ex);
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
      tryCache(cacheKey, cachedResultSet);
      return resultClass.cast(cachedResultSet.copyWithOwnCursor());
    }

    return resultClass.cast(result);
  }

  /**
   * Builds the cache key for a query from the server it runs on, the catalog, schema and user of
   * the connection, the tracked authorization session state, and the SQL text. Every component is
   * length-prefixed so that component boundaries are unambiguous. Nothing here writes session
   * state, which failover and read/write splitting would otherwise replay onto a new connection.
   *
   * @param query the SQL text
   * @return the cache key, or {@code null} if the session state needed for the key is unavailable,
   *     in which case the query bypasses the cache
   */
  private @Nullable String getCacheKey(final String query) {
    final @Nullable AuthorizationSessionState authorizationState =
        this.authorizationTracker.getAuthorizationStateForCacheKey();
    if (authorizationState == null) {
      return null;
    }

    try {
      final HostSpec hostSpec = this.pluginService.getCurrentHostSpec();
      final @Nullable String schemaName =
          this.pluginService.getSessionStateService().getSchema().orElse(null);
      // PostgreSQL does not report the database in the authorization state, and its driver ignores
      // setCatalog, so the database is read from the connection. The PostgreSQL JDBC driver reads it
      // once per physical connection. MySQL and MariaDB report it as the current database unless none
      // is selected.
      final @Nullable String catalogName = authorizationState.getCurrentDatabase().isEmpty()
          ? this.pluginService.getCurrentConnection().getCatalog()
          : this.pluginService.getSessionStateService().getCatalog().orElse(null);

      final StringBuilder cacheKey = new StringBuilder();
      appendCacheKeyPart(cacheKey, hostSpec.getHost());
      appendCacheKeyPart(cacheKey, String.valueOf(hostSpec.getPort()));
      appendCacheKeyPart(cacheKey, catalogName);
      appendCacheKeyPart(cacheKey, schemaName);
      appendCacheKeyPart(cacheKey, this.dbUserName);
      appendAuthorizationStateParts(cacheKey, authorizationState);
      appendCacheKeyPart(cacheKey, query);
      return cacheKey.toString();
    } catch (final SQLException e) {
      LOGGER.log(Level.FINE, Messages.get("DataLocalCacheConnectionPlugin.errorBuildingCacheKey"), e);
      return null;
    }
  }

  /**
   * Returns the cached result for the given key if it is present and not expired. An expired entry
   * is removed and reported as a miss, so the query runs again and the stale copy is replaced.
   *
   * @param cacheKey the cache key for the query.
   * @return the cached result set, or null if there is no usable entry.
   */
  protected @Nullable CachedResultSet getIfFresh(final String cacheKey) {
    final CacheEntry entry = dataCache.get(cacheKey);
    if (entry == null) {
      return null;
    }

    if (entry.isExpired()) {
      // remove(key, value) so a fresher entry stored concurrently by another thread survives.
      dataCache.remove(cacheKey, entry);
      return null;
    }

    return entry.resultSet;
  }

  /**
   * Stores the given result under the given key, unless the cache is at its configured size limit.
   * Expired entries are purged first, so a cache that is full only of stale entries makes room for
   * the new one. When no room can be made the result is left uncached: the caller still receives it,
   * and the only consequence is that the next identical query hits the database again.
   *
   * @param cacheKey  the cache key for the query.
   * @param resultSet the materialized result to store.
   */
  protected void tryCache(final String cacheKey, final CachedResultSet resultSet) {
    if (dataCache.size() >= this.maxSize && !dataCache.containsKey(cacheKey)) {
      removeExpiredEntries();

      if (dataCache.size() >= this.maxSize && !dataCache.containsKey(cacheKey)) {
        LOGGER.finest(
            () -> Messages.get(
                "DataLocalCacheConnectionPlugin.cacheSizeLimitReached",
                new Object[]{this.maxSize, cacheKey}));
        return;
      }
    }

    dataCache.put(cacheKey, new CacheEntry(resultSet, this.ttlNanos));
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

    protected final CachedResultSet resultSet;
    protected final long expiresAtNanos;

    protected CacheEntry(final CachedResultSet resultSet, final long ttlNanos) {
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
