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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class DataRemoteCachePlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(DataRemoteCachePlugin.class.getName());
  private static final String QUERY_HINT_START_PATTERN = "/*+";
  private static final String QUERY_HINT_END_PATTERN = "*/";
  private static final String CACHE_PARAM_PATTERN = "CACHE_PARAM(";
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName)));

  private PluginService pluginService;
  private TelemetryFactory telemetryFactory;
  private TelemetryCounter hitCounter;
  private TelemetryCounter missCounter;
  private TelemetryCounter totalCallsCounter;
  private TelemetryCounter malformedHintCounter;
  private CacheConnection cacheConnection;

  public DataRemoteCachePlugin(final PluginService pluginService, final Properties properties) {
    try {
      Class.forName("io.lettuce.core.RedisClient"); // Lettuce dependency
      Class.forName("org.apache.commons.pool2.impl.GenericObjectPool"); // Object pool dependency
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("DataRemoteCachePlugin.notInClassPath", new Object[] {e.getMessage()}));
    }
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.hitCounter = telemetryFactory.createCounter("remoteCache.cache.hit");
    this.missCounter = telemetryFactory.createCounter("remoteCache.cache.miss");
    this.totalCallsCounter = telemetryFactory.createCounter("remoteCache.cache.totalCalls");
    this.malformedHintCounter = telemetryFactory.createCounter("JdbcCacheMalformedQueryHint");
    this.cacheConnection = new CacheConnection(properties);
  }

  // Used for unit testing purposes only
  protected void setCacheConnection(CacheConnection conn) {
    this.cacheConnection = conn;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  private String getCacheQueryKey(String query) {
    // Check some basic session states. The important ones for caching include (but not limited to):
    //   schema name, username which can affect the query result from the DB in addition to the query string
    try {
      Connection currentConn = pluginService.getCurrentConnection();
      DatabaseMetaData metadata = currentConn.getMetaData();
      LOGGER.finest("DB driver protocol " + pluginService.getDriverProtocol()
          + ", schema: " + currentConn.getSchema()
          + ", database product: " + metadata.getDatabaseProductName() + " " + metadata.getDatabaseProductVersion()
          + ", user: " + metadata.getUserName()
          + ", driver: " + metadata.getDriverName() + " " + metadata.getDriverVersion());
      // The cache key contains the schema name, user name, and the query string
      String[] words = {currentConn.getSchema(), metadata.getUserName(), query};
      return String.join("_", words);
    } catch (SQLException e) {
      LOGGER.warning("Error getting session state: " + e.getMessage());
      return null;
    }
  }

  private ResultSet fetchResultSetFromCache(String queryStr) {
    if (cacheConnection == null) return null;

    String cacheQueryKey = getCacheQueryKey(queryStr);
    if (cacheQueryKey == null) return null; // Treat this as a cache miss
    byte[] cachedResult = cacheConnection.readFromCache(cacheQueryKey);
    if (cachedResult == null) return null;
    // Convert result into ResultSet
    try {
      return CachedResultSet.deserializeFromByteArray(cachedResult);
    } catch (Exception e) {
      LOGGER.warning("Error de-serializing cached result: " + e.getMessage());
      return null; // Treat this as a cache miss
    }
  }

  /**
   *  Cache the given ResultSet object.
   *  The ResultSet object passed in would be consumed to create a CacheResultSet object. It is returned
   *  for consumer consumption.
   */
  private ResultSet cacheResultSet(String queryStr, ResultSet rs, int expiry) throws SQLException {
    // Write the resultSet into the cache as a single key
    String cacheQueryKey = getCacheQueryKey(queryStr);
    if (cacheQueryKey == null) return rs; // Treat this condition as un-cacheable
    CachedResultSet crs = new CachedResultSet(rs);
    byte[] jsonString = crs.serializeIntoByteArray();
    cacheConnection.writeToCache(cacheQueryKey, jsonString, expiry);
    crs.beforeFirst();
    return crs;
  }

  /**
   * Determine the TTL based on an input query
   * @param queryHint string. e.g. "CACHE_PARAM(ttl=100s, key=custom)"
   * @return TTL in seconds to cache the query.
   *         null if the query is not cacheable.
   */
  protected Integer getTtlForQuery(String queryHint) {
    // Empty query is not cacheable
    if (StringUtils.isNullOrEmpty(queryHint)) return null;
    // Find CACHE_PARAM anywhere in the hint string (case insensitive)
    String upperHint = queryHint.toUpperCase();
    int cacheParamStart = upperHint.indexOf(CACHE_PARAM_PATTERN);
    if (cacheParamStart == -1) return null;

    // Find the matching closing parenthesis
    int paramsStart = cacheParamStart + CACHE_PARAM_PATTERN.length();
    int paramsEnd = upperHint.indexOf(")", paramsStart);
    if (paramsEnd == -1) return null;

    // Extract parameters between parentheses
    String cacheParams = upperHint.substring(paramsStart, paramsEnd).trim();
    // Empty parameters
    if (StringUtils.isNullOrEmpty(cacheParams)) {
      LOGGER.warning("Empty CACHE_PARAM parameters");
      incrCounter(malformedHintCounter);
      return null;
    }

    // Parse comma-separated parameters
    String[] params = cacheParams.split(",");
    Integer ttlValue = null;

    for (String param : params) {
      String[] keyValue = param.trim().split("=");
      if (keyValue.length != 2) {
        LOGGER.warning("Invalid caching parameter format: " + param);
        incrCounter(malformedHintCounter);
        return null;
      }
      String key = keyValue[0].trim();
      String value = keyValue[1].trim();

      if ("TTL".equals(key)) {
        if (!value.endsWith("S")) {
          LOGGER.warning("TTL must end with 's': " + value);
          incrCounter(malformedHintCounter);
          return null;
        } else{
          // Parse TTL value (e.g., "300s")
          try {
            ttlValue = Integer.parseInt(value.substring(0, value.length() - 1));
            // treat negative and 0 ttls as not cacheable
            if (ttlValue <= 0) {
              return null;
            }
          } catch (NumberFormatException e) {
            LOGGER.warning(String.format("Invalid TTL format of %s for query %s", value, queryHint));
            incrCounter(malformedHintCounter);
            return null;
          }
        }
      }
    }
    return ttlValue;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {
    ResultSet result;
    boolean needToCache = false;
    final String sql = getQuery(jdbcMethodArgs);

    // If the query is cacheable, we try to fetch the query result from the cache.
    boolean isInTransaction = pluginService.isInTransaction();
    // Get the query hint part in front of the query itself
    String mainQuery = sql; // The main part of the query with the query hint prefix trimmed
    int endOfQueryHint = 0;
    Integer configuredQueryTtl = null;
    // Queries longer than 16KB is not cacheable
    if ((sql.length() < 16000) && sql.startsWith(QUERY_HINT_START_PATTERN)) {
      endOfQueryHint = sql.indexOf(QUERY_HINT_END_PATTERN);
      if (endOfQueryHint > 0) {
        configuredQueryTtl = getTtlForQuery(sql.substring(2, endOfQueryHint).trim());
        mainQuery = sql.substring(endOfQueryHint + 2).trim();
      }
    }

    // Query result can be served from the cache if it has a configured TTL value, and it is
    // not executed in a transaction as a transaction typically need to return consistent results.
    if (!isInTransaction && (configuredQueryTtl != null)) {
      incrCounter(totalCallsCounter);
      result = fetchResultSetFromCache(mainQuery);
      if (result == null) {
        // Cache miss. Need to fetch result from the database
        needToCache = true;
        incrCounter(missCounter);
        LOGGER.finest("Got a cache miss for SQL: " + sql);
      } else {
        LOGGER.finest("Got a cache hit for SQL: " + sql);
        // Cache hit. Return the cached result
        incrCounter(hitCounter);
        try {
          result.beforeFirst();
        } catch (final SQLException ex) {
          throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
        }
        return resultClass.cast(result);
      }
    }

    result = (ResultSet) jdbcMethodFunc.call();

    // We need to cache the query result if we got a cache miss for the query result,
    // or the query is cacheable and executed inside a transaction.
    if (isInTransaction && (configuredQueryTtl != null)) {
      needToCache = true;
    }
    if (needToCache) {
      try {
        result = cacheResultSet(mainQuery, result, configuredQueryTtl);
      } catch (final SQLException ex) {
        // Log and re-throw exception
        LOGGER.warning("Encountered SQLException when caching query results: " + ex.getMessage());
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    return resultClass.cast(result);
  }

  private void incrCounter(TelemetryCounter counter) {
    if (counter == null) return;
    counter.inc();
  }

  protected String getQuery(final Object[] jdbcMethodArgs) {
    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString().trim();
    }
    return null;
  }
}
