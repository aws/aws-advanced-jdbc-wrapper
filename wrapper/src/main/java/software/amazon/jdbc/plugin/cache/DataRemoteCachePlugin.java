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

import java.nio.charset.StandardCharsets;
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
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class DataRemoteCachePlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(DataRemoteCachePlugin.class.getName());
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("Statement.executeQuery", "Statement.execute",
          "PreparedStatement.execute", "PreparedStatement.executeQuery",
          "CallableStatement.execute", "CallableStatement.executeQuery")));

  static {
    PropertyDefinition.registerPluginProperties(DataRemoteCachePlugin.class);
  }

  private PluginService pluginService;
  private TelemetryFactory telemetryFactory;
  private TelemetryCounter hitCounter;
  private TelemetryCounter missCounter;
  private TelemetryCounter totalCallsCounter;
  private CacheConnection cacheConnection;

  public DataRemoteCachePlugin(final PluginService pluginService, final Properties properties) {
    try {
      Class.forName("io.lettuce.core.RedisClient"); // Lettuce dependency
      Class.forName("org.apache.commons.pool2.impl.GenericObjectPool"); // Object pool dependency
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper"); // Jackson dependency
      Class.forName("com.fasterxml.jackson.datatype.jsr310.JavaTimeModule"); // JSR310 dependency
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("DataRemoteCachePlugin.notInClassPath", new Object[] {e.getMessage()}));
    }
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.hitCounter = telemetryFactory.createCounter("remoteCache.cache.hit");
    this.missCounter = telemetryFactory.createCounter("remoteCache.cache.miss");
    this.totalCallsCounter = telemetryFactory.createCounter("remoteCache.cache.totalCalls");
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
    byte[] result = cacheConnection.readFromCache(cacheQueryKey);
    if (result == null) return null;
    // Convert result into ResultSet
    try {
      return CachedResultSet.deserializeFromJsonString(new String(result, StandardCharsets.UTF_8));
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
    String jsonValue = crs.serializeIntoJsonString();
    cacheConnection.writeToCache(cacheQueryKey, jsonValue.getBytes(StandardCharsets.UTF_8), expiry);
    crs.beforeFirst();
    return crs;
  }

  /**
   * Determine the TTL based on an input query
   * @param queryHint string. e.g. "NO CACHE", or "cacheTTL=100s"
   * @return TTL in seconds to cache the query.
   *         null if the query is not cacheable.
   */
  protected Integer getTtlForQuery(String queryHint) {
    // Empty query is not cacheable
    if (StringUtils.isNullOrEmpty(queryHint)) return null;
    // Query longer than 16K is not cacheable
    String[] tokens = queryHint.toLowerCase().split("cache");
    if (tokens.length >= 2) {
      // Handle "no cache".
      if (!StringUtils.isNullOrEmpty(tokens[0]) && "no".equals(tokens[0])) return null;
      // Handle "cacheTTL=Xs"
      if (!StringUtils.isNullOrEmpty(tokens[1]) && tokens[1].startsWith("ttl=")) {
        int endIndex = tokens[1].indexOf('s');
        if (endIndex > 0) {
          try {
            return Integer.parseInt(tokens[1].substring(4, endIndex));
          } catch (Exception e) {
            LOGGER.warning("Encountered exception when parsing Cache TTL: " + e.getMessage());
          }
        }
      }
    }

    LOGGER.finest("Query hint " + queryHint + " indicates the query is not cacheable");
    return null;
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
    totalCallsCounter.inc();

    ResultSet result;
    boolean needToCache = false;
    final String sql = getQuery(jdbcMethodArgs);

    // If the query is cacheable, we try to fetch the query result from the cache.
    boolean isInTransaction = pluginService.isInTransaction();
    // Get the query hint part in front of the query itself
    String mainQuery = sql; // The main part of the query with the query hint prefix trimmed
    int endOfQueryHint = 0;
    Integer configuredQueryTtl = null;
    if ((sql.length() < 16000) && sql.startsWith("/*")) {
      endOfQueryHint = sql.indexOf("*/");
      if (endOfQueryHint > 0) {
        configuredQueryTtl = getTtlForQuery(sql.substring(2, endOfQueryHint).trim());
        mainQuery = sql.substring(endOfQueryHint + 2).trim();
      }
    }

    // Query result can be served from the cache if it has a configured TTL value, and it is
    // not executed in a transaction as a transaction typically need to return consistent results.
    if (!isInTransaction && (configuredQueryTtl != null)) {
      result = fetchResultSetFromCache(mainQuery);
      if (result == null) {
        // Cache miss. Need to fetch result from the database
        needToCache = true;
        missCounter.inc();
        LOGGER.finest("Got a cache miss for SQL: " + sql);
      } else {
        LOGGER.finest("Got a cache hit for SQL: " + sql);
        // Cache hit. Return the cached result
        hitCounter.inc();
        try {
          result.beforeFirst();
        } catch (final SQLException ex) {
          if (exceptionClass.isAssignableFrom(ex.getClass())) {
            throw exceptionClass.cast(ex);
          }
          throw new RuntimeException(ex);
        }
        return resultClass.cast(result);
      }
    }

    result = (ResultSet) jdbcMethodFunc.call();

    if (needToCache) {
      try {
        result = cacheResultSet(mainQuery, result, configuredQueryTtl);
      } catch (final SQLException ex) {
        // ignore exception
        LOGGER.warning("Encountered SQLException when caching results: " + ex.getMessage());
      }
    }

    return resultClass.cast(result);
  }

  protected String getQuery(final Object[] jdbcMethodArgs) {
    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString().trim();
    }
    return null;
  }
}
