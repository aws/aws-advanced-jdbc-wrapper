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

package software.amazon.jdbc.plugin;

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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.util.CacheConnection;
import software.amazon.jdbc.util.CachedResultSet;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class DataRemoteCachePlugin extends AbstractConnectionPlugin {

  private static long connectTime = 0L;
  private static final Logger LOGGER = Logger.getLogger(DataRemoteCachePlugin.class.getName());

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("Statement.executeQuery", "Statement.execute",
          "PreparedStatement.execute", "PreparedStatement.executeQuery",
          "CallableStatement.execute", "CallableStatement.executeQuery",
          "connect", "forceConnect")));

  static {
    PropertyDefinition.registerPluginProperties(DataRemoteCachePlugin.class);
  }

  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final TelemetryCounter hitCounter;
  private final TelemetryCounter missCounter;
  private final TelemetryCounter totalCallsCounter;
  private final CacheConnection cacheConnection;

  public DataRemoteCachePlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.hitCounter = telemetryFactory.createCounter("remoteCache.cache.hit");
    this.missCounter = telemetryFactory.createCounter("remoteCache.cache.miss");
    this.totalCallsCounter = telemetryFactory.createCounter("remoteCache.cache.totalCalls");
    this.cacheConnection = new CacheConnection(properties);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  private Connection connectHelper(JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    final long startTime = System.nanoTime();

    final Connection result = connectFunc.call();

    final long elapsedTimeNanos = System.nanoTime() - startTime;
    connectTime += elapsedTimeNanos;
    LOGGER.fine(
        () -> Messages.get(
            "DataRemoteCachePlugin.cacheConnectTime",
            new Object[] {elapsedTimeNanos}));
    return result;
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    System.out.println("DataRemoteCachingPlugin.connect()...");
    return this.connectHelper(connectFunc);
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    System.out.println("DataRemoteCachingPlugin.forceConnect()...");
    return this.connectHelper(forceConnectFunc);
  }

  private String getCacheQueryKey(String query) {
    // Check some basic session states. The important ones for caching include (but not limited to):
    //   schema name, username which can affect the query result from the DB in addition to the query string
    try {
      Connection currentConn = pluginService.getCurrentConnection();
      DatabaseMetaData metadata = currentConn.getMetaData();
      SessionStateService sessionStateService = pluginService.getSessionStateService();
      System.out.println("DB driver protocol " + pluginService.getDriverProtocol()
          + ", schema: " + currentConn.getSchema()
          + ", database product: " + metadata.getDatabaseProductName() + " " + metadata.getDatabaseProductVersion()
          + ", user: " + metadata.getUserName()
          + ", driver: " + metadata.getDriverName() + " " + metadata.getDriverVersion());
      // The cache key contains the schema name, user name, and the query string
      String[] words = {currentConn.getSchema(), metadata.getUserName(), query};
      return String.join("_", words);
    } catch (SQLException e) {
      System.out.println("Error getting session state: " + e.getMessage());
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
      System.out.println("Error de-serializing cached result: " + e.getMessage());
      return null; // Treat this as a cache miss
    }
  }

  private void cacheResultSet(String queryStr, ResultSet rs) throws SQLException {
    System.out.println("Caching resultSet returned from postgres database ....... ");
    String jsonValue = CachedResultSet.serializeIntoJsonString(rs);

    // Write the resultSet into the cache as a single key
    String cacheQueryKey = getCacheQueryKey(queryStr);
    if (cacheQueryKey == null) return; // Treat this condition as un-cacheable
    cacheConnection.writeToCache(cacheQueryKey, jsonValue.getBytes(StandardCharsets.UTF_8));
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

    // Try to fetch SELECT query from the cache
    if (!StringUtils.isNullOrEmpty(sql) && sql.startsWith("select ")) {
      result = fetchResultSetFromCache(sql);
      if (result == null) {
        System.out.println("We got a cache MISS.........");
        // Cache miss. Need to fetch result from the database
        needToCache = true;
        missCounter.inc();
        LOGGER.finest(
            () -> Messages.get(
                "DataRemoteCachePlugin.queryResultsCached",
                new Object[]{methodName, sql}));
      } else {
        System.out.println("We got a cache hit.........");
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
        cacheResultSet(sql, result);
        result.beforeFirst();
      } catch (final SQLException ex) {
        // ignore exception
        System.out.println("Encountered SQLException when caching results...");
      }
    }

    return resultClass.cast(result);
  }

  protected String getQuery(final Object[] jdbcMethodArgs) {
    // Get query from method argument
    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0 && jdbcMethodArgs[0] != null) {
      return jdbcMethodArgs[0].toString();
    }
    return null;
  }
}
