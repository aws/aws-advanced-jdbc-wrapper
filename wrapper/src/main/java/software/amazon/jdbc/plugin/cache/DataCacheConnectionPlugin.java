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
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class DataCacheConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(DataCacheConnectionPlugin.class.getName());

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
      "dataCacheTriggerCondition", "false",
      "A regular expression that, if it's matched, allows the plugin to cache SQL results.");

  protected static final Map<String, ResultSet> dataCache = new ConcurrentHashMap<>();

  protected final String dataCacheTriggerCondition;

  static {
    PropertyDefinition.registerPluginProperties(DataCacheConnectionPlugin.class);
  }

  private final TelemetryFactory telemetryFactory;
  private final TelemetryCounter hitCounter;
  private final TelemetryCounter missCounter;
  private final TelemetryCounter totalCallsCounter;
  private final TelemetryGauge cacheSizeGauge;

  public DataCacheConnectionPlugin(final PluginService pluginService, final Properties props) {
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.dataCacheTriggerCondition = DATA_CACHE_TRIGGER_CONDITION.getString(props);

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
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
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
      result = dataCache.get(sql);
      if (result == null) {
        needToCache = true;
        if (this.missCounter != null) {
          this.missCounter.inc();
        }
        LOGGER.finest(
            () -> Messages.get(
                "DataCacheConnectionPlugin.queryResultsCached",
                new Object[]{methodName, sql}));
      } else {
        if (this.hitCounter != null) {
          this.hitCounter.inc();
        }
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
      final ResultSet cachedResultSet;
      try {
        cachedResultSet = new CachedResultSet(result);
        dataCache.put(sql, cachedResultSet);
        cachedResultSet.beforeFirst();
        return resultClass.cast(cachedResultSet);
      } catch (final SQLException ex) {
        // ignore exception
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
