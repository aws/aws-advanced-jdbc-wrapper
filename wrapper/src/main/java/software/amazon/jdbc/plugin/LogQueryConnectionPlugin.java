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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

/*
 * The plugin logs a SQL statement to be executed. When SQL statement isn't passed as a method parameter,
 * it may require use of reflection to obtain SQL statement from a target object internal members. Using reflection
 * can cause a performance degradation. User needs to explicitly allow using reflection by setting
 * configuration parameter 'enhancedLogQueryEnabled' to true.
 */
public class LogQueryConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(LogQueryConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
                  JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
                  JdbcMethod.STATEMENT_EXECUTE.methodName,
                  JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
                  JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
                  JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
                  JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
                  JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
                  JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
                  JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,
                  JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName)));

  private static final Set<String> methodWithQueryArg =
      new HashSet<>(
          Arrays.asList(
              JdbcMethod.STATEMENT_EXECUTE.methodName,
              JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
              JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName));

  private static final Set<String> methodWithNoArg =
      new HashSet<>(
          Arrays.asList(
              JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName,
              JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,
              JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName));

  private final Map<String, String> queryAccessorByClassName =
      new HashMap<String, String>() {
        {
          put("com.mysql.cj.jdbc.ClientPreparedStatement", "query.originalSql");
          put("com.mysql.cj.jdbc.CallableStatement", "query.originalSql");
          put("org.postgresql.jdbc.PgPreparedStatement", "preparedQuery.key");
          put("org.postgresql.jdbc.PgCallableStatement", "preparedQuery.key.sql");
          put("org.mariadb.jdbc.ClientPreparedStatement", "sql");
        }
      };

  public static final AwsWrapperProperty ENHANCED_LOG_QUERY_ENABLED =
      new AwsWrapperProperty(
          "enhancedLogQueryEnabled",
          "false",
          "Allows the 'logQuery' plugin to inspect object internals to get prepared SQL statements and batches.");

  protected final boolean enhancedLogQueryEnabled;

  static {
    PropertyDefinition.registerPluginProperties(LogQueryConnectionPlugin.class);
  }

  public LogQueryConnectionPlugin(final Properties props) {
    this.enhancedLogQueryEnabled = ENHANCED_LOG_QUERY_ENABLED.getBoolean(props);
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

    final String sql = getQuery(methodInvokeOn, methodName, jdbcMethodArgs);

    if (!StringUtils.isNullOrEmpty(sql)) {
      LOGGER.fine(
          () ->
              Messages.get(
                  "LogQueryConnectionPlugin.executingQuery", new Object[] {methodName, sql}));
    }

    return jdbcMethodFunc.call();
  }

  protected <T> String getQuery(
      final Object methodInvokeOn, final String methodName, final Object[] jdbcMethodArgs) {

    // Get query from method argument
    if (methodWithQueryArg.contains(methodName)
        && jdbcMethodArgs != null
        && jdbcMethodArgs.length > 0) {
      return jdbcMethodArgs[0] == null ? null : jdbcMethodArgs[0].toString();
    }

    if (!this.enhancedLogQueryEnabled || methodInvokeOn == null) {
      return null;
    }

    final String targetClassName = methodInvokeOn.getClass().getName();

    // Get query from object internal variable
    if (methodWithNoArg.contains(methodName)
        && (jdbcMethodArgs == null || jdbcMethodArgs.length == 0)) {

      final String accessor = queryAccessorByClassName.get(targetClassName);
      if (accessor != null) {
        final Object query = WrapperUtils.getFieldValue(methodInvokeOn, accessor);
        return query == null ? null : query.toString();
      }
    }

    return null;
  }
}
