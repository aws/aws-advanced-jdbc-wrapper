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
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;
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
                  "Statement.executeQuery",
                  "Statement.executeUpdate",
                  "Statement.execute",
                  "PreparedStatement.execute",
                  "PreparedStatement.executeQuery",
                  "PreparedStatement.executeUpdate",
                  "PreparedStatement.executeLargeUpdate",
                  "CallableStatement.execute",
                  "CallableStatement.executeQuery",
                  "CallableStatement.executeUpdate",
                  "CallableStatement.executeLargeUpdate")));

  private static final Set<String> methodWithQueryArg =
      new HashSet<>(
          Arrays.asList(
              "Statement.execute",
              "Statement.executeQuery",
              "Statement.executeUpdate",
              "PreparedStatement.execute",
              "PreparedStatement.executeQuery",
              "PreparedStatement.executeUpdate",
              "CallableStatement.execute",
              "CallableStatement.executeQuery",
              "CallableStatement.executeUpdate"));

  private static final Set<String> methodWithNoArg =
      new HashSet<>(
          Arrays.asList(
              "PreparedStatement.execute",
              "PreparedStatement.executeQuery",
              "PreparedStatement.executeUpdate",
              "PreparedStatement.executeLargeUpdate",
              "CallableStatement.execute",
              "CallableStatement.executeQuery",
              "CallableStatement.executeUpdate",
              "CallableStatement.executeLargeUpdate"));

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

  public LogQueryConnectionPlugin(Properties props) {
    this.enhancedLogQueryEnabled = ENHANCED_LOG_QUERY_ENABLED.getBoolean(props);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {

    String sql = getQuery(methodInvokeOn, methodName, jdbcMethodArgs);

    if (!StringUtils.isNullOrEmpty(sql)) {
      LOGGER.fine(
          () -> Messages.get(
              "LogQueryConnectionPlugin.executingQuery",
              new Object[] {methodName, sql}));
    }

    return jdbcMethodFunc.call();
  }

  protected <T> String getQuery(Object methodInvokeOn, String methodName, Object[] jdbcMethodArgs) {

    // Get query from method argument
    if (methodWithQueryArg.contains(methodName)
        && jdbcMethodArgs != null
        && jdbcMethodArgs.length > 0) {
      return jdbcMethodArgs[0] == null ? null : jdbcMethodArgs[0].toString();
    }

    if (!this.enhancedLogQueryEnabled || methodInvokeOn == null) {
      return null;
    }

    String targetClassName = methodInvokeOn.getClass().getName();

    // Get query from object internal variable
    if (methodWithNoArg.contains(methodName)
        && (jdbcMethodArgs == null || jdbcMethodArgs.length == 0)) {

      String accessor = queryAccessorByClassName.get(targetClassName);
      if (accessor != null) {
        Object query = WrapperUtils.getFieldValue(methodInvokeOn, accessor);
        return query == null ? null : query.toString();
      }
    }

    return null;
  }
}
