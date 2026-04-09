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

package software.amazon.jdbc.plugin.sqlparser;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.EncryptionAnnotationParser;
import software.amazon.jdbc.parser.JSQLParserAnalyzer;
import software.amazon.jdbc.parser.SqlAnalysisService;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;

/**
 * Plugin that parses SQL statements and populates the {@link PluginCallContext}
 * with analysis results for downstream plugins to consume.
 *
 * <p>This plugin must be placed before any plugin that needs SQL analysis
 * (e.g., kmsEncryption, autoReadWriteSplitting).
 */
public class SqlParserConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(SqlParserConnectionPlugin.class.getName());

  private static final Pattern ROUTING_HINT_PATTERN =
      Pattern.compile("/\\*\\s*@\\s*(reader|writer)\\s*\\*/", Pattern.CASE_INSENSITIVE);

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName);
          add(JdbcMethod.CONNECTION_PREPARECALL.methodName);
          add(JdbcMethod.STATEMENT_EXECUTE.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_CLOSE.methodName);
        }
      });

  private final PluginService pluginService;

  // Cache parsed SQL per PreparedStatement so context can be re-populated on execute
  private final Map<PreparedStatement, String> preparedStatementSqlCache =
      Collections.synchronizedMap(new WeakHashMap<>());

  public SqlParserConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
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

    // On prepareStatement close: remove cached SQL
    if ("PreparedStatement.close".equals(methodName)
        && methodInvokeOn instanceof PreparedStatement) {
      preparedStatementSqlCache.remove(methodInvokeOn);
      return jdbcMethodFunc.call();
    }

    // On prepareStatement: parse SQL, cache it, and return early
    if (methodName.startsWith("Connection.prepare")) {
      if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0
          && jdbcMethodArgs[0] instanceof String) {
        String sql = (String) jdbcMethodArgs[0];
        populateContext(sql);
        T result = jdbcMethodFunc.call();
        if (result instanceof PreparedStatement) {
          preparedStatementSqlCache.put((PreparedStatement) result, sql);
        }
        return result;
      }
    } else if (methodName.startsWith("PreparedStatement.execute")
        && methodInvokeOn instanceof PreparedStatement) {
      // Re-populate context from cached SQL (context was reset between prepare and execute)
      String cachedSql = preparedStatementSqlCache.get(methodInvokeOn);
      if (cachedSql != null) {
        populateContext(cachedSql);
      }
    } else if (methodName.startsWith("Statement.execute")) {
      // Parse SQL from args; executeBatch has no SQL arg and will be skipped,
      // causing downstream plugins to fall back to method-name heuristics (routes to writer)
      if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0
          && jdbcMethodArgs[0] instanceof String) {
        populateContext((String) jdbcMethodArgs[0]);
      }
    }

    return jdbcMethodFunc.call();
  }

  private void populateContext(String sql) {
    PluginCallContext ctx = pluginService.getCallContext();

    // Parse routing hint
    Matcher routingMatcher = ROUTING_HINT_PATTERN.matcher(sql);
    if (routingMatcher.find()) {
      ctx.setAttribute(SqlContextKeys.ROUTING_HINT, routingMatcher.group(1).toLowerCase());
    }

    // Parse and strip annotations
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);
    String cleanSql = EncryptionAnnotationParser.stripAnnotations(sql);
    cleanSql = ROUTING_HINT_PATTERN.matcher(cleanSql).replaceAll("").trim();

    ctx.setAttribute(SqlContextKeys.ANNOTATIONS, annotations);
    ctx.setAttribute(SqlContextKeys.CLEAN_SQL, cleanSql);

    // Parse SQL structure
    JSQLParserAnalyzer.QueryAnalysis analysis = JSQLParserAnalyzer.analyze(cleanSql);

    Set<String> tables = new HashSet<>();
    for (String table : analysis.tables) {
      tables.add(table.replace("`", "").replace("\"", ""));
    }

    ctx.setAttribute(SqlContextKeys.QUERY_TYPE, analysis.queryType);
    ctx.setAttribute(SqlContextKeys.TABLES, tables);
    ctx.setAttribute(SqlContextKeys.FOR_UPDATE, analysis.forUpdate);

    // Build parameter mapping
    Map<Integer, String> paramMapping = new HashMap<>();
    paramMapping.putAll(SqlAnalysisService.getColumnParameterMapping(cleanSql));
    paramMapping.putAll(annotations);
    ctx.setAttribute(SqlContextKeys.PARAM_MAPPING, paramMapping);
  }
}
