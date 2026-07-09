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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.EncryptionAnnotationParser;
import software.amazon.jdbc.parser.JSQLParserAnalyzer;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
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

  private static final Pattern ROUTING_HINT_PATTERN =
      Pattern.compile("/\\*\\s*@\\s*(reader|writer|keep)\\s*\\*/", Pattern.CASE_INSENSITIVE);

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
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_CLOSE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_CLOSE.methodName);
        }
      });

  private final PluginService pluginService;

  // Cache parsed results per PreparedStatement to avoid re-parsing on every execute call.
  // WeakHashMap allows GC when the PreparedStatement is no longer referenced.
  private final Map<PreparedStatement, ParsedSqlResult> parsedSqlCache =
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
      final @Nullable Object[] jdbcMethodArgs)
      throws E {

    // On statement close: remove cached parse result. CallableStatement extends
    // PreparedStatement, so both close methods are handled by the same instanceof check.
    if ((JdbcMethod.PREPAREDSTATEMENT_CLOSE.methodName.equals(methodName)
        || JdbcMethod.CALLABLESTATEMENT_CLOSE.methodName.equals(methodName))
        && methodInvokeOn instanceof PreparedStatement) {
      parsedSqlCache.remove(methodInvokeOn);
      return jdbcMethodFunc.call();
    }

    // On prepareStatement / prepareCall: parse SQL, cache result, and return early
    if (methodName.startsWith("Connection.prepare")) {
      if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0
          && jdbcMethodArgs[0] instanceof String) {
        String sql = (String) jdbcMethodArgs[0];
        ParsedSqlResult parsed = parseSql(sql);
        applyToContext(parsed);

        T result = jdbcMethodFunc.call();
        if (result instanceof PreparedStatement) {
          parsedSqlCache.put((PreparedStatement) result, parsed);
        }
        return result;
      }
      // No SQL arg (shouldn't happen in practice) — fall through to jdbcMethodFunc.call()
    } else if ((methodName.startsWith("PreparedStatement.execute")
        || methodName.startsWith("CallableStatement.execute"))
        && methodInvokeOn instanceof PreparedStatement) {
      // Re-apply cached parse result (context was reset between prepare and execute)
      ParsedSqlResult cached = parsedSqlCache.get(methodInvokeOn);
      if (cached != null) {
        applyToContext(cached);
      }
    } else if (methodName.startsWith("Statement.execute")) {
      // Parse SQL from args; executeBatch has no SQL arg and will be skipped,
      // causing downstream plugins to fall back to method-name heuristics (routes to writer)
      if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0
          && jdbcMethodArgs[0] instanceof String) {
        applyToContext(parseSql((String) jdbcMethodArgs[0]));
      }
    }

    return jdbcMethodFunc.call();
  }

  private ParsedSqlResult parseSql(String sql) {
    // Parse routing hint
    RoutingHint routingHint = null;
    Matcher routingMatcher = ROUTING_HINT_PATTERN.matcher(sql);
    if (routingMatcher.find()) {
      routingHint = RoutingHint.fromString(routingMatcher.group(1));
    }

    // Parse and strip annotations
    final Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);
    String cleanSql = EncryptionAnnotationParser.stripAnnotations(sql);
    cleanSql = ROUTING_HINT_PATTERN.matcher(cleanSql).replaceAll("").trim();

    // Parse SQL structure
    JSQLParserAnalyzer.QueryAnalysis analysis = JSQLParserAnalyzer.analyze(cleanSql);

    Set<String> tables = new HashSet<>();
    for (String table : analysis.tables) {
      tables.add(table.replace("`", "").replace("\"", ""));
    }

    // Build parameter mapping
    Map<Integer, String> paramMapping = new HashMap<>();
    paramMapping.putAll(SqlAnalysisService.getColumnParameterMapping(cleanSql));
    paramMapping.putAll(annotations);

    return new ParsedSqlResult(
        analysis.queryType, tables, paramMapping, cleanSql,
        annotations, analysis.forUpdate, routingHint);
  }

  private void applyToContext(ParsedSqlResult parsed) {
    PluginCallContext ctx = pluginService.getCallContext();
    ctx.setAttribute(SqlContextKeys.QUERY_TYPE, parsed.queryType);
    ctx.setAttribute(SqlContextKeys.TABLES, parsed.tables);
    ctx.setAttribute(SqlContextKeys.PARAM_MAPPING, parsed.paramMapping);
    ctx.setAttribute(SqlContextKeys.CLEAN_SQL, parsed.cleanSql);
    ctx.setAttribute(SqlContextKeys.ANNOTATIONS, parsed.annotations);
    ctx.setAttribute(SqlContextKeys.FOR_UPDATE, parsed.forUpdate);
    if (parsed.routingHint != null) {
      ctx.setAttribute(SqlContextKeys.ROUTING_HINT, parsed.routingHint);
    }
  }

  /** Immutable cached parse result for a SQL statement. */
  private static class ParsedSqlResult {
    final QueryType queryType;
    final Set<String> tables;
    final Map<Integer, String> paramMapping;
    final String cleanSql;
    final Map<Integer, String> annotations;
    final boolean forUpdate;
    final @Nullable RoutingHint routingHint;

    ParsedSqlResult(QueryType queryType, Set<String> tables,
        Map<Integer, String> paramMapping, String cleanSql,
        Map<Integer, String> annotations, boolean forUpdate,
        @Nullable RoutingHint routingHint) {
      this.queryType = queryType;
      this.tables = tables;
      this.paramMapping = paramMapping;
      this.cleanSql = cleanSql;
      this.annotations = annotations;
      this.forUpdate = forUpdate;
      this.routingHint = routingHint;
    }
  }
}
