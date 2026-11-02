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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.sf.jsqlparser.statement.Statement;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.EncryptionAnnotationParser;
import software.amazon.jdbc.parser.JSQLParserAnalyzer;
import software.amazon.jdbc.parser.ParameterColumnMapper;
import software.amazon.jdbc.parser.ParameterMappingProblem;
import software.amazon.jdbc.parser.PlaceholderScanner;
import software.amazon.jdbc.parser.QueryType;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.targetdriverdialect.MariadbTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.MysqlConnectorJTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;

/**
 * Plugin that parses SQL statements and populates the {@link PluginCallContext}
 * with analysis results for downstream plugins to consume.
 *
 * <p>This plugin must be placed before any plugin that needs SQL analysis
 * (e.g., kmsEncryption, autoReadWriteSplitting).
 */
public class SqlParserConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(SqlParserConnectionPlugin.class.getName());

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

    // Find the real placeholders, literals and comments once, with the target driver's rules
    final PlaceholderScanner.Result scan = PlaceholderScanner.scan(sql, resolveScannerMode());

    // Parse and strip annotations
    final Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql, scan);
    String cleanSql = EncryptionAnnotationParser.stripAnnotations(sql, scan);
    cleanSql = ROUTING_HINT_PATTERN.matcher(cleanSql).replaceAll("").trim();

    // Parse SQL structure once; only comments were removed, so the placeholder ordinals of sql and
    // cleanSql are identical
    final @Nullable Statement statement = JSQLParserAnalyzer.parse(cleanSql);
    JSQLParserAnalyzer.QueryAnalysis analysis = JSQLParserAnalyzer.analyze(statement, cleanSql);

    Set<String> tables = new HashSet<>();
    for (String table : analysis.tables) {
      tables.add(table.replace("`", "").replace("\"", ""));
    }

    // Build parameter mapping; annotations take precedence
    final ParameterColumnMapper.ParameterMapping mapping = ParameterColumnMapper.map(statement, scan);
    Map<Integer, String> paramMapping = new HashMap<>(mapping.getColumns());
    for (Map.Entry<Integer, String> annotation : annotations.entrySet()) {
      final String mapped = paramMapping.get(annotation.getKey());
      if (mapped != null && conflicts(annotation.getValue(), mapped)) {
        LOGGER.warning(() -> Messages.get(
            "SqlParserConnectionPlugin.annotationConflictsWithMapping",
            new Object[] {String.valueOf(annotation.getKey()), annotation.getValue(), mapped}));
      }
      paramMapping.put(annotation.getKey(), annotation.getValue());
    }

    return new ParsedSqlResult(
        analysis.queryType, tables, paramMapping, cleanSql,
        annotations, analysis.forUpdate, routingHint, mapping.getProblems());
  }

  private PlaceholderScanner.Mode resolveScannerMode() {
    final TargetDriverDialect dialect = pluginService.getTargetDriverDialect();
    if (dialect instanceof PgTargetDriverDialect) {
      return PlaceholderScanner.Mode.POSTGRESQL;
    }
    if (dialect instanceof MysqlConnectorJTargetDriverDialect
        || dialect instanceof MariadbTargetDriverDialect) {
      return PlaceholderScanner.Mode.MYSQL;
    }
    return PlaceholderScanner.Mode.GENERIC;
  }

  /**
   * An annotation conflicts with the automatic mapping when the column differs, or when the last
   * segment of the table differs (so {@code public.users.ssn} and {@code users.ssn} agree).
   */
  private static boolean conflicts(final String annotation, final String mapped) {
    final int annotationDot = annotation.lastIndexOf('.');
    final int mappedDot = mapped.lastIndexOf('.');
    final String annotationColumn = annotation.substring(annotationDot + 1);
    final String mappedColumn = mapped.substring(mappedDot + 1);
    if (!annotationColumn.equalsIgnoreCase(mappedColumn)) {
      return true;
    }
    if (annotationDot < 0 || mappedDot < 0) {
      return false;
    }
    final String annotationTable = lastSegment(annotation.substring(0, annotationDot));
    final String mappedTable = lastSegment(mapped.substring(0, mappedDot));
    return !annotationTable.equalsIgnoreCase(mappedTable);
  }

  private static String lastSegment(final String qualifiedName) {
    return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
  }

  private void applyToContext(ParsedSqlResult parsed) {
    PluginCallContext ctx = pluginService.getCallContext();
    ctx.setAttribute(SqlContextKeys.QUERY_TYPE, parsed.queryType);
    ctx.setAttribute(SqlContextKeys.TABLES, parsed.tables);
    ctx.setAttribute(SqlContextKeys.PARAM_MAPPING, parsed.paramMapping);
    ctx.setAttribute(SqlContextKeys.PARAM_MAPPING_PROBLEMS, parsed.problems);
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
    final List<ParameterMappingProblem> problems;

    ParsedSqlResult(QueryType queryType, Set<String> tables,
        Map<Integer, String> paramMapping, String cleanSql,
        Map<Integer, String> annotations, boolean forUpdate,
        @Nullable RoutingHint routingHint, List<ParameterMappingProblem> problems) {
      this.queryType = queryType;
      this.tables = tables;
      this.paramMapping = paramMapping;
      this.cleanSql = cleanSql;
      this.annotations = annotations;
      this.forUpdate = forUpdate;
      this.routingHint = routingHint;
      this.problems = Collections.unmodifiableList(new ArrayList<>(problems));
    }
  }
}
