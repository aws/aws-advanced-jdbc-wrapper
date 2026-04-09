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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
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

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName);
          add(JdbcMethod.CONNECTION_PREPARECALL.methodName);
          add(JdbcMethod.STATEMENT_EXECUTE.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
        }
      });

  private final PluginService pluginService;
  private final SqlAnalysisService sqlAnalysisService;

  public SqlParserConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.sqlAnalysisService = new SqlAnalysisService();
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
      final software.amazon.jdbc.JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    if (jdbcMethodArgs != null && jdbcMethodArgs.length > 0
        && jdbcMethodArgs[0] instanceof String) {
      String sql = (String) jdbcMethodArgs[0];
      populateContext(sql);
    }

    return jdbcMethodFunc.call();
  }

  private void populateContext(String sql) {
    PluginCallContext ctx = pluginService.getCallContext();

    // Parse and strip annotations
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);
    String cleanSql = EncryptionAnnotationParser.stripAnnotations(sql);

    ctx.setAttribute(SqlContextKeys.ANNOTATIONS, annotations);
    ctx.setAttribute(SqlContextKeys.CLEAN_SQL, cleanSql);

    // Parse SQL structure
    JSQLParserAnalyzer.QueryAnalysis analysis = JSQLParserAnalyzer.analyze(cleanSql);

    Set<String> tables = new HashSet<>();
    for (String table : analysis.tables) {
      tables.add(table.replace("`", ""));
    }

    ctx.setAttribute(SqlContextKeys.QUERY_TYPE, analysis.queryType);
    ctx.setAttribute(SqlContextKeys.TABLES, tables);

    // Build parameter mapping
    Map<Integer, String> paramMapping = new ConcurrentHashMap<>();
    paramMapping.putAll(sqlAnalysisService.getColumnParameterMapping(cleanSql));
    paramMapping.putAll(annotations);
    ctx.setAttribute(SqlContextKeys.PARAM_MAPPING, paramMapping);
  }
}
