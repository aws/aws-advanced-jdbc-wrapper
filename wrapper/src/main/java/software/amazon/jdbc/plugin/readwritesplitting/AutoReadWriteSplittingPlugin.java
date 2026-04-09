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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * Read/write splitting plugin that automatically routes queries based on SQL analysis.
 * SELECT queries are routed to a reader instance; DML/DDL queries are routed to the writer.
 *
 * <p>Routing can be overridden using SQL comment hints:
 * <ul>
 *   <li>{@code /*@reader* /} — force query to reader</li>
 *   <li>{@code /*@writer* /} — force query to writer (e.g., SELECT ... FOR UPDATE)</li>
 * </ul>
 *
 * <p>Requires the {@code sqlParser} plugin to be loaded before this plugin.
 * Queries inside a transaction are always routed to the writer.
 */
public class AutoReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(AutoReadWriteSplittingPlugin.class.getName());

  private static final Set<String> autoSubscribedMethods;

  static {
    Set<String> methods = new HashSet<>();
    // We'll add parent methods via getSubscribedMethods() at instance level
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName);
    methods.add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTE.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
    methods.add(JdbcMethod.STATEMENT_EXECUTEBATCH.methodName);
    autoSubscribedMethods = Collections.unmodifiableSet(methods);
  }

  private final Set<String> allSubscribedMethods;

  public AutoReadWriteSplittingPlugin(
      final PluginService pluginService, final @NonNull Properties properties) {
    super(pluginService, properties);
    Set<String> combined = new HashSet<>(super.getSubscribedMethods());
    combined.addAll(autoSubscribedMethods);
    this.allSubscribedMethods = Collections.unmodifiableSet(combined);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return allSubscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] args)
      throws E {

    // For execute methods, determine routing before delegating
    if (isExecuteMethod(methodName)) {
      try {
        boolean readOnly = shouldRouteToReader(methodName, args);
        switchConnectionIfRequired(readOnly);
      } catch (final SQLException e) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
      }
    }

    return super.execute(resultClass, exceptionClass, methodInvokeOn,
        methodName, jdbcMethodFunc, args);
  }

  private boolean shouldRouteToReader(String methodName, Object[] args) {
    // Never route to reader inside a transaction
    if (pluginService.isInTransaction()) {
      return false;
    }

    PluginCallContext ctx = pluginService.getCallContext();

    // Check for explicit routing hint (highest priority)
    if (ctx != null) {
      String routingHint = ctx.getAttribute(SqlContextKeys.ROUTING_HINT, String.class);
      if ("reader".equals(routingHint)) {
        return true;
      }
      if ("writer".equals(routingHint)) {
        return false;
      }

      // Use parsed query type from context
      String queryType = ctx.getAttribute(SqlContextKeys.QUERY_TYPE, String.class);
      if (queryType != null) {
        return "SELECT".equals(queryType);
      }
    }

    // Fallback: executeQuery → reader, executeUpdate → writer
    if (methodName.endsWith("executeQuery")) {
      return true;
    }
    if (methodName.endsWith("executeUpdate") || methodName.endsWith("executeBatch")) {
      return false;
    }

    // Default to writer for safety
    return false;
  }

  private boolean isExecuteMethod(String methodName) {
    return methodName.startsWith("PreparedStatement.execute")
        || methodName.startsWith("Statement.execute");
  }
}
