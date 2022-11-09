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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostAvailability;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlMethodAnalyzer;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain, and will
 * invoke the JDBC method passed down the chain.
 */
public final class DefaultConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(DefaultConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("*")));
  private static final SqlMethodAnalyzer sqlMethodAnalyzer = new SqlMethodAnalyzer();

  private final ConnectionProvider connectionProvider;
  private final PluginService pluginService;
  private final PluginManagerService pluginManagerService;

  public DefaultConnectionPlugin(
      final PluginService pluginService,
      final ConnectionProvider connectionProvider,
      final PluginManagerService pluginManagerService) {
    if (pluginService == null) {
      throw new IllegalArgumentException("pluginService");
    }
    if (pluginManagerService == null) {
      throw new IllegalArgumentException("pluginManagerService");
    }
    if (connectionProvider == null) {
      throw new IllegalArgumentException("connectionProvider");
    }

    this.pluginService = pluginService;
    this.pluginManagerService = pluginManagerService;
    this.connectionProvider = connectionProvider;
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

    LOGGER.finest(
        () -> Messages.get(
            "DefaultConnectionPlugin.executingMethod",
            new Object[] {methodName}));
    final T result = jdbcMethodFunc.call();

    Connection currentConn = this.pluginService.getCurrentConnection();
    if (sqlMethodAnalyzer.doesOpenTransaction(currentConn, methodName, jdbcMethodArgs)) {
      this.pluginManagerService.setInTransaction(true);
    } else if (sqlMethodAnalyzer.doesCloseTransaction(methodName, jdbcMethodArgs)) {
      this.pluginManagerService.setInTransaction(false);
    }

    if (sqlMethodAnalyzer.isStatementSettingAutoCommit(methodName, jdbcMethodArgs)) {
      final Boolean autocommit = sqlMethodAnalyzer.getAutoCommitValueFromSqlStatement(jdbcMethodArgs);
      if (autocommit != null) {
        try {
          currentConn.setAutoCommit(autocommit);
        } catch (final SQLException e) {
          // do nothing
        }
      }
    }

    return result;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final Connection conn = this.connectionProvider.connect(driverProtocol, hostSpec, props);

    // It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be
    // omitted.

    this.pluginService.setAvailability(hostSpec.asAliases(), HostAvailability.AVAILABLE);
    return conn;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {

    // do nothing
    // It's guaranteed that this plugin is always the last in plugin chain so initHostProviderFunc
    // can be omitted.
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {
    // do nothing
  }

  List<String> parseMultiStatementQueries(String query) {
    if (query == null || query.isEmpty()) {
      return new ArrayList<>();
    }

    query = query.replaceAll("\\s+", " ");

    // Check to see if string only has blank spaces.
    if (query.trim().isEmpty()) {
      return new ArrayList<>();
    }

    return Arrays.stream(query.split(";")).collect(Collectors.toList());
  }
}
