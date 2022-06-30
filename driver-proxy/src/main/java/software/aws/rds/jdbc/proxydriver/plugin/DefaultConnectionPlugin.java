/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin;

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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.ConnectionProvider;
import software.aws.rds.jdbc.proxydriver.HostListProviderService;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;
import software.aws.rds.jdbc.proxydriver.NodeChangeOptions;
import software.aws.rds.jdbc.proxydriver.OldConnectionSuggestedAction;
import software.aws.rds.jdbc.proxydriver.PluginManagerService;
import software.aws.rds.jdbc.proxydriver.PluginService;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain, and will invoke the JDBC method
 * passed down the chain.
 */
public final class DefaultConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(DefaultConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("*")));

  private final ConnectionProvider connectionProvider;
  private final PluginService pluginService;
  private final PluginManagerService pluginManagerService;

  public DefaultConnectionPlugin(
      PluginService pluginService,
      ConnectionProvider connectionProvider,
      PluginManagerService pluginManagerService) {
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
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {

    LOGGER.log(Level.FINEST, String.format("Executing method %s", methodName));
    final T result = jdbcMethodFunc.call();

    if (!(methodName.contains("execute") && jdbcMethodArgs != null && jdbcMethodArgs.length >= 1)) {
      return result;
    }

    final String query = String.valueOf(jdbcMethodArgs[0]);
    final String statement = parseMultiStatementQueries(query).get(0);

    if (doesOpenTransaction(statement)) {
      this.pluginManagerService.setInTransaction(true);
    }

    if (doesCloseTransaction(statement)) {
      this.pluginManagerService.setInTransaction(false);
    }

    return result;
  }

  @Override
  public Connection connect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    Connection conn = this.connectionProvider.connect(driverProtocol, hostSpec, props);

    // It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be
    // omitted.

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
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    // do nothing
  }

  public boolean doesOpenTransaction(String statement) {
    statement = statement.toUpperCase();
    statement = statement.replaceAll("START(?:\\s*/\\*(.*?)\\*/\\s*)TRANSACTION", "START TRANSACTION");
    return statement.startsWith("BEGIN") || statement.startsWith("START TRANSACTION");
  }

  public boolean doesCloseTransaction(String statement) {
    statement = statement.toUpperCase();
    return statement.startsWith("COMMIT")
        || statement.startsWith("ROLLBACK")
        || statement.startsWith("END")
        || statement.startsWith("ABORT");
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
