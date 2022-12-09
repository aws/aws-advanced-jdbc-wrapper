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

package software.amazon.jdbc.plugin.staledns;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;

/**
 * After Aurora DB cluster fail over is completed and a cluster has elected a new writer node, the corresponding
 * cluster (writer) endpoint contains stale data and points to an old writer node. That old writer node plays
 * a reader role after fail over and connecting with the cluster endpoint connects to it. In such case a user
 * application expects a writer connection but practically gets connected to a reader. Any DML statements fail
 * on the connection since the reader node allows just read-only statements.
 *
 * <p>Such stale DNS data usually lasts 20-40s, up to a minute. Update time is not predictable and depends on cluster
 * control plane.
 *
 * <p>This plugin tries to recognize such a wrong connection to a reader when a writer connection is expected, and to
 * mitigate it by reconnecting a proper new writer.
 */
public class AuroraStaleDnsPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AuroraStaleDnsPlugin.class.getName());

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList("connect", "initHostProvider", "notifyNodeListChanged", "*")));
  private final PluginService pluginService;
  private final AuroraStaleDnsHelper helper;
  private static final String POSTGRESQL_READONLY_QUERY = "SELECT rt.server_id, CASE\n"
      + "           WHEN 'MASTER_SESSION_ID' = rt.session_id THEN 'false'\n"
      + "           ELSE 'true'\n"
      + "           END AS is_reader\n"
      + "FROM aurora_replica_status() rt,\n"
      + "     aurora_db_instance_identifier() di\n"
      + "WHERE rt.server_id = di;";

  private static final String MYSQL_READONLY_QUERY = "SELECT @@innodb_read_only AS is_reader";
  static final String IS_READER_COLUMN = "is_reader";

  public AuroraStaleDnsPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.helper = new AuroraStaleDnsHelper(this.pluginService);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final String readonly_query = driverProtocol.contains("postgresql")
        ? POSTGRESQL_READONLY_QUERY
        : MYSQL_READONLY_QUERY;

    return this.helper.getVerifiedConnection(hostSpec, props, connectFunc, readonly_query);
  }

  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException {

    if (hostListProviderService.isStaticHostListProvider()) {
      throw new SQLException(Messages.get("AuroraStaleDnsPlugin.requireDynamicProvider"));
    }
    initHostProviderFunc.call();
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

    try {
      this.pluginService.refreshHostList();
    } catch (SQLException ex) {
      if (exceptionClass.isAssignableFrom(ex.getClass())) {
        throw exceptionClass.cast(ex);
      }
      throw new RuntimeException(ex);
    }

    return jdbcMethodFunc.call();
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    this.helper.notifyNodeListChanged(changes);
  }
}
