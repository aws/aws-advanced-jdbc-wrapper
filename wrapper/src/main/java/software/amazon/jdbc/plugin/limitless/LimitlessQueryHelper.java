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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.AuroraLimitlessDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SynchronousExecutor;

public class LimitlessQueryHelper {

  protected final @NonNull PluginService pluginService;
  protected static final Logger LOGGER = Logger.getLogger(LimitlessQueryHelper.class.getName());

  protected static final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected static final int DEFAULT_QUERY_TIMEOUT_MS = 5000;

  public LimitlessQueryHelper(final @NonNull PluginService pluginService) {
    this.pluginService = pluginService;
  }

  public List<HostSpec> queryForLimitlessRouters(final Connection conn, final int hostPortToMap) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(networkTimeoutExecutor, DEFAULT_QUERY_TIMEOUT_MS);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("LimitlessRouterMonitor.getNetworkTimeoutError",
          new Object[] {e.getMessage()}));
    }

    final Dialect dialect = this.pluginService.getDialect();
    if (!(dialect instanceof AuroraLimitlessDialect)) {
      throw new UnsupportedOperationException(Messages.get("LimitlessQueryHelper.unsupportedDialectOrDatabase",
          new Object[] {dialect}));
    }

    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(
             ((AuroraLimitlessDialect) this.pluginService.getDialect()).getLimitlessRouterEndpointQuery())) {
      return mapResultSetToHostSpecList(resultSet, hostPortToMap);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("LimitlessRouterMonitor.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  protected List<HostSpec> mapResultSetToHostSpecList(
      final ResultSet resultSet,
      final int hostPortToMap) throws SQLException {

    List<HostSpec> hosts = new ArrayList<>();
    while (resultSet.next()) {
      final HostSpec host = createHost(resultSet, hostPortToMap);
      hosts.add(host);
    }

    return hosts;
  }

  protected HostSpec createHost(final ResultSet resultSet, final int hostPortToMap) throws SQLException {
    final String hostName = resultSet.getString(1);
    final float cpu = resultSet.getFloat(2);

    long weight = Math.round(10 - cpu * 10);

    if (weight < 1 || weight > 10) {
      weight = 1; // default to 1
      LOGGER.warning(() -> Messages.get(
          "LimitlessRouterMonitor.invalidRouterLoad",
          new Object[] {hostName, cpu}));
    }

    return this.pluginService.getHostSpecBuilder()
        .host(hostName)
        .port(hostPortToMap)
        .role(HostRole.WRITER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .hostId(hostName)
        .build();
  }
}
