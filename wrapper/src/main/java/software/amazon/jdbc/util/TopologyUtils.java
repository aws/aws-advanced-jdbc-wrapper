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


package software.amazon.jdbc.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.dialect.TopologyQueryHostSpec;
import software.amazon.jdbc.hostavailability.HostAvailability;

public class TopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(TopologyUtils.class.getName());
  protected static final int DEFAULT_QUERY_TIMEOUT_MS = 1000;

  protected final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected final TopologyDialect dialect;
  protected final HostSpec clusterInstanceTemplate;
  protected final HostSpec initialHostSpec;
  protected final HostSpecBuilder hostSpecBuilder;

  public TopologyUtils(
      TopologyDialect dialect, HostSpec clusterInstanceTemplate, HostSpec initialHostSpec, HostSpecBuilder hostSpecBuilder) {
    this.dialect = dialect;
    this.clusterInstanceTemplate = clusterInstanceTemplate;
    this.initialHostSpec = initialHostSpec;
    this.hostSpecBuilder = hostSpecBuilder;
  }

  public List<HostSpec> queryForTopology(Connection conn) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The topology query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(this.networkTimeoutExecutor, DEFAULT_QUERY_TIMEOUT_MS);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("TopologyUtils.errorGettingNetworkTimeout",
          new Object[] {e.getMessage()}));
    }

    final String writerId = this.dialect.getWriterId(conn);
    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      List<TopologyQueryHostSpec> queryHosts = this.dialect.processQueryResults(resultSet, writerId);
      return this.processQueryResults(queryHosts);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  protected @Nullable List<HostSpec> processQueryResults(@Nullable List<TopologyQueryHostSpec> queryHosts) {
    if (queryHosts == null) {
      return null;
    }

    List<HostSpec> hosts = new ArrayList<>();
    List<HostSpec> writers = new ArrayList<>();
    for (TopologyQueryHostSpec queryHost : queryHosts) {
      if (queryHost.isWriter()) {
        writers.add(this.toHostspec(queryHost));
      } else {
        hosts.add(this.toHostspec(queryHost));
      }
    }

    int writerCount = writers.size();
    if (writerCount == 0) {
      LOGGER.warning(() -> Messages.get("ClusterTopologyMonitorImpl.invalidTopology"));
      return null;
    } else if (writerCount == 1) {
      hosts.add(writers.get(0));
    } else {
      // Assume the latest updated writer instance is the current writer. Other potential writers will be ignored.
      List<HostSpec> sortedWriters = writers.stream()
          .sorted(Comparator.comparing(HostSpec::getLastUpdateTime, Comparator.nullsLast(Comparator.reverseOrder())))
          .collect(Collectors.toList());
      hosts.add(sortedWriters.get(0));
    }

    return hosts;
  }

  protected HostSpec toHostspec(TopologyQueryHostSpec queryHost) {
    final String instanceId = queryHost.getInstanceId() == null ? "?" : queryHost.getInstanceId();
    final String endpoint = this.clusterInstanceTemplate.getHost().replace("?", instanceId);
    final int port = this.clusterInstanceTemplate.isPortSpecified()
        ? this.clusterInstanceTemplate.getPort()
        : this.initialHostSpec.getPort();

    final HostSpec hostSpec = this.hostSpecBuilder
        .host(endpoint)
        .port(port)
        .role(queryHost.isWriter() ? HostRole.WRITER : HostRole.READER)
        .availability(HostAvailability.AVAILABLE)
        .weight(queryHost.getWeight())
        .lastUpdateTime(queryHost.getLastUpdateTime())
        .build();
    hostSpec.addAlias(instanceId);
    hostSpec.setHostId(instanceId);
    return hostSpec;
  }

  public HostRole getHostRole(Connection conn) throws SQLException {
    try (final Statement stmt = conn.createStatement();
         final ResultSet rs = stmt.executeQuery(this.isReaderQuery)) {
      if (rs.next()) {
        boolean isReader = rs.getBoolean(1);
        return isReader ? HostRole.READER : HostRole.WRITER;
      }
    } catch (SQLException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.errorGettingHostRole"), e);
    }

    throw new SQLException(Messages.get("RdsHostListProvider.errorGettingHostRole"));
  }
}
