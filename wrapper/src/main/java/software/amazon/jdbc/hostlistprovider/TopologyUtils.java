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


package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;

public class TopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(TopologyUtils.class.getName());
  protected static final int DEFAULT_QUERY_TIMEOUT_MS = 1000;

  protected final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected final TopologyDialect dialect;
  protected final HostSpec initialHostSpec;
  protected final HostSpecBuilder hostSpecBuilder;

  public TopologyUtils(
      TopologyDialect dialect,
      HostSpec initialHostSpec,
      HostSpecBuilder hostSpecBuilder) {
    this.dialect = dialect;
    this.initialHostSpec = initialHostSpec;
    this.hostSpecBuilder = hostSpecBuilder;
  }

  public @Nullable List<HostSpec> queryForTopology(Connection conn, HostSpec hostTemplate) throws SQLException {
    int networkTimeout = setNetworkTimeout(conn);
    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      List<HostSpec> hosts = new ArrayList<>();
      List<TopologyQueryHostSpec> queryHosts = this.dialect.processTopologyResults(conn, resultSet);
      if (Utils.isNullOrEmpty(queryHosts)) {
        return null;
      }

      for (TopologyQueryHostSpec queryHost : queryHosts) {
        hosts.add(this.toHostspec(queryHost, hostTemplate));
      }

      return this.verifyWriter(hosts);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  public @Nullable List<HostSpec> queryForTopology(Connection conn, Map<String, HostSpec> hostTemplateByRegion) throws SQLException {
    int networkTimeout = setNetworkTimeout(conn);
    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      List<HostSpec> hosts = new ArrayList<>();
      List<TopologyQueryHostSpec> queryHosts = this.dialect.processTopologyResults(conn, resultSet);
      if (Utils.isNullOrEmpty(queryHosts)) {
        return null;
      }

      for (TopologyQueryHostSpec queryHost : queryHosts) {
        String region = queryHost.getRegion();
        if (region == null) {
          throw new SQLException("");
        }

        HostSpec template = hostTemplateByRegion.get(region);
        if (template == null) {
          throw new SQLException("");
        }

        hosts.add(this.toHostspec(queryHost, template));
      }

      return this.verifyWriter(hosts);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  private int setNetworkTimeout(Connection conn) {
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
    return networkTimeout;
  }

  protected @Nullable List<HostSpec> verifyWriter(List<HostSpec> allHosts) {
    List<HostSpec> hosts = new ArrayList<>();
    List<HostSpec> writers = new ArrayList<>();
    for (HostSpec host : allHosts) {
      if (HostRole.WRITER == host.getRole()) {
        writers.add(host);
      } else {
        hosts.add(host);
      }
    }

    int writerCount = writers.size();
    if (writerCount == 0) {
      LOGGER.warning(() -> Messages.get("TopologyUtils.invalidTopology"));
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

  protected HostSpec toHostspec(TopologyQueryHostSpec queryHost, HostSpec hostTemplate) {
    final String instanceId = queryHost.getInstanceId() == null ? "?" : queryHost.getInstanceId();
    final String endpoint = hostTemplate.getHost().replace("?", instanceId);
    final int port = hostTemplate.isPortSpecified()
        ? hostTemplate.getPort()
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

  /**
   * Identifies instances across different database types using instanceId and instanceName values.
   *
   * <p>Database types handle these identifiers differently:
   * - Aurora: Uses the instance name as both instanceId and instanceName
   *   Example: "test-instance-1" for both values
   * - RDS Cluster: Uses distinct values for instanceId and instanceName
   *   Example:
   *      instanceId: "db-WQFQKBTL2LQUPIEFIFBGENS4ZQ"
   *      instanceName: "test-multiaz-instance-1"
   */
  public @Nullable Pair<String /* instanceId */, String /* instanceName */> getInstanceId(final Connection connection) {
    try {
      try (final Statement stmt = connection.createStatement();
           final ResultSet resultSet = stmt.executeQuery(this.dialect.getInstanceIdQuery())) {
        if (resultSet.next()) {
          return Pair.create(resultSet.getString(1), resultSet.getString(2));
        }
      }
    } catch (SQLException ex) {
      return null;
    }

    return null;
  }

  public boolean isWriterInstance(Connection connection) throws SQLException {
    return this.dialect.isWriterInstance(connection);
  }

  public HostRole getHostRole(Connection conn) throws SQLException {
    try (final Statement stmt = conn.createStatement();
         final ResultSet rs = stmt.executeQuery(this.dialect.getIsReaderQuery())) {
      if (rs.next()) {
        boolean isReader = rs.getBoolean(1);
        return isReader ? HostRole.READER : HostRole.WRITER;
      }
    } catch (SQLException e) {
      throw new SQLException(Messages.get("TopologyUtils.errorGettingHostRole"), e);
    }

    throw new SQLException(Messages.get("TopologyUtils.errorGettingHostRole"));
  }
}
