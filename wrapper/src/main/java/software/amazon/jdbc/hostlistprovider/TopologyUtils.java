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
import java.sql.Timestamp;
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
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.SynchronousExecutor;

/**
 * An abstract class defining utility methods that can be used to retrieve and process a variety of database topology
 * information. This class can be overridden to define logic specific to various database engine deployments
 * (e.g. Aurora, Multi-AZ, Global Aurora etc.).
 */
public abstract class TopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(TopologyUtils.class.getName());
  protected static final int DEFAULT_QUERY_TIMEOUT_MS = 1000;

  protected final Executor networkTimeoutExecutor = new SynchronousExecutor();
  protected final TopologyDialect dialect;
  protected final HostSpecBuilder hostSpecBuilder;

  public TopologyUtils(
      TopologyDialect dialect,
      HostSpecBuilder hostSpecBuilder) {
    this.dialect = dialect;
    this.hostSpecBuilder = hostSpecBuilder;
  }

  /**
   * Query the database for information for each instance in the database topology.
   *
   * @param conn             the connection to use to query the database.
   * @param initialHostSpec  the {@link HostSpec} that was used to initially connect.
   * @param instanceTemplate the template {@link HostSpec} to use when constructing new {@link HostSpec} objects from
   *                         the data returned by the topology query.
   * @return a list of {@link HostSpec} objects representing the results of the topology query.
   * @throws SQLException if an error occurs when executing the topology or processing the results.
   */
  public @Nullable List<HostSpec> queryForTopology(Connection conn, HostSpec initialHostSpec, HostSpec instanceTemplate)
      throws SQLException {
    final Pair<Integer, Boolean> networkTimeoutPair = this.setNetworkTimeout(conn);
    int originalNetworkTimeout = networkTimeoutPair.getValue1();
    boolean timeoutChanged = networkTimeoutPair.getValue2();
    try (final Statement stmt = conn.createStatement();
         final ResultSet rs = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      if (rs.getMetaData().getColumnCount() == 0) {
        // We expect at least 4 columns. Note that the server may return 0 columns if failover has occurred.
        LOGGER.finest(Messages.get("TopologyUtils.unexpectedTopologyQueryColumnCount"));
        return null;
      }

      return this.verifyWriter(this.getHosts(conn, rs, initialHostSpec, instanceTemplate));
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (timeoutChanged && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, originalNetworkTimeout);
      }
    }
  }

  protected Pair<Integer, Boolean> setNetworkTimeout(Connection conn) {
    int networkTimeout = -1;
    boolean timeoutChanged = false;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The topology query is not monitored by the EFM plugin, so it needs a socket timeout.
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(this.networkTimeoutExecutor, DEFAULT_QUERY_TIMEOUT_MS);
        timeoutChanged = true;
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("TopologyUtils.errorGettingNetworkTimeout", new Object[] {e.getMessage()}));
    }
    return Pair.create(networkTimeout, timeoutChanged);
  }

  protected abstract @Nullable List<HostSpec> getHosts(
      Connection conn, ResultSet rs, HostSpec initialHostSpec, HostSpec instanceTemplate) throws SQLException;

  protected @Nullable List<HostSpec> verifyWriter(@Nullable List<HostSpec> allHosts) {
    if (allHosts == null) {
      return null;
    }

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

  /**
   * Creates a {@link HostSpec} from the given topology information.
   *
   * @param instanceId       the database instance identifier, e.g. "mydb-instance-1".
   * @param isWriter         true if this is a writer instance, false for reader.
   * @param weight           the instance weight for load balancing.
   * @param lastUpdateTime   the timestamp of the last update to this instance's information.
   * @param initialHostSpec  the original host specification used for connecting.
   * @param instanceTemplate the template used to construct the new {@link HostSpec}.
   * @return a {@link HostSpec} representing the given information.
   */
  public HostSpec createHost(
      String instanceId,
      String instanceName,
      final boolean isWriter,
      final long weight,
      final Timestamp lastUpdateTime,
      final HostSpec initialHostSpec,
      final HostSpec instanceTemplate) {
    instanceName = instanceName == null ? "?" : instanceName;
    final String endpoint = instanceTemplate.getHost().replace("?", instanceName);
    final int port = instanceTemplate.isPortSpecified()
        ? instanceTemplate.getPort()
        : (initialHostSpec == null ? HostSpec.NO_PORT : initialHostSpec.getPort());

    final HostSpec hostSpec = this.hostSpecBuilder
        .hostId(instanceId)
        .host(endpoint)
        .port(port)
        .role(isWriter ? HostRole.WRITER : HostRole.READER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .lastUpdateTime(lastUpdateTime)
        .build();
    hostSpec.addAlias(instanceName);
    hostSpec.setHostId(instanceName);
    return hostSpec;
  }

  /**
   * Identifies instances across different database types using instanceId and instanceName values.
   *
   * <p>Database types handle these identifiers differently:
   * - Aurora: Uses the instance name as both instanceId and instanceName
   * Example: "test-instance-1" for both values
   * - RDS Cluster: Uses distinct values for instanceId and instanceName
   * Example:
   * instanceId: "db-WQFQKBTL2LQUPIEFIFBGENS4ZQ"
   * instanceName: "test-multiaz-instance-1"
   */
  public @Nullable Pair<String /* instanceId */, String /* instanceName */> getInstanceId(final Connection connection) {
    try {
      try (final Statement stmt = connection.createStatement();
           final ResultSet rs = stmt.executeQuery(this.dialect.getInstanceIdQuery())) {
        if (rs.next()) {
          return Pair.create(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException ex) {
      return null;
    }

    return null;
  }

  /**
   * Evaluate whether the given connection is to a writer instance.
   *
   * @param connection the connection to evaluate.
   * @return true if the connection is to a writer instance, false otherwise.
   * @throws SQLException if an exception occurs when querying the database or processing the database response.
   */
  public abstract boolean isWriterInstance(Connection connection) throws SQLException;

  /**
   * Evaluate the database role of the given connection, either {@link HostRole#WRITER} or {@link HostRole#READER}.
   *
   * @param conn the connection to evaluate.
   * @return the database role of the given connection.
   * @throws SQLException if an exception occurs when querying the database or processing the database response.
   */
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
