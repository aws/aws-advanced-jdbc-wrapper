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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;

public class RdsMultiAzDbClusterListProvider extends RdsHostListProvider {
  private final String fetchWriterNodeQuery;
  private final String fetchWriterNodeQueryHeader;
  static final Logger LOGGER = Logger.getLogger(RdsMultiAzDbClusterListProvider.class.getName());

  public RdsMultiAzDbClusterListProvider(
      final Properties properties,
      final String originalUrl,
      final HostListProviderService hostListProviderService,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery,
      final String fetchWriterNodeQuery,
      final String fetchWriterNodeQueryHeader
  ) {
    super(properties,
        originalUrl,
        hostListProviderService,
        topologyQuery,
        nodeIdQuery,
        isReaderQuery);
    this.fetchWriterNodeQuery = fetchWriterNodeQuery;
    this.fetchWriterNodeQueryHeader = fetchWriterNodeQueryHeader;
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return a list of {@link HostSpec} objects representing the topology
   * @throws SQLException if errors occurred while retrieving the topology.
   */
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The topology query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(networkTimeoutExecutor, defaultTopologyQueryTimeoutMs);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("RdsHostListProvider.errorGettingNetworkTimeout",
          new Object[] {e.getMessage()}));
    }

    try {
      final Statement stmt = conn.createStatement();
      String writerNodeId = processWriterNodeId(stmt.executeQuery(this.fetchWriterNodeQuery));
      if (writerNodeId.isEmpty()) {
        final ResultSet nodeIdResultSet = stmt.executeQuery(this.nodeIdQuery);
        while (nodeIdResultSet.next()) {
          writerNodeId = nodeIdResultSet.getString(1);
        }
      }
      final ResultSet topologyResultSet = stmt.executeQuery(this.topologyQuery);
      return processTopologyQueryResults(topologyResultSet, writerNodeId);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("RdsHostListProvider.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  /**
   * Get writer node ID.
   *
   * @param fetchWriterNodeResultSet A ResultSet of writer node query
   * @return String The ID of a writer node
   * @throws SQLException if errors occurred while retrieving the topology
   */
  private String processWriterNodeId(final ResultSet fetchWriterNodeResultSet) throws SQLException {
    String writerNodeId = null;
    while (fetchWriterNodeResultSet.next()) {
      writerNodeId = fetchWriterNodeResultSet.getString(fetchWriterNodeQueryHeader);
    }
    return writerNodeId;
  }

  /**
   * Form a list of hosts from the results of the topology query.
   *
   * @param topologyResultSet The results of the topology query
   * @param writerNodeId The writer node ID
   * @return a list of {@link HostSpec} objects representing
   *     the topology that was returned by the
   *     topology query. The list will be empty if the topology query returned an invalid topology
   *     (no writer instance).
   */
  private List<HostSpec> processTopologyQueryResults(
      final ResultSet topologyResultSet,
      final String writerNodeId) throws SQLException {

    final HashMap<String, HostSpec> hostMap = new HashMap<>();

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    while (topologyResultSet.next()) {
      final HostSpec host = createHost(topologyResultSet, writerNodeId);
      hostMap.put(host.getHost(), host);
    }

    final List<HostSpec> hosts = new ArrayList<>();
    final List<HostSpec> writers = new ArrayList<>();

    for (final HostSpec host : hostMap.values()) {
      if (host.getRole() != HostRole.WRITER) {
        hosts.add(host);
      } else {
        writers.add(host);
      }
    }

    int writerCount = writers.size();

    if (writerCount == 0) {
      LOGGER.severe(() -> Messages.get("RdsHostListProvider.invalidTopology"));
      hosts.clear();
    } else if (writerCount == 1) {
      hosts.add(writers.get(0));
    } else {
      // Take the latest updated writer node as the current writer. All others will be ignored.
      List<HostSpec> sortedWriters = writers.stream()
          .sorted(Comparator.comparing(HostSpec::getLastUpdateTime).reversed())
          .collect(Collectors.toList());
      hosts.add(sortedWriters.get(0));
    }

    return hosts;
  }

  /**
   * Creates an instance of HostSpec which captures details about a connectable host.
   *
   * @param resultSet the result set from querying the topology
   * @return a {@link HostSpec} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostSpec createHost(final ResultSet resultSet, final String writerNodeId) throws SQLException {
    String hostName = resultSet.getString("endpoint");
    String hostId = resultSet.getString("id");
    int port = resultSet.getInt("port");
    final boolean isWriter = hostId.equals(writerNodeId);

    final HostSpec hostSpec = this.hostListProviderService.getHostSpecBuilder()
        .host(hostName)
        .port(port)
        .role(isWriter ? HostRole.WRITER : HostRole.READER)
        .availability(HostAvailability.AVAILABLE)
        .weight(0)
        .lastUpdateTime(Timestamp.from(Instant.now()))
        .build();
    hostSpec.addAlias(hostName);
    hostSpec.setHostId(hostId);
    return hostSpec;
  }
}
