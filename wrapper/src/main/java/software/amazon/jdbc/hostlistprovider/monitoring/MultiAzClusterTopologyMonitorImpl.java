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

package software.amazon.jdbc.hostlistprovider.monitoring;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.StringUtils;

public class MultiAzClusterTopologyMonitorImpl extends ClusterTopologyMonitorImpl {

  private static final Logger LOGGER = Logger.getLogger(MultiAzClusterTopologyMonitorImpl.class.getName());

  protected final String fetchWriterNodeQuery;
  protected final String fetchWriterNodeColumnName;

  public MultiAzClusterTopologyMonitorImpl(
      final String clusterId,
      final CacheMap<String, List<HostSpec>> topologyMap,
      final HostSpec initialHostSpec,
      final Properties properties,
      final PluginService pluginService,
      final HostListProviderService hostListProviderService,
      final HostSpec clusterInstanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano,
      final long topologyCacheExpirationNano,
      final String topologyQuery,
      final String writerTopologyQuery,
      final String nodeIdQuery,
      final String fetchWriterNodeQuery,
      final String fetchWriterNodeColumnName) {
    super(clusterId, topologyMap, initialHostSpec, properties, pluginService, hostListProviderService,
        clusterInstanceTemplate, refreshRateNano, highRefreshRateNano, topologyCacheExpirationNano,
        topologyQuery, writerTopologyQuery, nodeIdQuery);
    this.fetchWriterNodeQuery = fetchWriterNodeQuery;
    this.fetchWriterNodeColumnName = fetchWriterNodeColumnName;
  }

  // Returns a writer node ID if connected to a writer node. Returns null otherwise.
  @Override
  protected String getWriterNodeId(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.fetchWriterNodeQuery)) {
        if (resultSet.next()) {
          String nodeId = resultSet.getString(this.fetchWriterNodeColumnName);
          if (!StringUtils.isNullOrEmpty(nodeId)) {
            // Replica status exists and shows a writer node ID.
            // That means that this node (this connection) is a reader
            return null;
          }
        }
      }
      // Replica status doesn't exist. That means that this node is a writer.
      try (final ResultSet resultSet = stmt.executeQuery(this.nodeIdQuery)) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    }
    return null;
  }

  @Override
  protected String getSuggestedWriterNodeId(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.fetchWriterNodeQuery)) {
        if (resultSet.next()) {
          String nodeId = resultSet.getString(this.fetchWriterNodeColumnName);
          if (!StringUtils.isNullOrEmpty(nodeId)) {
            // Replica status exists and shows a writer node ID.
            // That means that this node (this connection) is a reader.
            // But we now what replication source is and that is a writer node.
            return nodeId;
          }
        }
      }
      // Replica status doesn't exist. That means that this node is a writer.
      try (final ResultSet resultSet = stmt.executeQuery(this.nodeIdQuery)) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    }
    return null;
  }

  @Override
  protected HostSpec createHost(
      final ResultSet resultSet,
      final String suggestedWriterNodeId) throws SQLException {

    String endpoint = resultSet.getString("endpoint"); // "instance-name.XYZ.us-west-2.rds.amazonaws.com"
    String instanceName = endpoint.substring(0, endpoint.indexOf(".")); // "instance-name"
    String hostId = resultSet.getString("id"); // "1034958454"
    final boolean isWriter = hostId.equals(suggestedWriterNodeId);

    return createHost(instanceName, isWriter, 0, Timestamp.from(Instant.now()), this.clusterInstanceTemplate);
  }
}
