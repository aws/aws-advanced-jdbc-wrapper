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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.StringUtils;

public class GlobalDbClusterTopologyMonitorImpl extends ClusterTopologyMonitorImpl {

  private static final Logger LOGGER = Logger.getLogger(GlobalDbClusterTopologyMonitorImpl.class.getName());

  protected final Map<String, HostSpec> globalClusterInstanceTemplateByAwsRegion;
  protected final String regionByNodeIdQuery;

  public GlobalDbClusterTopologyMonitorImpl(String clusterId,
      CacheMap<String, List<HostSpec>> topologyMap,
      HostSpec initialHostSpec, Properties properties,
      PluginService pluginService,
      HostListProviderService hostListProviderService,
      HostSpec clusterInstanceTemplate, long refreshRateNano,
      long highRefreshRateNano, long topologyCacheExpirationNano, String globalTopologyQuery,
      String writerTopologyQuery, String nodeIdQuery,
      Map<String, HostSpec> globalClusterInstanceTemplateByAwsRegion,
      String regionByNodeIdQuery) {

    super(clusterId, topologyMap, initialHostSpec, properties, pluginService, hostListProviderService,
        clusterInstanceTemplate, refreshRateNano, highRefreshRateNano, topologyCacheExpirationNano, globalTopologyQuery,
        writerTopologyQuery, nodeIdQuery);
    this.globalClusterInstanceTemplateByAwsRegion = globalClusterInstanceTemplateByAwsRegion;
    this.regionByNodeIdQuery = regionByNodeIdQuery;
  }

  @Override
  protected HostSpec getClusterInstanceTemplate(String nodeId, Connection connection) {
    try {
      try (final PreparedStatement stmt = connection.prepareStatement(this.regionByNodeIdQuery)) {
        stmt.setString(1, nodeId);
        try (final ResultSet resultSet = stmt.executeQuery()) {
          if (resultSet.next()) {
            String awsRegion = resultSet.getString(1);
            if (!StringUtils.isNullOrEmpty(awsRegion)) {
              final HostSpec clusterInstanceTemplateForRegion
                  = this.globalClusterInstanceTemplateByAwsRegion.get(awsRegion);
              if (clusterInstanceTemplateForRegion == null) {
                throw new SQLException("Can't find cluster template for region " + awsRegion);
              }
              return clusterInstanceTemplateForRegion;
            }
          }
        }
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
    return this.clusterInstanceTemplate;
  }

  @Override
  protected HostSpec createHost(
      final ResultSet resultSet,
      final String suggestedWriterNodeId) throws SQLException {

    // suggestedWriterNodeId is not used for Aurora clusters. Topology query can detect a writer for itself.

    // According to the topology query the result set
    // should contain 4 columns: node ID, 1/0 (writer/reader), node lag in time (msec), AWS region.
    String hostName = resultSet.getString(1);
    final boolean isWriter = resultSet.getBoolean(2);
    final float nodeLag = resultSet.getFloat(3);
    final String awsRegion = resultSet.getString(4);

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L;

    final HostSpec clusterInstanceTemplateForRegion = this.globalClusterInstanceTemplateByAwsRegion.get(awsRegion);
    if (clusterInstanceTemplateForRegion == null) {
      throw new SQLException("Can't find cluster template for region " + awsRegion);
    }

    return createHost(hostName, isWriter, weight, Timestamp.from(Instant.now()), clusterInstanceTemplateForRegion);
  }
}
