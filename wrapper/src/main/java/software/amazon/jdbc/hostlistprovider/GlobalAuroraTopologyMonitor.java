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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class GlobalAuroraTopologyMonitor extends ClusterTopologyMonitorImpl {
  protected final Map<String, HostSpec> instanceTemplatesByRegion;
  protected final GlobalAuroraTopologyUtils topologyUtils;

  public GlobalAuroraTopologyMonitor(
      final FullServicesContainer servicesContainer,
      final GlobalAuroraTopologyUtils topologyUtils,
      final String clusterId,
      final HostSpec initialHostSpec,
      final Properties properties,
      final HostSpec instanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano,
      final Map<String, HostSpec> instanceTemplatesByRegion) {
    super(
        servicesContainer,
        topologyUtils,
        clusterId,
        initialHostSpec,
        properties,
        instanceTemplate,
        refreshRateNano,
        highRefreshRateNano);

    this.instanceTemplatesByRegion = instanceTemplatesByRegion;
    this.topologyUtils = topologyUtils;
  }

  @Override
  protected HostSpec getInstanceTemplate(String instanceId, Connection connection)
      throws SQLException {
    String region = this.topologyUtils.getRegion(instanceId, connection);
    if (!StringUtils.isNullOrEmpty(region)) {
      final HostSpec instanceTemplate = this.instanceTemplatesByRegion.get(region);
      if (instanceTemplate == null) {
        throw new SQLException(
            Messages.get(
                "GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", new Object[] {region}));
      }

      return instanceTemplate;
    }

    return this.instanceTemplate;
  }

  @Override
  protected List<HostSpec> queryForTopology(Connection connection) throws SQLException {
    return this.topologyUtils.queryForTopology(
        connection, this.initialHostSpec, this.instanceTemplatesByRegion);
  }
}
