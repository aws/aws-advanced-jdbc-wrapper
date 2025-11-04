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
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.dialect.GlobalTopologyDialect;
import software.amazon.jdbc.hostlistprovider.TopologyUtils;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.StringUtils;


public class GlobalTopologyMonitor extends ClusterTopologyMonitorImpl {

  private static final Logger LOGGER = Logger.getLogger(GlobalTopologyMonitor.class.getName());

  protected final Map<String, HostSpec> hostTemplatesByRegion;
  protected final GlobalTopologyDialect dialect;

  public GlobalTopologyMonitor(
      final FullServicesContainer servicesContainer,
      final TopologyUtils topologyUtils,
      final GlobalTopologyDialect dialect,
      final String clusterId,
      final HostSpec initialHostSpec,
      final Properties properties,
      final HostSpec clusterInstanceTemplate,
      final long refreshRateNano,
      final long highRefreshRateNano,
      final Map<String, HostSpec> hostTemplatesByRegion) {
    super(servicesContainer,
        topologyUtils,
        clusterId,
        initialHostSpec,
        properties,
        clusterInstanceTemplate,
        refreshRateNano,
        highRefreshRateNano);

    this.hostTemplatesByRegion = hostTemplatesByRegion;
    this.dialect = dialect;
  }

  @Override
  protected HostSpec getClusterInstanceTemplate(String instanceId, Connection connection) throws SQLException {
    String region = dialect.getRegion(instanceId, connection);
    if (!StringUtils.isNullOrEmpty(region)) {
      final HostSpec clusterInstanceTemplateForRegion = this.hostTemplatesByRegion.get(region);
      if (clusterInstanceTemplateForRegion == null) {
        throw new SQLException("Can't find cluster template for region " + region);
      }

      return clusterInstanceTemplateForRegion;
    }

    return this.clusterInstanceTemplate;
  }
}
