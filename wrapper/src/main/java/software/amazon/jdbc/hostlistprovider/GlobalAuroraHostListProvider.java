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

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.RdsUtils;

public class GlobalAuroraHostListProvider extends RdsHostListProvider {

  public static final AwsWrapperProperty GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS =
      new AwsWrapperProperty(
          "globalClusterInstanceHostPatterns",
          null,
          "Comma-separated list of the cluster instance DNS patterns that will be used to "
              + "build a complete instance endpoints. "
              + "A \"?\" character in these patterns should be used as a placeholder for cluster instance names. "
              + "This parameter is required for Global Aurora Databases. "
              + "Each region in the Global Aurora Database should be specified in the list.");

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final GlobalAuroraTopologyUtils topologyUtils;

  protected Map<String, HostSpec> instanceTemplatesByRegion;

  static {
    PropertyDefinition.registerPluginProperties(GlobalAuroraHostListProvider.class);
  }

  public GlobalAuroraHostListProvider(
      GlobalAuroraTopologyUtils topologyUtils, Properties properties, String originalUrl,
      FullServicesContainer servicesContainer) throws SQLException {
    super(topologyUtils, properties, originalUrl, servicesContainer);
    this.topologyUtils = topologyUtils;
    String instanceTemplates = GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.getString(properties);
    this.instanceTemplatesByRegion =
        this.topologyUtils.parseInstanceTemplates(instanceTemplates, this::validateHostPatternSetting);
  }

  protected ClusterTopologyMonitor getOrCreateMonitor() throws SQLException {
    return this.servicesContainer.getMonitorService().runIfAbsent(
        ClusterTopologyMonitorImpl.class,
        this.clusterId,
        this.servicesContainer,
        this.properties,
        (servicesContainer) ->
            new GlobalAuroraTopologyMonitor(
                servicesContainer,
                this.topologyUtils,
                this.clusterId,
                this.initialHostSpec,
                this.properties,
                this.instanceTemplate,
                this.refreshRateNano,
                this.highRefreshRateNano,
                this.instanceTemplatesByRegion));
  }
}
