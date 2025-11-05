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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraTopologyUtils;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class MonitoringGlobalAuroraHostListProvider extends MonitoringRdsHostListProvider {

  static final Logger LOGGER = Logger.getLogger(MonitoringGlobalAuroraHostListProvider.class.getName());

  protected Map<String, HostSpec> instanceTemplatesByRegion = new HashMap<>();

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final GlobalAuroraTopologyUtils topologyUtils;

  static {
    // Intentionally register property definition in GlobalAuroraHostListProvider class.
    PropertyDefinition.registerPluginProperties(GlobalAuroraHostListProvider.class);
  }

  public MonitoringGlobalAuroraHostListProvider(
      GlobalAuroraTopologyUtils topologyUtils,
      Properties properties,
      String originalUrl,
      FullServicesContainer servicesContainer) {
    super(topologyUtils, properties, originalUrl, servicesContainer);
    this.topologyUtils = topologyUtils;
  }

  @Override
  protected void initSettings() throws SQLException {
    super.initSettings();

    // TODO: check if we have other places that parse into string-HostSpec maps, consider refactoring
    String templates = GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.getString(properties);
    if (StringUtils.isNullOrEmpty(templates)) {
      throw new SQLException(Messages.get("MonitoringGlobalAuroraHostListProvider.globalHostPatternsRequired"));
    }

    HostSpecBuilder hostSpecBuilder = this.hostListProviderService.getHostSpecBuilder();
    this.instanceTemplatesByRegion = Arrays.stream(templates.split(","))
        .map(x -> ConnectionUrlParser.parseHostPortPairWithRegionPrefix(x.trim(), () -> hostSpecBuilder))
        .collect(Collectors.toMap(
            Pair::getValue1,
            v -> {
              this.validateHostPatternSetting(v.getValue2().getHost());
              return v.getValue2();
            }));
    LOGGER.finest(Messages.get(
        "GlobalAuroraHostListProvider.detectedGdbPatterns", new Object[] {
            this.instanceTemplatesByRegion.entrySet().stream()
                .map(x -> String.format("\t[%s] -> %s", x.getKey(), x.getValue().getHostAndPort()))
                .collect(Collectors.joining("\n"))
        })
    );
  }

  protected ClusterTopologyMonitor initMonitor() throws SQLException {
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
                this.clusterInstanceTemplate,
                this.refreshRateNano,
                this.highRefreshRateNano,
                this.instanceTemplatesByRegion));
  }

  @Override
  protected List<HostSpec> queryForTopology(Connection connection) throws SQLException {
    return this.topologyUtils.queryForTopology(connection, this.initialHostSpec, this.instanceTemplatesByRegion);
  }
}
