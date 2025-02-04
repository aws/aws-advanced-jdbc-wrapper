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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraGlobalDbHostListProvider;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class AuroraGlobalDbMonitoringHostListProvider extends MonitoringRdsHostListProvider {

  static final Logger LOGGER = Logger.getLogger(AuroraGlobalDbMonitoringHostListProvider.class.getName());

  protected Map<String, HostSpec> globalClusterInstanceTemplateByAwsRegion = new HashMap<>();

  protected final RdsUtils rdsUtils = new RdsUtils();

  protected String regionByNodeIdQuery;

  static {
    // Register property definition in AuroraGlobalDbHostListProvider class. It's not a mistake.
    PropertyDefinition.registerPluginProperties(AuroraGlobalDbHostListProvider.class);
  }

  public AuroraGlobalDbMonitoringHostListProvider(Properties properties, String originalUrl,
      HostListProviderService hostListProviderService, String globalTopologyQuery,
      String nodeIdQuery, String isReaderQuery, String writerTopologyQuery,
      String regionByNodeIdQuery,
      PluginService pluginService) {

    super(properties, originalUrl, hostListProviderService, globalTopologyQuery, nodeIdQuery, isReaderQuery,
        writerTopologyQuery, pluginService);
    this.regionByNodeIdQuery = regionByNodeIdQuery;
  }

  @Override
  protected void initSettings() throws SQLException {
    super.initSettings();

    String templates = AuroraGlobalDbHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.getString(properties);
    if (StringUtils.isNullOrEmpty(templates)) {
      throw new SQLException("Parameter 'globalClusterInstanceHostPatterns' is required for Aurora Global Database.");
    }

    HostSpecBuilder hostSpecBuilder = this.hostListProviderService.getHostSpecBuilder();
    this.globalClusterInstanceTemplateByAwsRegion = Arrays.stream(templates.split(","))
        .map(x -> ConnectionUrlParser.parseHostPortPairWithRegionPrefix(x.trim(), () -> hostSpecBuilder))
        .collect(Collectors.toMap(
            k -> k.getValue1(),
            v -> {
              this.validateHostPatternSetting(v.getValue2().getHost());
              return v.getValue2();
            }));
    LOGGER.finest(() -> "Recognized GDB instance template patterns:\n"
          + this.globalClusterInstanceTemplateByAwsRegion.entrySet().stream()
              .map(x -> String.format("\t[%s] -> %s", x.getKey(), x.getValue().getHostAndPort()))
              .collect(Collectors.joining("\n"))
    );
  }

  protected ClusterTopologyMonitor initMonitor() {
    return monitors.computeIfAbsent(this.clusterId,
        (key) -> new GlobalDbClusterTopologyMonitorImpl(
            key, topologyCache, this.initialHostSpec, this.properties, this.pluginService,
            this.hostListProviderService, this.clusterInstanceTemplate,
            this.refreshRateNano, this.highRefreshRateNano, TOPOLOGY_CACHE_EXPIRATION_NANO,
            this.topologyQuery,
            this.writerTopologyQuery,
            this.nodeIdQuery,
            this.globalClusterInstanceTemplateByAwsRegion,
            this.regionByNodeIdQuery),
        MONITOR_EXPIRATION_NANO);
  }
}
