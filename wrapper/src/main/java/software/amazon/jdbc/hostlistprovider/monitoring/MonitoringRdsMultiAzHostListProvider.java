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

import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.PluginService;

public class MonitoringRdsMultiAzHostListProvider extends MonitoringRdsHostListProvider {

  private static final Logger LOGGER = Logger.getLogger(MonitoringRdsMultiAzHostListProvider.class.getName());

  protected final String fetchWriterNodeQuery;
  protected final String fetchWriterNodeColumnName;

  public MonitoringRdsMultiAzHostListProvider(
      final Properties properties,
      final String originalUrl,
      final HostListProviderService hostListProviderService,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery,
      final PluginService pluginService,
      final String fetchWriterNodeQuery,
      final String fetchWriterNodeColumnName) {
    super(properties, originalUrl, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery,
        "", pluginService);
    this.fetchWriterNodeQuery = fetchWriterNodeQuery;
    this.fetchWriterNodeColumnName = fetchWriterNodeColumnName;
  }

  @Override
  protected ClusterTopologyMonitor initMonitor() {
    return monitors.computeIfAbsent(this.clusterId,
        (key) -> new MultiAzClusterTopologyMonitorImpl(
            key, topologyCache, this.initialHostSpec, this.properties, this.pluginService,
            this.hostListProviderService, this.clusterInstanceTemplate,
            this.refreshRateNano, this.highRefreshRateNano, TOPOLOGY_CACHE_EXPIRATION_NANO,
            this.topologyQuery,
            this.writerTopologyQuery,
            this.nodeIdQuery,
            this.fetchWriterNodeQuery,
            this.fetchWriterNodeColumnName),
        MONITOR_EXPIRATION_NANO);
  }

}
