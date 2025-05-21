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
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.util.CompleteServicesContainer;

public class MonitoringRdsMultiAzHostListProvider extends MonitoringRdsHostListProvider {

  private static final Logger LOGGER = Logger.getLogger(MonitoringRdsMultiAzHostListProvider.class.getName());

  protected final String fetchWriterNodeQuery;
  protected final String fetchWriterNodeColumnName;

  public MonitoringRdsMultiAzHostListProvider(
      final Properties properties,
      final String originalUrl,
      final CompleteServicesContainer servicesContainer,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery,
      final String fetchWriterNodeQuery,
      final String fetchWriterNodeColumnName) {
    super(
        properties,
        originalUrl,
        servicesContainer,
        topologyQuery,
        nodeIdQuery,
        isReaderQuery,
        "");
    this.fetchWriterNodeQuery = fetchWriterNodeQuery;
    this.fetchWriterNodeColumnName = fetchWriterNodeColumnName;
  }

  @Override
  protected ClusterTopologyMonitor initMonitor() throws SQLException {
    return this.servicesContainer.getMonitorService().runIfAbsent(MultiAzClusterTopologyMonitorImpl.class,
        this.clusterId,
        this.servicesContainer.getStorageService(),
        this.pluginService.getTelemetryFactory(),
        this.originalUrl,
        this.pluginService.getDriverProtocol(),
        this.pluginService.getTargetDriverDialect(),
        this.pluginService.getDialect(),
        this.properties,
        (connectionService, pluginService) -> new MultiAzClusterTopologyMonitorImpl(
            this.clusterId,
            this.servicesContainer.getStorageService(),
            this.servicesContainer.getMonitorService(),
            connectionService,
            this.initialHostSpec,
            this.properties,
            pluginService,
            this.hostListProviderService,
            this.clusterInstanceTemplate,
            this.refreshRateNano,
            this.highRefreshRateNano,
            this.topologyQuery,
            this.writerTopologyQuery,
            this.nodeIdQuery,
            this.fetchWriterNodeQuery,
            this.fetchWriterNodeColumnName));
  }

}
