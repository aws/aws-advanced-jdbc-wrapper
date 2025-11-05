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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class GlobalAuroraHostListProvider extends RdsHostListProvider {

  static final Logger LOGGER = Logger.getLogger(GlobalAuroraHostListProvider.class.getName());

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
      FullServicesContainer servicesContainer) {
    super(topologyUtils, properties, originalUrl, servicesContainer);
    this.topologyUtils = topologyUtils;
  }

  @Override
  protected void initSettings() throws SQLException {
    super.initSettings();

    String templates = GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.getString(properties);
    if (StringUtils.isNullOrEmpty(templates)) {
      throw new SQLException(Messages.get("GlobalAuroraHostListProvider.globalClusterInstanceHostPatternsRequired"));
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

    // TODO: utility to convert Map<String, HostSpec> to log string
    LOGGER.finest(Messages.get("GlobalAuroraHostListProvider.detectedGdbPatterns", new Object[] {
        this.instanceTemplatesByRegion.entrySet().stream()
            .map(x -> String.format("\t[%s] -> %s", x.getKey(), x.getValue().getHostAndPort()))
            .collect(Collectors.joining("\n"))
    }));
  }

  @Override
  protected List<HostSpec> queryForTopology(final Connection conn) throws SQLException {
    init();
    return this.topologyUtils.queryForTopology(conn, this.initialHostSpec, this.instanceTemplatesByRegion);
  }
}
