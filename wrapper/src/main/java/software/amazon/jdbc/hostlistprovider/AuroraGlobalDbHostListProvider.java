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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class AuroraGlobalDbHostListProvider extends AuroraHostListProvider {

  static final Logger LOGGER = Logger.getLogger(AuroraGlobalDbHostListProvider.class.getName());

  public static final AwsWrapperProperty GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS =
      new AwsWrapperProperty(
          "globalClusterInstanceHostPatterns",
          null,
          "Coma-separated list of the cluster instance DNS patterns that will be used to "
              + "build a complete instance endpoints. "
              + "A \"?\" character in these patterns should be used as a placeholder for cluster instance names. "
              + "This parameter is required for Global Aurora Databases. "
              + "Each region of Global Aurora Database should be specified in the list.");

  protected final RdsUtils rdsUtils = new RdsUtils();

  protected Map<String, HostSpec> globalClusterInstanceTemplateByAwsRegion;

  static {
    PropertyDefinition.registerPluginProperties(AuroraGlobalDbHostListProvider.class);
  }

  public AuroraGlobalDbHostListProvider(Properties properties, String originalUrl,
      HostListProviderService hostListProviderService, String topologyQuery,
      String nodeIdQuery, String isReaderQuery) {
    super(properties, originalUrl, hostListProviderService, topologyQuery, nodeIdQuery, isReaderQuery);
  }

  @Override
  protected void initSettings() throws SQLException {
    super.initSettings();

    String templates = GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.getString(properties);
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

  @Override
  protected HostSpec createHost(final ResultSet resultSet) throws SQLException {

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
