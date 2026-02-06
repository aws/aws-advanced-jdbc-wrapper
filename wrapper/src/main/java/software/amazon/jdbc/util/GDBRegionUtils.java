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

package software.amazon.jdbc.util;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.authentication.AwsCredentialsManager;

public class GDBRegionUtils extends RegionUtils {

  private static final Pattern GDB_CLUSTER_ARN_PATTERN =
      Pattern.compile("^arn:aws:rds:(?<region>[^:\\n]*):[^:\\n]*:([^:/\\n]*[:/])?(.*)$");
  private static final String REGION_GROUP = "region";

  private AwsCredentialsProvider credentialsProvider;

  static {
    try {
      Class.forName("software.amazon.awssdk.services.rds.RdsClient");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("AuthenticationToken.rdsSdkNotInClasspath"), e);
    }
  }

  public GDBRegionUtils() {
  }

  public GDBRegionUtils(AwsCredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public @Nullable Region getRegion(HostSpec hostSpec, Properties props, String propKey) {
    final String clusterId = rdsUtils.getRdsClusterId(hostSpec.getHost());
    final String writerClusterArn = findWriterClusterArn(hostSpec, props, clusterId);

    if (StringUtils.isNullOrEmpty(writerClusterArn)) {
      return null;
    }

    return getRegionFromClusterArn(writerClusterArn);
  }

  private String findWriterClusterArn(final HostSpec hostSpec, final Properties props,
      final String globalClusterIdentifier) {

    if (this.credentialsProvider == null) {
      this.credentialsProvider = AwsCredentialsManager.getProvider(hostSpec, props);
    }
    
    try (RdsClient rdsClient = RdsClient.builder()
        .credentialsProvider(this.credentialsProvider)
        .build()) {
      final DescribeGlobalClustersRequest request = DescribeGlobalClustersRequest.builder()
          .globalClusterIdentifier(globalClusterIdentifier)
          .build();

      return rdsClient.describeGlobalClusters(request)
          .globalClusters()
          .stream()
          .flatMap(cluster -> cluster.globalClusterMembers().stream())
          .filter(GlobalClusterMember::isWriter)
          .map(GlobalClusterMember::dbClusterArn)
          .findFirst()
          .orElse(null);
    }
  }

  private Region getRegionFromClusterArn(String clusterArn) {
    Matcher matcher = GDB_CLUSTER_ARN_PATTERN.matcher(clusterArn);
    return matcher.matches() ? Region.of(matcher.group(REGION_GROUP)) : null;
  }
}
