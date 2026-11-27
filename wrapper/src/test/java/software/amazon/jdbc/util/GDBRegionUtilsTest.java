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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class GDBRegionUtilsTest {

  private static final String IAM_REGION = "iamRegion";
  private static final String GLOBAL_WRITER_HOST =
      "global-cluster-test-name.global-XYZ.global.rds.amazonaws.com";
  private static final String REGIONAL_CLUSTER_HOST =
      "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";

  private MockedStatic<RdsClient> rdsClientStatic;
  private RdsClient rdsClient;
  private GDBRegionUtils target;

  @BeforeEach
  public void setUp() {
    RdsUtils.clearCache();

    rdsClient = mock(RdsClient.class);
    final RdsClientBuilder builder = mock(RdsClientBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(rdsClient);
    when(rdsClient.describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(
        DescribeGlobalClustersResponse.builder()
            .globalClusters(GlobalCluster.builder()
                .globalClusterIdentifier("global-cluster-test-name")
                .globalClusterMembers(
                    GlobalClusterMember.builder()
                        .dbClusterArn("arn:aws:rds:eu-west-1:123456789012:cluster:secondary-cluster")
                        .isWriter(false)
                        .build(),
                    GlobalClusterMember.builder()
                        .dbClusterArn("arn:aws:rds:us-west-2:123456789012:cluster:primary-cluster")
                        .isWriter(true)
                        .build())
                .build())
            .build());

    rdsClientStatic = mockStatic(RdsClient.class);
    rdsClientStatic.when(RdsClient::builder).thenReturn(builder);

    target = new GDBRegionUtils(mock(AwsCredentialsProvider.class));
  }

  @AfterEach
  public void tearDown() {
    rdsClientStatic.close();
  }

  @Test
  public void testGlobalWriterHostLooksUpGlobalClusterAndReturnsWriterRegion() {
    final Region region = target.getRegion(hostSpec(GLOBAL_WRITER_HOST), new Properties(), IAM_REGION);

    final ArgumentCaptor<DescribeGlobalClustersRequest> requestCaptor =
        ArgumentCaptor.forClass(DescribeGlobalClustersRequest.class);
    verify(rdsClient).describeGlobalClusters(requestCaptor.capture());
    assertEquals("global-cluster-test-name", requestCaptor.getValue().globalClusterIdentifier());
    assertEquals(Region.US_WEST_2, region);
  }

  @Test
  public void testRegionPropertySkipsRdsLookup() {
    final Properties props = new Properties();
    props.setProperty(IAM_REGION, "us-east-2");

    final Region region = target.getRegion(hostSpec(GLOBAL_WRITER_HOST), props, IAM_REGION);

    assertEquals(Region.US_EAST_2, region);
    rdsClientStatic.verifyNoInteractions();
  }

  @Test
  public void testNonGlobalHostReturnsNullWithoutRdsLookup() {
    final Region region = target.getRegion(hostSpec(REGIONAL_CLUSTER_HOST), new Properties(), IAM_REGION);

    assertNull(region);
    rdsClientStatic.verifyNoInteractions();
  }

  private static HostSpec hostSpec(final String host) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).build();
  }
}
