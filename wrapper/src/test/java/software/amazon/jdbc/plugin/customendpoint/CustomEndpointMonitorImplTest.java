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

package software.amazon.jdbc.plugin.customendpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class CustomEndpointMonitorImplTest {
  @Mock private PluginService mockPluginService;
  @Mock private BiFunction<HostSpec, Region, RdsClient> mockRdsClientFunc;
  @Mock private RdsClient mockRdsClient;
  @Mock private DescribeDbClusterEndpointsResponse mockDescribeResponse;
  @Mock private DBClusterEndpoint mockClusterEndpoint1;
  @Mock private DBClusterEndpoint mockClusterEndpoint2;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryCounter mockTelemetryCounter;

  private final String customEndpointUrl1 = "custom1.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";
  private final String customEndpointUrl2 = "custom2.cluster-custom-XYZ.us-east-1.rds.amazonaws.com";
  private final String endpointId = "custom1";
  private final String clusterId = "cluster1";
  private final String endpointRoleType = "ANY";
  private List<DBClusterEndpoint> twoEndpointList;
  private List<DBClusterEndpoint> oneEndpointList;
  private final List<String> staticMembersList = Arrays.asList("member1", "member2");
  private final Set<String> staticMembersSet = new HashSet<>(staticMembersList);
  private final CustomEndpointInfo expectedInfo = new CustomEndpointInfo(
      endpointId,
      clusterId,
      customEndpointUrl1,
      CustomEndpointRoleType.valueOf(endpointRoleType),
      staticMembersSet,
      MemberListType.STATIC_LIST);

  private AutoCloseable closeable;
  private final HostAvailabilityStrategy availabilityStrategy = new SimpleHostAvailabilityStrategy();
  private final HostSpecBuilder hostSpecBuilder = new HostSpecBuilder(availabilityStrategy);
  private final HostSpec host = hostSpecBuilder.host(customEndpointUrl1).build();

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    twoEndpointList = Arrays.asList(mockClusterEndpoint1, mockClusterEndpoint2);
    oneEndpointList = Collections.singletonList(mockClusterEndpoint1);

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(any(String.class))).thenReturn(mockTelemetryCounter);
    when(mockRdsClientFunc.apply(any(HostSpec.class), any(Region.class))).thenReturn(mockRdsClient);
    when(mockRdsClient.describeDBClusterEndpoints(any(Consumer.class))).thenReturn(mockDescribeResponse);
    when(mockDescribeResponse.dbClusterEndpoints()).thenReturn(twoEndpointList).thenReturn(oneEndpointList);
    when(mockClusterEndpoint1.endpoint()).thenReturn(customEndpointUrl1);
    when(mockClusterEndpoint2.endpoint()).thenReturn(customEndpointUrl2);
    when(mockClusterEndpoint1.hasStaticMembers()).thenReturn(true);
    when(mockClusterEndpoint1.staticMembers()).thenReturn(staticMembersList);
    when(mockClusterEndpoint1.dbClusterEndpointIdentifier()).thenReturn(endpointId);
    when(mockClusterEndpoint1.dbClusterIdentifier()).thenReturn(clusterId);
    when(mockClusterEndpoint1.customEndpointType()).thenReturn(endpointRoleType);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    CustomEndpointPlugin.monitors.clear();
  }

  @Test
  public void testRun() throws InterruptedException {
    CustomEndpointMonitorImpl monitor = new CustomEndpointMonitorImpl(
        mockPluginService, host, endpointId, Region.US_EAST_1, TimeUnit.MILLISECONDS.toNanos(50), mockRdsClientFunc);
    // Wait for 2 run cycles. The first will return an unexpected number of endpoints in the API response, the second
    // will return the expected number of endpoints (one).
    TimeUnit.MILLISECONDS.sleep(100);
    assertEquals(expectedInfo, CustomEndpointMonitorImpl.customEndpointInfoCache.get(host.getHost()));
    monitor.close();

    ArgumentCaptor<AllowedAndBlockedHosts> captor = ArgumentCaptor.forClass(AllowedAndBlockedHosts.class);
    verify(mockPluginService).setAllowedAndBlockedHosts(captor.capture());
    assertEquals(staticMembersSet, captor.getValue().getAllowedHostIds());
    assertNull(captor.getValue().getBlockedHostIds());

    // Wait for monitor to close
    TimeUnit.MILLISECONDS.sleep(50);
    assertTrue(monitor.stop.get());
    verify(mockRdsClient, atLeastOnce()).close();
  }
}
