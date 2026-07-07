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
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

@SuppressWarnings("unchecked")
public class CustomEndpointMonitorImplTest {
  @Mock private StorageService mockStorageService;
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
  }

  @Test
  public void testRun() throws InterruptedException {
    int refreshRateMs = 50;
    int maxRefreshRateMs = refreshRateMs * 10;
    CustomEndpointMonitorImpl monitor = new CustomEndpointMonitorImpl(
        mockStorageService,
        mockTelemetryFactory,
        host,
        endpointId,
        Region.US_EAST_1,
        TimeUnit.MILLISECONDS.toNanos(refreshRateMs),
        2,
        TimeUnit.MILLISECONDS.toNanos(maxRefreshRateMs),
        mockRdsClientFunc);
    monitor.start();

    // Wait for after 2 run cycles. The first will return an unexpected number of endpoints in the API response, the
    // second will return the expected number of endpoints (one).
    TimeUnit.MILLISECONDS.sleep(2 * refreshRateMs);
    int runCycles = 2;
    @Nullable CustomEndpointInfo customEndpointInfo = CustomEndpointMonitorImpl.customEndpointInfoCache
        .get(host.getUrl());
    while (customEndpointInfo == null && runCycles < 5) {
      TimeUnit.MILLISECONDS.sleep(refreshRateMs);
      runCycles++;
      customEndpointInfo = CustomEndpointMonitorImpl.customEndpointInfoCache.get(host.getUrl());
    }
    assertEquals(expectedInfo, customEndpointInfo);
    monitor.stop();

    ArgumentCaptor<AllowedAndBlockedHosts> captor = ArgumentCaptor.forClass(AllowedAndBlockedHosts.class);
    verify(mockStorageService).set(eq(host.getUrl()), captor.capture());
    assertEquals(staticMembersSet, captor.getValue().getAllowedHostIds());
    assertNull(captor.getValue().getBlockedHostIds());

    // Wait for monitor to close
    TimeUnit.MILLISECONDS.sleep(refreshRateMs);
    verify(mockRdsClient, atLeastOnce()).close();
  }

  /**
   * Reproduces the production issue where RDS {@code DescribeDBClusterEndpoints} calls keep getting throttled even
   * though the monitor has exponential-backoff logic that is supposed to slow the call rate down.
   *
   * <p>When the RDS API throttles the monitor, no custom endpoint info is ever cached. As a result, every incoming
   * connection calls {@link CustomEndpointMonitorImpl#requestCustomEndpointInfoUpdate()} /
   * {@link CustomEndpointMonitorImpl#hasCustomEndpointInfo()}, both of which set {@code refreshRequired = true}. That
   * flag is never cleared on the throttling path, and {@link CustomEndpointMonitorImpl#sleep(long)} returns
   * immediately whenever {@code refreshRequired} is {@code true}. Consequently the backoff sleep is bypassed and the
   * monitor spins, issuing RDS API calls as fast as it can instead of honoring the growing refresh rate.
   *
   * <p>This test starts a background "connection storm" that continuously requests custom endpoint info while the
   * RDS API always throttles, then asserts that the number of RDS API calls over a fixed window stays small (i.e.
   * the backoff is actually honored). It fails against the current (buggy) implementation and should pass once the
   * throttling backoff can no longer be short-circuited by connection-driven refresh requests.
   */
  @Test
  public void testThrottlingBackoffBypassedByConnectionStorm() throws InterruptedException {
    // Ensure no leftover cached info from other tests so hasCustomEndpointInfo() reflects the throttled state.
    CustomEndpointMonitorImpl.customEndpointInfoCache.clear();

    final int refreshRateMs = 50;
    final int backoffFactor = 2;
    final int maxRefreshRateMs = 2000;

    // The RDS API always responds with a throttling exception.
    final RdsException throttlingException = (RdsException) RdsException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
        .statusCode(400)
        .message("Rate exceeded")
        .build();

    final AtomicInteger describeCallCount = new AtomicInteger(0);
    when(mockRdsClient.describeDBClusterEndpoints(any(Consumer.class))).thenAnswer(invocation -> {
      int count = describeCallCount.incrementAndGet();
      // Safety valve: if the backoff is bypassed the monitor spins. Slow the runaway loop once it is clearly
      // established so the test does not exhaust resources while still far exceeding the expected call count.
      if (count > 2000) {
        TimeUnit.MILLISECONDS.sleep(5);
      }
      throw throttlingException;
    });

    final CustomEndpointMonitorImpl monitor = new CustomEndpointMonitorImpl(
        mockStorageService,
        mockTelemetryFactory,
        host,
        endpointId,
        Region.US_EAST_1,
        TimeUnit.MILLISECONDS.toNanos(refreshRateMs),
        backoffFactor,
        TimeUnit.MILLISECONDS.toNanos(maxRefreshRateMs),
        mockRdsClientFunc);

    // Simulate a connection storm: connections continuously ask the monitor for custom endpoint info while it is
    // unable to provide any (because it is being throttled). Each request sets refreshRequired = true.
    final AtomicBoolean stormActive = new AtomicBoolean(true);
    final Thread stormThread = new Thread(() -> {
      while (stormActive.get()) {
        monitor.requestCustomEndpointInfoUpdate();
        try {
          TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    });

    monitor.start();
    stormThread.start();

    // Measure how many RDS API calls the monitor issues during the window.
    final long measurementWindowMs = 1000;
    TimeUnit.MILLISECONDS.sleep(measurementWindowMs);

    stormActive.set(false);
    stormThread.join(TimeUnit.SECONDS.toMillis(5));
    monitor.stop();

    final int calls = describeCallCount.get();

    // With the backoff honored (50 -> 100 -> 200 -> 400 -> 800 -> ... capped at 2000ms), the monitor makes only a
    // handful of calls (~4) during a 1s window. If connection-driven refresh requests bypass the backoff, the
    // monitor spins and issues far more calls.
    final int maxExpectedCalls = 10;
    assertTrue(
        calls <= maxExpectedCalls,
        "Expected throttling backoff to limit RDS DescribeDBClusterEndpoints calls to at most " + maxExpectedCalls
            + " during the " + measurementWindowMs + "ms window, but the monitor made " + calls + " calls. "
            + "The backoff is being bypassed by connection-driven refresh requests (refreshRequired).");
  }
}
