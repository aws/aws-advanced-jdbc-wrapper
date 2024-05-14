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

package software.amazon.jdbc.plugin.endpoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class EndpointServiceImplTest {

  private static final String CLUSTER_ID = "someClusterId";
  @Mock private PluginService mockPluginService;
  @Mock private HostListProvider mockHostListProvider;
  @Mock private EndpointMonitor mockEndpointMonitor;
  @Mock private EndpointMonitorInitializer mockEndpointMonitorInitializer;
  private static final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("some-instance").role(HostRole.WRITER).build();
  private static final int intervalMs = 1000;
  private static Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
  }

  @Test
  void test() {
    when(mockPluginService.getHostListProvider()).thenReturn(mockHostListProvider);
    when(mockHostListProvider.getClusterId()).thenReturn(CLUSTER_ID);
    final List<HostSpec> endpointHostSpecList = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-1").role(HostRole.WRITER).weight(-100)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-2").role(HostRole.WRITER).weight(0)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("instance-3").role(HostRole.WRITER).weight(100)
            .build()
    );
    when(mockEndpointMonitor.getEndpoints()).thenReturn(new ArrayDeque<>(endpointHostSpecList));

    EndpointService endpointService = new EndpointServiceImpl((a, b, c, d) -> mockEndpointMonitor);
    endpointService.startMonitoring(mockPluginService, hostSpec, props, intervalMs);

    final List<HostSpec> actualEndpointHostSpecList = endpointService.getEndpoints(CLUSTER_ID, props);

    assertEquals(endpointHostSpecList, actualEndpointHostSpecList);
  }

  @Test
  void test_nullEndpointMonitor() {
    EndpointService endpointService = new EndpointServiceImpl((a, b, c, d) -> mockEndpointMonitor);
    final List<HostSpec> actualEndpointHostSpecList = endpointService.getEndpoints(CLUSTER_ID, props);
    assertEquals(0, actualEndpointHostSpecList.size());
  }
}
