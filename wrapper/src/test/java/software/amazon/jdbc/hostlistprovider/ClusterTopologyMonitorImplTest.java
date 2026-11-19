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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.storage.StorageService;

class ClusterTopologyMonitorImplTest {

  @Mock FullServicesContainer mockContainer;
  @Mock PluginService mockPluginService;
  @Mock StorageService mockStorageService;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock TopologyUtils mockTopologyUtils;

  private AutoCloseable closeable;
  private ClusterTopologyMonitorImpl monitor;
  private AtomicConnection mockMonitoringConnection;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockStorageService.get(any(), any(), any(Boolean.class))).thenReturn(null);

    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("host").build();
    monitor = new ClusterTopologyMonitorImpl(
        mockContainer, mockTopologyUtils, "cluster-id", hostSpec, new Properties(), hostSpec, 0, 0);
    mockMonitoringConnection = mock(AtomicConnection.class);
    monitor.monitoringConnection = mockMonitoringConnection;
  }

  @AfterEach
  void cleanUp() throws Exception {
    Thread.interrupted();
    closeable.close();
  }

  @Test
  void test_forceRefresh_verifyTopology_entersPanicMode() throws Exception {
    monitor.forceRefresh(true, 0);

    verify(mockMonitoringConnection).set(null);
  }

  @Test
  void test_forceRefresh_verifyTopology_interruptedThread_doesNotEnterPanicMode() throws Exception {
    Thread.currentThread().interrupt();
    monitor.forceRefresh(true, 0);

    verify(mockMonitoringConnection, never()).set(null);
  }
}
