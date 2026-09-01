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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.HostListProviderSupplier;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;

public class PartialPluginServiceTests {

  private static final Properties PROPERTIES = new Properties();
  private static final String URL = "url";
  private static final String DRIVER_PROTOCOL = "driverProtocol";

  private AutoCloseable closeable;

  @Mock FullServicesContainer servicesContainer;
  @Mock ConnectionPluginManager pluginManager;
  @Mock HostListProvider hostListProvider;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;

  private final ConfigurationProfile configurationProfile =
      ConfigurationProfileBuilder.get().withName("test").build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    final HostListProviderSupplier supplier = (props, url, container) -> hostListProvider;
    when(mockDialect.getHostListProviderSupplier()).thenReturn(supplier);
    PartialPluginService.hostAvailabilityExpiringCache.clear();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    PartialPluginService.hostAvailabilityExpiringCache.clear();
  }

  private PartialPluginService createTarget() throws SQLException {
    return new PartialPluginService(
        servicesContainer,
        new ExceptionManager(),
        PROPERTIES,
        URL,
        DRIVER_PROTOCOL,
        mockTargetDriverDialect,
        mockDialect,
        configurationProfile);
  }

  @Test
  public void testSetNodeListMeasuredDetailsChanged() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(any());

    when(hostListProvider.refresh()).thenReturn(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER)
            .weight(200).cpuPercent(90.0F).lagMs(800.0F).build()));

    final PartialPluginService target = createTarget();
    target.allHosts = Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER)
        .weight(100).cpuPercent(10.0F).lagMs(5.0F).build());

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    final HostSpec updatedHost = target.getAllHosts().get(0);
    assertEquals("hostA", updatedHost.getHost());
    assertEquals(200, updatedHost.getWeight());
    assertEquals(90.0F, updatedHost.getCpuPercent(), 0.0001F);
    assertEquals(800.0F, updatedHost.getLagMs(), 0.0001F);
    verify(pluginManager, times(0)).notifyNodeListChanged(any());
  }

  @Test
  public void testSetNodeListNoChanges() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(any());

    when(hostListProvider.refresh()).thenReturn(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER)
            .weight(100).cpuPercent(10.0F).lagMs(5.0F).build()));

    final PartialPluginService target = createTarget();
    target.allHosts = Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER)
        .weight(100).cpuPercent(10.0F).lagMs(5.0F).build());

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    assertEquals("hostA", target.getAllHosts().get(0).getHost());
    verify(pluginManager, times(0)).notifyNodeListChanged(any());
  }
}
