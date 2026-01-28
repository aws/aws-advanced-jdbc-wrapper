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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.dialect.MysqlDialect;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.events.EventPublisher;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.storage.TestStorageServiceImpl;

public class PluginServiceImplTests {

  private static final Properties PROPERTIES = new Properties();
  private static final String URL = "url";
  private static final String DRIVER_PROTOCOL = "driverProtocol";
  private StorageService storageService;
  private AutoCloseable closeable;

  @Mock FullServicesContainer servicesContainer;
  @Mock ImportantEventService mockImportantEventService;
  @Mock EventPublisher mockEventPublisher;
  @Mock ConnectionPluginManager pluginManager;
  @Mock Connection newConnection;
  @Mock Connection oldConnection;
  @Mock HostListProvider hostListProvider;
  @Mock DialectManager dialectManager;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock Statement statement;
  @Mock ResultSet resultSet;
  ConfigurationProfile configurationProfile = ConfigurationProfileBuilder.get().withName("test").build();
  @Mock SessionStateService sessionStateService;

  @Captor ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges;
  @Captor ArgumentCaptor<Map<String, EnumSet<NodeChangeOptions>>> argumentChangesMap;
  @Captor ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(oldConnection.isClosed()).thenReturn(false);
    when(newConnection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(any())).thenReturn(resultSet);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    when(servicesContainer.getStorageService()).thenReturn(storageService);
    when(servicesContainer.getImportantEventService()).thenReturn(mockImportantEventService);
    storageService = new TestStorageServiceImpl(mockEventPublisher);
    PluginServiceImpl.hostAvailabilityExpiringCache.clear();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    storageService.clearAll();
    PluginServiceImpl.hostAvailabilityExpiringCache.clear();
  }

  @Test
  public void testOldConnectionNoSuggestion() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("old-host")
        .build();

    target.setCurrentConnection(newConnection,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("new-host").build());

    assertNotEquals(oldConnection, target.currentConnection);
    assertEquals(newConnection, target.currentConnection);
    assertEquals("new-host", target.currentHostSpec.getHost());
    verify(oldConnection, times(1)).close();
  }

  @Test
  public void testOldConnectionDisposeSuggestion() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.DISPOSE));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("old-host")
        .build();

    target.setCurrentConnection(newConnection,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("new-host").build());

    assertNotEquals(oldConnection, target.currentConnection);
    assertEquals(newConnection, target.currentConnection);
    assertEquals("new-host", target.currentHostSpec.getHost());
    verify(oldConnection, times(1)).close();
  }

  @Test
  public void testOldConnectionPreserveSuggestion() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.PRESERVE));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("old-host")
        .build();

    target.setCurrentConnection(newConnection,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("new-host").build());

    assertNotEquals(oldConnection, target.currentConnection);
    assertEquals(newConnection, target.currentConnection);
    assertEquals("new-host", target.currentHostSpec.getHost());
    verify(oldConnection, times(0)).close();
  }

  @Test
  public void testOldConnectionMixedSuggestion() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(
            EnumSet.of(
                OldConnectionSuggestedAction.NO_OPINION,
                OldConnectionSuggestedAction.PRESERVE,
                OldConnectionSuggestedAction.DISPOSE));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("old-host")
        .build();

    target.setCurrentConnection(newConnection,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("new-host").build());

    assertNotEquals(oldConnection, target.currentConnection);
    assertEquals(newConnection, target.currentConnection);
    assertEquals("new-host", target.currentHostSpec.getHost());
    verify(oldConnection, times(0)).close();
  }

  @Test
  public void testChangesNewConnectionNewHostNewPortNewRoleNewAvailability() throws SQLException {
    when(pluginManager.notifyConnectionChanged(
        argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("old-host").port(1000).role(HostRole.WRITER).availability(HostAvailability.AVAILABLE).build();

    target.setCurrentConnection(
        newConnection,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("new-host").port(2000).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
            .build());

    assertNull(argumentSkipPlugin.getValue());
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.NODE_CHANGED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_ADDED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_DELETED));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.HOSTNAME));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_READER));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_WRITER));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.WENT_DOWN));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testChangesNewConnectionNewRoleNewAvailability() throws SQLException {
    when(pluginManager.notifyConnectionChanged(
        argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("old-host").port(1000).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
            .build();

    target.setCurrentConnection(newConnection, new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("old-host").port(1000).role(HostRole.WRITER).availability(HostAvailability.AVAILABLE)
            .build());

    assertNull(argumentSkipPlugin.getValue());
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.NODE_CHANGED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_ADDED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_DELETED));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.HOSTNAME));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_READER));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_WRITER));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.WENT_DOWN));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testChangesNewConnection() throws SQLException {
    when(pluginManager.notifyConnectionChanged(
        argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("old-host").port(1000).role(HostRole.READER).availability(HostAvailability.AVAILABLE)
            .build();

    target.setCurrentConnection(
        newConnection, new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("old-host").port(1000).role(HostRole.READER).availability(HostAvailability.AVAILABLE)
            .build());

    assertNull(argumentSkipPlugin.getValue());
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_CHANGED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_ADDED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.NODE_DELETED));
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.HOSTNAME));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_READER));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.PROMOTED_TO_WRITER));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.WENT_DOWN));
    assertFalse(argumentChanges.getValue().contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testChangesNoChanges() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any())).thenReturn(
        EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("old-host").port(1000).role(HostRole.READER).availability(HostAvailability.AVAILABLE).build();

    target.setCurrentConnection(
        oldConnection, new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("old-host").port(1000).role(HostRole.READER).availability(HostAvailability.AVAILABLE)
            .build());

    verify(pluginManager, times(0)).notifyConnectionChanged(any(), any());
  }

  @Test
  public void testSetNodeListAdded() throws SQLException {

    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    when(hostListProvider.refresh()).thenReturn(Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").build()));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = new ArrayList<>();
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    assertEquals("hostA", target.getAllHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(1, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_ADDED));
  }

  @Test
  public void testSetNodeListDeleted() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    when(hostListProvider.refresh()).thenReturn(Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").build()));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").build());
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    assertEquals("hostB", target.getAllHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(1, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_DELETED));
  }

  @Test
  public void testSetNodeListChanged() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    when(hostListProvider.refresh()).thenReturn(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA")
            .port(HostSpec.NO_PORT).role(HostRole.READER).build()));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.WRITER).build());
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    assertEquals("hostA", target.getAllHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.PROMOTED_TO_READER));
  }

  @Test
  public void testSetNodeListNoChanges() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(any());

    when(hostListProvider.refresh()).thenReturn(
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).build()));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).build());
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getAllHosts().size());
    assertEquals("hostA", target.getAllHosts().get(0).getHost());
    verify(pluginManager, times(0)).notifyNodeListChanged(any());
  }

  @Test
  public void testNodeAvailabilityNotChanged() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.AVAILABLE)
            .build());

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(1, target.getAllHosts().size());
    assertEquals(HostAvailability.AVAILABLE, target.getAllHosts().get(0).getAvailability());
    verify(pluginManager, never()).notifyNodeListChanged(any());
  }

  @Test
  public void testNodeAvailabilityChanged_WentDown() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.AVAILABLE)
            .build());

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.NOT_AVAILABLE);

    assertEquals(1, target.getAllHosts().size());
    assertEquals(HostAvailability.NOT_AVAILABLE, target.getAllHosts().get(0).getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_DOWN));
  }

  @Test
  public void testNodeAvailabilityChanged_WentUp() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.allHosts = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
            .build());

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(1, target.getAllHosts().size());
    assertEquals(HostAvailability.AVAILABLE, target.getAllHosts().get(0).getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testNodeAvailabilityChanged_WentUp_ByAlias() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    final HostSpec hostA = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
        .build();
    hostA.addAlias("ip-10-10-10-10");
    hostA.addAlias("hostA.custom.domain.com");
    final HostSpec hostB = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostB").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
        .build();
    hostB.addAlias("ip-10-10-10-10");
    hostB.addAlias("hostB.custom.domain.com");

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));

    target.allHosts = Arrays.asList(hostA, hostB);

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA.custom.domain.com");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(HostAvailability.AVAILABLE, hostA.getAvailability());
    assertEquals(HostAvailability.NOT_AVAILABLE, hostB.getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testNodeAvailabilityChanged_WentUp_MultipleHostsByAlias() throws SQLException {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    final HostSpec hostA = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostA").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
        .build();
    hostA.addAlias("ip-10-10-10-10");
    hostA.addAlias("hostA.custom.domain.com");
    final HostSpec hostB = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("hostB").port(HostSpec.NO_PORT).role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE)
        .build();
    hostB.addAlias("ip-10-10-10-10");
    hostB.addAlias("hostB.custom.domain.com");

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));

    target.allHosts = Arrays.asList(hostA, hostB);

    Set<String> aliases = new HashSet<>();
    aliases.add("ip-10-10-10-10");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(HostAvailability.AVAILABLE, hostA.getAvailability());
    assertEquals(HostAvailability.AVAILABLE, hostB.getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_UP));

    assertTrue(notifiedChanges.containsKey("hostB/"));
    EnumSet<NodeChangeOptions> hostBChanges = notifiedChanges.get("hostB/");
    assertEquals(2, hostBChanges.size());
    assertTrue(hostBChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostBChanges.contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  void testRefreshHostList_withCachedHostAvailability() throws SQLException {
    final List<HostSpec> newHostSpecs = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );
    final List<HostSpec> newHostSpecs2 = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );
    final List<HostSpec> expectedHostSpecs = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );
    final List<HostSpec> expectedHostSpecs2 = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostA/", HostAvailability.NOT_AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.NOT_AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    when(hostListProvider.refresh()).thenReturn(newHostSpecs);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    when(target.getHostListProvider()).thenReturn(hostListProvider);

    assertNotEquals(expectedHostSpecs, newHostSpecs);
    target.refreshHostList();
    assertEquals(expectedHostSpecs, newHostSpecs);

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    target.refreshHostList();
    assertEquals(expectedHostSpecs2, newHostSpecs);
  }

  @Test
  void testForceRefreshHostList_withCachedHostAvailability() throws SQLException, TimeoutException {
    final List<HostSpec> newHostSpecs = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );
    final List<HostSpec> expectedHostSpecs = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );
    final List<HostSpec> expectedHostSpecs2 = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostA").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.NOT_AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostB").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("hostC").port(HostSpec.NO_PORT)
            .role(HostRole.READER).availability(HostAvailability.AVAILABLE).build()
    );

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostA/", HostAvailability.NOT_AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.NOT_AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    when(hostListProvider.forceRefresh(anyBoolean(), anyLong())).thenReturn(newHostSpecs);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    when(target.getHostListProvider()).thenReturn(hostListProvider);

    assertNotEquals(expectedHostSpecs, newHostSpecs);
    target.forceRefreshHostList();
    assertEquals(expectedHostSpecs, newHostSpecs);

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.AVAILABLE,
        PluginServiceImpl.DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO);
    target.forceRefreshHostList();
    assertEquals(expectedHostSpecs2, newHostSpecs);
  }

  @Test
  void testIdentifyConnectionWithNoAliases() throws SQLException {
    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = Mockito.mock(MysqlDialect.class);
    target.dialect = dialect;
    doReturn(null).when(dialect).getHostId(newConnection);

    // When getHostId returns null, identifyConnection should throw SQLException
    assertThrows(SQLException.class, () -> target.identifyConnection(newConnection));
  }

  @Test
  void testIdentifyConnectionHostFoundAfterForceRefresh() throws SQLException, TimeoutException {
    final HostSpec expectedHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-b-1.xyz.us-east-2.rds.amazonaws.com")
        .hostId("instance-b-1")
        .port(HostSpec.NO_PORT)
        .role(HostRole.READER)
        .build();
    final List<HostSpec> staleTopology = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .hostId("instance-a-1")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build());
    final List<HostSpec> freshTopology = Collections.singletonList(expectedHost);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = spy(new AuroraPgDialect());
    target.dialect = dialect;
    doReturn(Pair.create("instance-b-1", "instance-b-1")).when(dialect).getHostId(newConnection);
    when(hostListProvider.refresh()).thenReturn(staleTopology);
    when(hostListProvider.forceRefresh()).thenReturn(freshTopology);

    final HostSpec actual = target.identifyConnection(newConnection);
    verify(hostListProvider).refresh();
    verify(hostListProvider).forceRefresh();
    assertEquals(expectedHost, actual);
  }

  @Test
  void testFillAliasesNonEmptyAliases() throws SQLException {
    final HostSpec oneAlias = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("foo")
        .build();
    oneAlias.addAlias(oneAlias.asAlias());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));

    assertEquals(1, oneAlias.getAliases().size());
    target.fillAliases(newConnection, oneAlias);
    // Fill aliases should return directly and no additional aliases should be added.
    assertEquals(1, oneAlias.getAliases().size());
  }

  @ParameterizedTest
  @MethodSource("fillAliasesDialects")
  void testFillAliasesWithInstanceEndpoint(Dialect dialectParam, String[] expectedInstanceAliases) throws SQLException {
    final HostSpec empty = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("foo").build();
    final HostSpec instanceHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .hostId("instance")
        .host("instance")
        .build();
    final List<HostSpec> topology = Collections.singletonList(instanceHost);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;
    
    final Dialect dialect = Mockito.mock(dialectParam.getClass());
    target.dialect = dialect;
    doReturn(true).doReturn(false).when(resultSet).next(); // Result set contains 1 row.
    when(resultSet.getString(eq(1))).thenReturn("ip");

    if (dialectParam instanceof AuroraPgDialect) {
      doReturn(Pair.create("instance", "instance")).when(dialect).getHostId(newConnection);
      when(hostListProvider.refresh()).thenReturn(topology);
    } else {
      // For non-Aurora dialects, getHostId should return a value that won't be found in topology
      // so identifyConnection returns null without throwing an exception
      doReturn(Pair.create("not-found", "not-found")).when(dialect).getHostId(newConnection);
      when(hostListProvider.refresh()).thenReturn(Collections.emptyList());
    }

    target.fillAliases(newConnection, empty);

    final String[] aliases = empty.getAliases().toArray(new String[] {});
    assertArrayEquals(expectedInstanceAliases, aliases);
  }

  private static Stream<Arguments> fillAliasesDialects() {
    return Stream.of(
        Arguments.of(new AuroraPgDialect(), new String[]{"instance", "foo", "ip"}),
        Arguments.of(new MysqlDialect(), new String[]{"foo", "ip"})
    );
  }

  @Test
  void testIdentifyConnectionWithInvalidNodeIdQuery() throws SQLException {
    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = spy(new MysqlDialect());
    target.dialect = dialect;
    when(dialect.getHostId(newConnection)).thenThrow(new SQLException("exception"));

    assertThrows(SQLException.class, () -> target.identifyConnection(newConnection));
  }

  @Test
  void testIdentifyConnectionNullTopology() throws SQLException, TimeoutException {
    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = spy(new AuroraPgDialect());
    target.dialect = dialect;
    doReturn(Pair.create("instance-1", "instance-1")).when(dialect).getHostId(newConnection);
    when(hostListProvider.refresh()).thenReturn(null);
    when(hostListProvider.forceRefresh()).thenReturn(null);

    assertNull(target.identifyConnection(newConnection));
  }

  @Test
  void testIdentifyConnectionHostNotInTopology() throws SQLException, TimeoutException {
    final List<HostSpec> cachedTopology = Collections.singletonList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
            .hostId("instance-a-1")
            .port(HostSpec.NO_PORT)
            .role(HostRole.WRITER)
            .build());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = spy(new AuroraPgDialect());
    target.dialect = dialect;
    doReturn(Pair.create("instance-2", "instance-2")).when(dialect).getHostId(newConnection);
    when(hostListProvider.refresh()).thenReturn(cachedTopology);
    when(hostListProvider.forceRefresh()).thenReturn(cachedTopology);

    assertNull(target.identifyConnection(newConnection));
  }

  @Test
  void testIdentifyConnectionHostInTopology() throws SQLException {
    final HostSpec expectedHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("instance-a-1.xyz.us-east-2.rds.amazonaws.com")
        .hostId("instance-a-1")
        .port(HostSpec.NO_PORT)
        .role(HostRole.WRITER)
        .build();
    final List<HostSpec> cachedTopology = Collections.singletonList(expectedHost);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(
            servicesContainer,
            new ExceptionManager(),
            PROPERTIES,
            URL,
            DRIVER_PROTOCOL,
            dialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            sessionStateService));
    target.hostListProvider = hostListProvider;

    final Dialect dialect = spy(new AuroraPgDialect());
    target.dialect = dialect;
    doReturn(Pair.create("instance-a-1", "instance-a-1")).when(dialect).getHostId(newConnection);
    when(hostListProvider.refresh()).thenReturn(cachedTopology);

    final HostSpec actual = target.identifyConnection(newConnection);
    assertEquals("instance-a-1.xyz.us-east-2.rds.amazonaws.com", Objects.requireNonNull(actual).getHost());
    assertEquals("instance-a-1", actual.getHostId());
  }
}
