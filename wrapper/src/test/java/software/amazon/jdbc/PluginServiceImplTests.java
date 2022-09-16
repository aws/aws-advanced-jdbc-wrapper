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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PluginServiceImplTests {

  private static final Properties PROPERTIES = new Properties();
  private static final String URL = "url";
  private static final String DRIVER_PROTOCOL = "driverProtocol";
  private AutoCloseable closeable;

  @Mock ConnectionPluginManager pluginManager;
  @Mock Connection newConnection;
  @Mock Connection oldConnection;
  @Mock HostListProvider hostListProvider;

  @Captor ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges;
  @Captor ArgumentCaptor<Map<String, EnumSet<NodeChangeOptions>>> argumentChangesMap;
  @Captor ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(oldConnection.isClosed()).thenReturn(false);
    PluginServiceImpl.hostAvailabilityExpiringCache.clear();
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    PluginServiceImpl.hostAvailabilityExpiringCache.clear();
  }

  @Test
  public void testOldConnectionNoSuggestion() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    PluginServiceImpl target =
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpec("old-host");

    target.setCurrentConnection(newConnection, new HostSpec("new-host"));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpec("old-host");

    target.setCurrentConnection(newConnection, new HostSpec("new-host"));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpec("old-host");

    target.setCurrentConnection(newConnection, new HostSpec("new-host"));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec = new HostSpec("old-host");

    target.setCurrentConnection(newConnection, new HostSpec("new-host"));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpec("old-host", 1000, HostRole.WRITER, HostAvailability.AVAILABLE);

    target.setCurrentConnection(
        newConnection,
        new HostSpec("new-host", 2000, HostRole.READER, HostAvailability.NOT_AVAILABLE));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.NOT_AVAILABLE);

    target.setCurrentConnection(
        newConnection, new HostSpec("old-host", 1000, HostRole.WRITER, HostAvailability.AVAILABLE));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE);

    target.setCurrentConnection(
        newConnection, new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE));

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
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE);

    target.setCurrentConnection(
        oldConnection, new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE));

    verify(pluginManager, times(0)).notifyConnectionChanged(any(), any());
  }

  @Test
  public void testSetNodeListAdded() throws SQLException {

    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    when(hostListProvider.refresh()).thenReturn(Collections.singletonList(new HostSpec("hostA")));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = new ArrayList<>();
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostA", target.getHosts().get(0).getHost());
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

    when(hostListProvider.refresh()).thenReturn(Collections.singletonList(new HostSpec("hostB")));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Arrays.asList(new HostSpec("hostA"), new HostSpec("hostB"));
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostB", target.getHosts().get(0).getHost());
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
        Collections.singletonList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER)));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Collections.singletonList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.WRITER));
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostA", target.getHosts().get(0).getHost());
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
        Collections.singletonList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER)));

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Collections.singletonList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER));
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostA", target.getHosts().get(0).getHost());
    verify(pluginManager, times(0)).notifyNodeListChanged(any());
  }

  @Test
  public void testNodeAvailabilityNotChanged() {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Collections.singletonList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(1, target.getHosts().size());
    assertEquals(HostAvailability.AVAILABLE, target.getHosts().get(0).getAvailability());
    verify(pluginManager, never()).notifyNodeListChanged(any());
  }

  @Test
  public void testNodeAvailabilityChanged_WentDown() {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Collections.singletonList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.NOT_AVAILABLE);

    assertEquals(1, target.getHosts().size());
    assertEquals(HostAvailability.NOT_AVAILABLE, target.getHosts().get(0).getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_DOWN));
  }

  @Test
  public void testNodeAvailabilityChanged_WentUp() {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Collections.singletonList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE));

    Set<String> aliases = new HashSet<>();
    aliases.add("hostA");
    target.setAvailability(aliases, HostAvailability.AVAILABLE);

    assertEquals(1, target.getHosts().size());
    assertEquals(HostAvailability.AVAILABLE, target.getHosts().get(0).getAvailability());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChangesMap.getValue();
    assertTrue(notifiedChanges.containsKey("hostA/"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA/");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.WENT_UP));
  }

  @Test
  public void testNodeAvailabilityChanged_WentUp_ByAlias() {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    final HostSpec hostA = new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE);
    hostA.addAlias("ip-10-10-10-10");
    hostA.addAlias("hostA.custom.domain.com");
    final HostSpec hostB = new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE);
    hostB.addAlias("ip-10-10-10-10");
    hostB.addAlias("hostB.custom.domain.com");

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));

    target.hosts = Arrays.asList(hostA, hostB);

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
  public void testNodeAvailabilityChanged_WentUp_MultipleHostsByAlias() {
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChangesMap.capture());

    final HostSpec hostA = new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE);
    hostA.addAlias("ip-10-10-10-10");
    hostA.addAlias("hostA.custom.domain.com");
    final HostSpec hostB = new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE);
    hostB.addAlias("ip-10-10-10-10");
    hostB.addAlias("hostB.custom.domain.com");

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));

    target.hosts = Arrays.asList(hostA, hostB);

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
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE)
    );
    final List<HostSpec> expectedHostSpecs = Arrays.asList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));
    final List<HostSpec> expectedHostSpecs2 = Arrays.asList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostA/", HostAvailability.NOT_AVAILABLE);
    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.NOT_AVAILABLE);
    when(hostListProvider.refresh()).thenReturn(newHostSpecs);
    when(hostListProvider.refresh(newConnection)).thenReturn(newHostSpecs);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    when(target.getHostListProvider()).thenReturn(hostListProvider);

    assertNotEquals(expectedHostSpecs, newHostSpecs);
    target.refreshHostList();
    assertEquals(expectedHostSpecs, newHostSpecs);

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.AVAILABLE);
    target.refreshHostList(newConnection);
    assertEquals(expectedHostSpecs2, newHostSpecs);
  }

  @Test
  void testForceRefreshHostList_withCachedHostAvailability() throws SQLException {
    final List<HostSpec> newHostSpecs = Arrays.asList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE)
    );
    final List<HostSpec> expectedHostSpecs = Arrays.asList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));
    final List<HostSpec> expectedHostSpecs2 = Arrays.asList(
        new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER, HostAvailability.NOT_AVAILABLE),
        new HostSpec("hostB", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE),
        new HostSpec("hostC", HostSpec.NO_PORT, HostRole.READER, HostAvailability.AVAILABLE));

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostA/", HostAvailability.NOT_AVAILABLE);
    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.NOT_AVAILABLE);
    when(hostListProvider.forceRefresh()).thenReturn(newHostSpecs);
    when(hostListProvider.forceRefresh(newConnection)).thenReturn(newHostSpecs);

    PluginServiceImpl target = spy(
        new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    when(target.getHostListProvider()).thenReturn(hostListProvider);

    assertNotEquals(expectedHostSpecs, newHostSpecs);
    target.forceRefreshHostList();
    assertEquals(expectedHostSpecs, newHostSpecs);

    PluginServiceImpl.hostAvailabilityExpiringCache.put("hostB/", HostAvailability.AVAILABLE);
    target.forceRefreshHostList(newConnection);
    assertEquals(expectedHostSpecs2, newHostSpecs);
  }
}
