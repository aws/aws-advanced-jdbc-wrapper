/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 *
 */

package software.aws.rds.jdbc.proxydriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PluginServiceImplTests {

  private static final Properties PROPERTIES = new Properties();
  private static final String URL = "url";
  private static final String DRIVER_PROTOCOL = "driverProtocol";

  @Test
  public void testOldConnectionNoSuggestion() throws SQLException {
    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.DISPOSE));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.PRESERVE));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(
            EnumSet.of(
                OldConnectionSuggestedAction.NO_OPINION,
                OldConnectionSuggestedAction.PRESERVE,
                OldConnectionSuggestedAction.DISPOSE));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges =
        ArgumentCaptor.forClass(EnumSet.class);
    ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin =
        ArgumentCaptor.forClass(ConnectionPlugin.class);
    when(pluginManager.notifyConnectionChanged(
            argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges =
        ArgumentCaptor.forClass(EnumSet.class);
    ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin =
        ArgumentCaptor.forClass(ConnectionPlugin.class);
    when(pluginManager.notifyConnectionChanged(
            argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges =
        ArgumentCaptor.forClass(EnumSet.class);
    ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin =
        ArgumentCaptor.forClass(ConnectionPlugin.class);
    when(pluginManager.notifyConnectionChanged(
            argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

    Connection newConnection = mock(Connection.class);

    PluginServiceImpl target =
        spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.currentConnection = oldConnection;
    target.currentHostSpec =
        new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE);

    target.setCurrentConnection(
        newConnection, new HostSpec("old-host", 1000, HostRole.READER, HostAvailability.AVAILABLE));

    assertNull(argumentSkipPlugin.getValue());
    assertTrue(argumentChanges.getValue().contains(NodeChangeOptions.NODE_CHANGED));
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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    @SuppressWarnings("unchecked") ArgumentCaptor<EnumSet<NodeChangeOptions>> argumentChanges = ArgumentCaptor.forClass(EnumSet.class);
    ArgumentCaptor<ConnectionPlugin> argumentSkipPlugin = ArgumentCaptor.forClass(ConnectionPlugin.class);
    when(pluginManager.notifyConnectionChanged(argumentChanges.capture(), argumentSkipPlugin.capture()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection oldConnection = mock(Connection.class);
    when(oldConnection.isClosed()).thenReturn(false);

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

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    ArgumentCaptor<Map<String, EnumSet<NodeChangeOptions>>> argumentChanges = ArgumentCaptor.forClass(Map.class);
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChanges.capture());

    HostListProvider hostListProvider = mock(HostListProvider.class);
    when(hostListProvider.refresh()).thenReturn(Arrays.asList(new HostSpec("hostA")));

    PluginServiceImpl target = spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = new ArrayList<>();
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostA", target.getHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChanges.getValue();
    assertTrue(notifiedChanges.containsKey("hostA"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA");
    assertEquals(1, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_ADDED));
  }

  @Test
  public void testSetNodeListDeleted() throws SQLException {

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    ArgumentCaptor<Map<String, EnumSet<NodeChangeOptions>>> argumentChanges = ArgumentCaptor.forClass(Map.class);
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChanges.capture());

    HostListProvider hostListProvider = mock(HostListProvider.class);
    when(hostListProvider.refresh()).thenReturn(Arrays.asList(new HostSpec("hostB")));

    PluginServiceImpl target = spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Arrays.asList(new HostSpec("hostA"), new HostSpec("hostB"));
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostB", target.getHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChanges.getValue();
    assertTrue(notifiedChanges.containsKey("hostA"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA");
    assertEquals(1, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_DELETED));
  }

  @Test
  public void testSetNodeListChanged() throws SQLException {

    ConnectionPluginManager pluginManager = mock(ConnectionPluginManager.class);
    ArgumentCaptor<Map<String, EnumSet<NodeChangeOptions>>> argumentChanges = ArgumentCaptor.forClass(Map.class);
    doNothing().when(pluginManager).notifyNodeListChanged(argumentChanges.capture());

    HostListProvider hostListProvider = mock(HostListProvider.class);
    when(hostListProvider.refresh()).thenReturn(Arrays.asList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.READER)));

    PluginServiceImpl target = spy(new PluginServiceImpl(pluginManager, PROPERTIES, URL, DRIVER_PROTOCOL));
    target.hosts = Arrays.asList(new HostSpec("hostA", HostSpec.NO_PORT, HostRole.WRITER));
    target.hostListProvider = hostListProvider;

    target.refreshHostList();

    assertEquals(1, target.getHosts().size());
    assertEquals("hostA", target.getHosts().get(0).getHost());
    verify(pluginManager, times(1)).notifyNodeListChanged(any());

    Map<String, EnumSet<NodeChangeOptions>> notifiedChanges = argumentChanges.getValue();
    assertTrue(notifiedChanges.containsKey("hostA"));
    EnumSet<NodeChangeOptions> hostAChanges = notifiedChanges.get("hostA");
    assertEquals(2, hostAChanges.size());
    assertTrue(hostAChanges.contains(NodeChangeOptions.NODE_CHANGED));
    assertTrue(hostAChanges.contains(NodeChangeOptions.PROMOTED_TO_READER));
  }
}
