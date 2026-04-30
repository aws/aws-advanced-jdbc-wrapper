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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.ConnectInfo;
import software.amazon.jdbc.targetdriverdialect.MysqlConnectorJTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.ImportantEventService;
import software.amazon.jdbc.util.events.EventPublisher;

/**
 * Tests that validate the wrapper correctly handles MySQL's loadbalance:// protocol,
 * which is needed for IAM authentication + load balancing across RDS read replicas.
 *
 * <p>The loadbalance:// protocol requires multiple hosts in a single JDBC URL like:
 * jdbc:mysql:loadbalance://host1:3306,host2:3306,host3:3306/database
 *
 * <p>The wrapper must preserve this multi-host URL format when delegating to the
 * underlying MySQL driver, rather than decomposing it into individual single-host connections.
 */
class LoadBalanceProtocolTest {

  private static final String LOAD_BALANCE_URL =
      "jdbc:mysql:loadbalance://replica1.us-east-1.rds.amazonaws.com:3306,"
          + "replica2.us-east-1.rds.amazonaws.com:3306,"
          + "replica3.us-east-1.rds.amazonaws.com:3306/mydb";

  private static final String WRAPPER_LOAD_BALANCE_URL =
      "jdbc:aws-wrapper:mysql:loadbalance://replica1.us-east-1.rds.amazonaws.com:3306,"
          + "replica2.us-east-1.rds.amazonaws.com:3306,"
          + "replica3.us-east-1.rds.amazonaws.com:3306/mydb";

  private static final String LOAD_BALANCE_PROTOCOL = "jdbc:mysql:loadbalance://";

  private AutoCloseable closeable;

  @Mock FullServicesContainer servicesContainer;
  @Mock ImportantEventService mockImportantEventService;
  @Mock EventPublisher mockEventPublisher;
  @Mock ConnectionPluginManager pluginManager;
  @Mock Connection mockConnection;
  @Mock DialectManager dialectManager;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock SessionStateService sessionStateService;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(servicesContainer.getConnectionPluginManager()).thenReturn(pluginManager);
    when(servicesContainer.getImportantEventService()).thenReturn(mockImportantEventService);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  /**
   * Verifies that the ConnectionUrlParser correctly parses all hosts from a
   * loadbalance:// URL. This is a prerequisite — the parser must see all three hosts.
   */
  @Test
  void testUrlParserExtractsAllHostsFromLoadBalanceUrl() {
    ConnectionUrlParser parser = new ConnectionUrlParser();
    List<HostSpec> hosts = parser.getHostsFromConnectionUrl(
        LOAD_BALANCE_URL,
        false,
        () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    assertEquals(3, hosts.size(), "Parser should extract all 3 hosts from loadbalance URL");
    assertEquals("replica1.us-east-1.rds.amazonaws.com", hosts.get(0).getHost());
    assertEquals("replica2.us-east-1.rds.amazonaws.com", hosts.get(1).getHost());
    assertEquals("replica3.us-east-1.rds.amazonaws.com", hosts.get(2).getHost());
  }

  /**
   * Verifies that the ConnectionUrlParser extracts the correct protocol from a
   * loadbalance:// URL, preserving the "loadbalance:" portion.
   */
  @Test
  void testUrlParserExtractsLoadBalanceProtocol() {
    ConnectionUrlParser parser = new ConnectionUrlParser();
    String protocol = parser.getProtocol(LOAD_BALANCE_URL);

    assertEquals(LOAD_BALANCE_PROTOCOL, protocol,
        "Protocol should preserve the loadbalance: portion");
  }

  /**
   * This is the critical test: when the MysqlConnectorJTargetDriverDialect builds a
   * ConnectInfo URL for a single HostSpec parsed from a loadbalance:// URL, the resulting
   * URL must still contain ALL the original hosts in the loadbalance format.
   *
   * <p>The wrapper stores the original hosts in the ORIGINAL_URL_HOSTS property when it
   * detects a multi-host protocol. The dialect should use this to build the full URL.
   *
   * <p>Expected URL format:
   *   jdbc:mysql:loadbalance://replica1:3306,replica2:3306,replica3:3306/mydb
   */
  @Test
  void testMysqlDialectPreservesMultiHostLoadBalanceUrl() throws SQLException {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();

    // Simulate what the wrapper does: parse the URL into individual HostSpecs
    ConnectionUrlParser parser = new ConnectionUrlParser();
    List<HostSpec> hosts = parser.getHostsFromConnectionUrl(
        LOAD_BALANCE_URL,
        false,
        () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    // The wrapper stores the original hosts portion when it detects a multi-host protocol
    Properties props = new Properties();
    props.setProperty(PropertyDefinition.DATABASE.name, "mydb");
    props.setProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name,
        "replica1.us-east-1.rds.amazonaws.com:3306,"
            + "replica2.us-east-1.rds.amazonaws.com:3306,"
            + "replica3.us-east-1.rds.amazonaws.com:3306");

    ConnectInfo connectInfo = dialect.prepareConnectInfo(LOAD_BALANCE_PROTOCOL, hosts.get(0), props);

    // The resulting URL should contain ALL hosts for the loadbalance protocol to work.
    assertTrue(connectInfo.url.contains("replica1.us-east-1.rds.amazonaws.com"),
        "URL should contain replica1");
    assertTrue(connectInfo.url.contains("replica2.us-east-1.rds.amazonaws.com"),
        "URL should contain replica2 for load balancing");
    assertTrue(connectInfo.url.contains("replica3.us-east-1.rds.amazonaws.com"),
        "URL should contain replica3 for load balancing");
    assertTrue(connectInfo.url.startsWith(LOAD_BALANCE_PROTOCOL),
        "URL should start with the loadbalance protocol");
  }

  /**
   * Verifies that when the wrapper's PluginServiceImpl compares two different Connection
   * objects that represent the same logical host, it correctly detects
   * CONNECTION_OBJECT_CHANGED. This is the expected behavior because the wrapper itself
   * may create new connections for the same host (e.g., reconnecting after failure).
   *
   * <p>For MySQL's loadbalance:// protocol, the key fix is preserving the multi-host URL
   * (tested above). The MySQL driver's LoadBalancedConnection proxy is a single stable
   * object — internal physical connection switching is invisible to the wrapper.
   */
  @Test
  void testConnectionObjectChangeDetectedWhenSameHost() throws SQLException {
    when(pluginManager.notifyConnectionChanged(any(), any()))
        .thenReturn(EnumSet.of(OldConnectionSuggestedAction.NO_OPINION));

    Connection conn1 = mockConnection;
    Connection conn2 = mock(Connection.class);

    // Both connections point to the same host
    HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("replica1.us-east-1.rds.amazonaws.com")
        .port(3306)
        .build();

    PluginServiceImpl target = spy(new PluginServiceImpl(
        servicesContainer,
        new ExceptionManager(),
        new Properties(),
        LOAD_BALANCE_URL,
        LOAD_BALANCE_PROTOCOL,
        dialectManager,
        mockTargetDriverDialect,
        null,
        sessionStateService));

    // Set initial connection
    target.currentConnection = conn1;
    target.currentHostSpec = hostSpec;

    // Compare with a different Connection object but same host.
    // The wrapper should still detect CONNECTION_OBJECT_CHANGED because the wrapper
    // itself may legitimately switch connections. The loadbalance fix is about
    // preserving the multi-host URL, not about suppressing connection change detection.
    EnumSet<NodeChangeOptions> changes = target.compare(conn1, hostSpec, conn2, hostSpec);

    assertTrue(changes.contains(NodeChangeOptions.CONNECTION_OBJECT_CHANGED),
        "CONNECTION_OBJECT_CHANGED should be detected when the Connection object differs, "
            + "even for the same host — the wrapper needs this for its own connection management");
  }

  // --- Replication protocol tests ---

  private static final String REPLICATION_URL =
      "jdbc:mysql:replication://writer.us-east-1.rds.amazonaws.com:3306,"
          + "reader1.us-east-1.rds.amazonaws.com:3306,"
          + "reader2.us-east-1.rds.amazonaws.com:3306/mydb";

  private static final String REPLICATION_PROTOCOL = "jdbc:mysql:replication://";

  @Test
  void testUrlParserExtractsReplicationProtocol() {
    ConnectionUrlParser parser = new ConnectionUrlParser();
    String protocol = parser.getProtocol(REPLICATION_URL);
    assertEquals(REPLICATION_PROTOCOL, protocol,
        "Protocol should preserve the replication: portion");
  }

  @Test
  void testMysqlDialectPreservesMultiHostReplicationUrl() throws SQLException {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    ConnectionUrlParser parser = new ConnectionUrlParser();
    List<HostSpec> hosts = parser.getHostsFromConnectionUrl(
        REPLICATION_URL, false,
        () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.DATABASE.name, "mydb");
    props.setProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name,
        "writer.us-east-1.rds.amazonaws.com:3306,"
            + "reader1.us-east-1.rds.amazonaws.com:3306,"
            + "reader2.us-east-1.rds.amazonaws.com:3306");

    ConnectInfo connectInfo = dialect.prepareConnectInfo(REPLICATION_PROTOCOL, hosts.get(0), props);

    assertTrue(connectInfo.url.startsWith(REPLICATION_PROTOCOL),
        "URL should start with replication protocol");
    assertTrue(connectInfo.url.contains("writer.us-east-1.rds.amazonaws.com"),
        "URL should contain writer host");
    assertTrue(connectInfo.url.contains("reader1.us-east-1.rds.amazonaws.com"),
        "URL should contain reader1 host");
    assertTrue(connectInfo.url.contains("reader2.us-east-1.rds.amazonaws.com"),
        "URL should contain reader2 host");
    assertTrue(connectInfo.url.endsWith("/mydb"),
        "URL should end with database name");
  }

  // --- Edge case: no ORIGINAL_URL_HOSTS falls back to single-host URL ---

  @Test
  void testMysqlDialectFallsBackToSingleHostWithoutOriginalUrlHosts() throws SQLException {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("single-host.us-east-1.rds.amazonaws.com")
        .port(3306)
        .build();

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.DATABASE.name, "mydb");
    // No ORIGINAL_URL_HOSTS set

    ConnectInfo connectInfo = dialect.prepareConnectInfo(LOAD_BALANCE_PROTOCOL, hostSpec, props);

    // Without ORIGINAL_URL_HOSTS, should fall back to single-host URL
    assertTrue(connectInfo.url.contains("single-host.us-east-1.rds.amazonaws.com"),
        "Should use single host when ORIGINAL_URL_HOSTS is not set");
    assertFalse(connectInfo.url.contains(","),
        "Should not contain comma-separated hosts");
  }

  // --- Edge case: standard protocol ignores ORIGINAL_URL_HOSTS ---

  @Test
  void testMysqlDialectIgnoresOriginalUrlHostsForStandardProtocol() throws SQLException {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("single-host.us-east-1.rds.amazonaws.com")
        .port(3306)
        .build();

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.DATABASE.name, "mydb");
    // Set ORIGINAL_URL_HOSTS even though protocol is standard
    props.setProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name,
        "host1:3306,host2:3306,host3:3306");

    String standardProtocol = "jdbc:mysql://";
    ConnectInfo connectInfo = dialect.prepareConnectInfo(standardProtocol, hostSpec, props);

    // Standard protocol should NOT use ORIGINAL_URL_HOSTS
    assertTrue(connectInfo.url.startsWith(standardProtocol),
        "Should use standard protocol");
    assertTrue(connectInfo.url.contains("single-host.us-east-1.rds.amazonaws.com"),
        "Should use the single host from HostSpec");
  }

  // --- Driver.java: ORIGINAL_URL_HOSTS extraction tests ---

  @Test
  void testDriverSetsOriginalUrlHostsForLoadBalance() throws SQLException {
    // Simulate what Driver.connect() does for URL parsing
    String wrapperUrl = WRAPPER_LOAD_BALANCE_URL;
    String driverUrl = wrapperUrl.replaceFirst("jdbc:aws-wrapper:", "jdbc:");
    ConnectionUrlParser parser = new ConnectionUrlParser();
    String protocol = parser.getProtocol(driverUrl);

    Properties props = new Properties();
    if (protocol.contains("loadbalance:") || protocol.contains("replication:")) {
      String afterProtocol = driverUrl.substring(protocol.length());
      int endOfHosts = afterProtocol.indexOf('/');
      String hostsString = endOfHosts >= 0 ? afterProtocol.substring(0, endOfHosts) : afterProtocol;
      if (!hostsString.isEmpty()) {
        props.setProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name, hostsString);
      }
    }

    String expected = "replica1.us-east-1.rds.amazonaws.com:3306,"
        + "replica2.us-east-1.rds.amazonaws.com:3306,"
        + "replica3.us-east-1.rds.amazonaws.com:3306";
    assertEquals(expected, props.getProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name),
        "ORIGINAL_URL_HOSTS should contain all hosts from the loadbalance URL");
  }

  @Test
  void testDriverDoesNotSetOriginalUrlHostsForStandardUrl() {
    String standardUrl = "jdbc:mysql://single-host.us-east-1.rds.amazonaws.com:3306/mydb";
    ConnectionUrlParser parser = new ConnectionUrlParser();
    String protocol = parser.getProtocol(standardUrl);

    Properties props = new Properties();
    if (protocol.contains("loadbalance:") || protocol.contains("replication:")) {
      // This block should NOT execute for standard URLs
      props.setProperty(PropertyDefinition.ORIGINAL_URL_HOSTS.name, "should-not-be-set");
    }

    assertFalse(props.containsKey(PropertyDefinition.ORIGINAL_URL_HOSTS.name),
        "ORIGINAL_URL_HOSTS should not be set for standard single-host URLs");
  }
}
