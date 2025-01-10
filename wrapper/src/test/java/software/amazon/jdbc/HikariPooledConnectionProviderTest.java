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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.ConnectInfo;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.SlidingExpirationCache;

class HikariPooledConnectionProviderTest {
  @Mock Connection mockConnection;
  @Mock HikariDataSource mockDataSource;
  @Mock HostSpec mockHostSpec;
  @Mock HikariConfig mockConfig;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock HikariDataSource dsWithNoConnections;
  @Mock HikariDataSource dsWith1Connection;
  @Mock HikariDataSource dsWith2Connections;
  @Mock HikariPoolMXBean mxBeanWithNoConnections;
  @Mock HikariPoolMXBean mxBeanWith1Connection;
  @Mock HikariPoolMXBean mxBeanWith2Connections;
  private static final String LEAST_CONNECTIONS = "leastConnections";
  private final int port = 5432;
  private final String user1 = "user1";
  private final String user2 = "user2";
  private final String password = "password";
  private final String db = "mydb";
  private final String writerUrlNoConnections = "writerWithNoConnections.XYZ.us-east-1.rds.amazonaws.com";
  private final HostSpec writerHostNoConnections = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(writerUrlNoConnections).port(port).role(HostRole.WRITER).build();
  private final String readerUrl1Connection = "readerWith1connection.XYZ.us-east-1.rds.amazonaws.com";
  private final HostSpec readerHost1Connection = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(readerUrl1Connection).port(port).role(HostRole.READER).build();
  private final String readerUrl2Connection = "readerWith2connection.XYZ.us-east-1.rds.amazonaws.com";
  private final HostSpec readerHost2Connection = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(readerUrl2Connection).port(port).role(HostRole.READER).build();
  private final String protocol = "protocol://";

  private final Properties defaultProps = getDefaultProps();
  private final List<HostSpec> testHosts = getTestHosts();
  private HikariPooledConnectionProvider provider;

  private AutoCloseable closeable;

  private List<HostSpec> getTestHosts() {
    List<HostSpec> hosts = new ArrayList<>();
    hosts.add(writerHostNoConnections);
    hosts.add(readerHost1Connection);
    hosts.add(readerHost2Connection);
    return hosts;
  }

  private Properties getDefaultProps() {
    Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, user1);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    props.setProperty(PropertyDefinition.DATABASE.name, db);
    return props;
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.isValid(any(Integer.class))).thenReturn(true);
    when(dsWithNoConnections.getHikariPoolMXBean()).thenReturn(mxBeanWithNoConnections);
    when(mxBeanWithNoConnections.getActiveConnections()).thenReturn(0);
    when(dsWith1Connection.getHikariPoolMXBean()).thenReturn(mxBeanWith1Connection);
    when(mxBeanWith1Connection.getActiveConnections()).thenReturn(1);
    when(dsWith2Connections.getHikariPoolMXBean()).thenReturn(mxBeanWith2Connections);
    when(mxBeanWith2Connections.getActiveConnections()).thenReturn(2);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (provider != null) {
      provider.releaseResources();
    }
    closeable.close();
  }

  @Test
  void testConnectWithDefaultMapping() throws SQLException {
    when(mockHostSpec.getUrl()).thenReturn("url");
    final Set<String> expectedUrls = new HashSet<>(Collections.singletonList("url"));
    final Set<Pair> expectedKeys = new HashSet<>(
        Collections.singletonList(Pair.create("url", user1)));

    provider = spy(new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig));

    doReturn(mockDataSource).when(provider).createHikariDataSource(any(), any(), any(), any());
    doReturn(new ConnectInfo("url", new Properties()))
        .when(mockTargetDriverDialect).prepareConnectInfo(anyString(), any(), any());

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, user1);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    try (Connection conn = provider.connect(protocol, mockDialect, mockTargetDriverDialect, mockHostSpec, props)) {
      assertEquals(mockConnection, conn);
      assertEquals(1, provider.getHostCount());
      final Set<String> hosts = provider.getHosts();
      assertEquals(expectedUrls, hosts);
      final Set<Pair> keys = provider.getKeys();
      assertEquals(expectedKeys, keys);
    }
  }

  @Test
  void testConnectWithCustomMapping() throws SQLException {
    when(mockHostSpec.getUrl()).thenReturn("url");
    final Set<Pair> expectedKeys = new HashSet<>(
        Collections.singletonList(Pair.create("url", "url+someUniqueKey")));

    provider = spy(new HikariPooledConnectionProvider(
        (hostSpec, properties) -> mockConfig,
        (hostSpec, properties) -> hostSpec.getUrl() + "+someUniqueKey"));

    doReturn(mockDataSource).when(provider).createHikariDataSource(any(), any(), any(), any());

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, user1);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    try (Connection conn = provider.connect(protocol, mockDialect, mockTargetDriverDialect, mockHostSpec, props)) {
      assertEquals(mockConnection, conn);
      assertEquals(1, provider.getHostCount());
      final Set<Pair> keys = provider.getKeys();
      assertEquals(expectedKeys, keys);
    }
  }

  @Test
  public void testAcceptsUrl() {
    final String clusterUrl = "my-database.cluster-XYZ.us-east-1.rds.amazonaws.com";
    provider = new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig);

    assertTrue(
        provider.acceptsUrl(protocol,
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(readerUrl2Connection).build(),
            defaultProps));
    assertFalse(
        provider.acceptsUrl(protocol,
            new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(clusterUrl).build(), defaultProps));
  }

  @Test
  public void testRandomStrategy() throws SQLException {
    provider = new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig);
    provider.setDatabasePools(getTestPoolMap());

    HostSpec selectedHost = provider.getHostSpecByStrategy(testHosts, HostRole.READER, "random", defaultProps);
    assertTrue(readerUrl1Connection.equals(selectedHost.getHost())
        || readerUrl2Connection.equals(selectedHost.getHost()));
  }

  @Test
  public void testLeastConnectionsStrategy() throws SQLException {
    provider = new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig);
    provider.setDatabasePools(getTestPoolMap());

    HostSpec selectedHost = provider.getHostSpecByStrategy(testHosts, HostRole.READER, LEAST_CONNECTIONS, defaultProps);
    // Other reader has 2 connections
    assertEquals(readerUrl1Connection, selectedHost.getHost());
  }

  private SlidingExpirationCache<Pair, AutoCloseable> getTestPoolMap() {
    SlidingExpirationCache<Pair, AutoCloseable> map = new SlidingExpirationCache<>();
    map.computeIfAbsent(Pair.create(readerHost2Connection.getUrl(), user1),
        (key) -> dsWith1Connection, TimeUnit.MINUTES.toNanos(10));
    map.computeIfAbsent(Pair.create(readerHost2Connection.getUrl(), user2),
        (key) -> dsWith1Connection, TimeUnit.MINUTES.toNanos(10));
    map.computeIfAbsent(Pair.create(readerHost1Connection.getUrl(), user1),
        (key) -> dsWith1Connection, TimeUnit.MINUTES.toNanos(10));
    return map;
  }

  @Test
  public void testConfigurePool() throws SQLException {
    provider = new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig);
    final String expectedJdbcUrl =
        protocol + readerHost1Connection.getUrl() + db + "?database=" + db;
    doReturn(new ConnectInfo(protocol + readerHost1Connection.getUrl() + db, defaultProps))
        .when(mockTargetDriverDialect).prepareConnectInfo(anyString(), any(), any());

    provider.configurePool(mockConfig, protocol, readerHost1Connection, defaultProps, mockTargetDriverDialect);
    verify(mockConfig).setJdbcUrl(expectedJdbcUrl);
    verify(mockConfig).setUsername(user1);
    verify(mockConfig).setPassword(password);
  }

  @Test
  public void testConnectToDeletedInstance() throws SQLException {
    provider = spy(new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig));

    doReturn(mockDataSource).when(provider)
        .createHikariDataSource(eq(protocol), eq(readerHost1Connection), eq(defaultProps), eq(mockTargetDriverDialect));
    when(mockDataSource.getConnection()).thenThrow(SQLException.class);

    assertThrows(SQLException.class,
        () -> provider.connect(protocol, mockDialect, mockTargetDriverDialect, readerHost1Connection, defaultProps));
  }
}
