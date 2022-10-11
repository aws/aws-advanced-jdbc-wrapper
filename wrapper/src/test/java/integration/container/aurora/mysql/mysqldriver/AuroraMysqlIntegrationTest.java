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

package integration.container.aurora.mysql.mysqldriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import eu.rekawek.toxiproxy.Proxy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class AuroraMysqlIntegrationTest extends MysqlAuroraMysqlBaseTest {

  protected String currWriter;
  protected String currReader;

  protected static String buildConnectionString(
      String connStringPrefix,
      String host,
      String port,
      String databaseName) {
    return connStringPrefix + host + ":" + port + "/" + databaseName;
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        // missing username
        Arguments.of(buildConnectionString(
                DB_CONN_STR_PREFIX,
                MYSQL_INSTANCE_1_URL,
                String.valueOf(AURORA_MYSQL_PORT),
                AURORA_MYSQL_DB),
            "",
            AURORA_MYSQL_PASSWORD),
        // missing password
        Arguments.of(buildConnectionString(
                DB_CONN_STR_PREFIX,
                MYSQL_INSTANCE_1_URL,
                String.valueOf(AURORA_MYSQL_PORT),
                AURORA_MYSQL_DB),
            AURORA_MYSQL_USERNAME,
            ""),
        // missing connection prefix
        Arguments.of(buildConnectionString(
                "",
                MYSQL_INSTANCE_1_URL,
                String.valueOf(AURORA_MYSQL_PORT),
                AURORA_MYSQL_DB),
            AURORA_MYSQL_USERNAME,
            AURORA_MYSQL_PASSWORD),
        // incorrect database name
        Arguments.of(buildConnectionString(DB_CONN_STR_PREFIX,
                MYSQL_INSTANCE_1_URL,
                String.valueOf(AURORA_MYSQL_PORT),
                "failedDatabaseNameTest"),
            AURORA_MYSQL_USERNAME,
            AURORA_MYSQL_PASSWORD)
    );
  }

  @ParameterizedTest(name = "test_ConnectionString")
  @MethodSource("generateConnectionString")
  public void test_ConnectionString(String connStr, int port) throws SQLException {
    final Connection conn = connectToInstance(connStr, port);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  private static Stream<Arguments> generateConnectionString() {
    return Stream.of(
        Arguments.of(MYSQL_INSTANCE_1_URL, AURORA_MYSQL_PORT),
        Arguments.of(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_CLUSTER_URL, AURORA_MYSQL_PORT),
        Arguments.of(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL, AURORA_MYSQL_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)
    );
  }

  @Test
  public void test_ValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));

    containerHelper.disableConnectivity(proxyInstance_1);

    assertFalse(conn.isValid(5));

    containerHelper.enableConnectivity(proxyInstance_1);

    conn.close();
  }

  @Test
  public void test_ConnectWhenNetworkDown() throws SQLException, IOException {
    containerHelper.disableConnectivity(proxyInstance_1);

    assertThrows(Exception.class, () -> {
      // expected to fail since communication is cut
      connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    });

    containerHelper.enableConnectivity(proxyInstance_1);

    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @Test
  public void test_LostConnectionToWriter() throws SQLException, IOException {

    final String initialWriterId = instanceIDs[0];

    final Properties props = initDefaultProxiedProps();
    props.setProperty("failoverTimeoutMs", "60000");

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(
                 initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
                 MYSQL_PROXY_PORT,
                 props)) {
      // Get writer
      currWriter = queryInstanceId(testConnection);

      // Put cluster & writer down
      final Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currWriter));
      }
      containerHelper.disableConnectivity(proxyCluster);

      assertFirstQueryThrows(testConnection, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState());

    } finally {
      final Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        containerHelper.enableConnectivity(proxyInstance);
      }
      containerHelper.enableConnectivity(proxyCluster);
    }
  }

  @Test
  public void test_LostConnectionToAllReaders() throws SQLException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(
        currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    }

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(
        anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);
      assertNotEquals(currWriter, currReader);

      // Put all but writer down
      proxyMap.forEach((instance, proxy) -> {
        if (!instance.equalsIgnoreCase(currWriter)) {
          try {
            containerHelper.disableConnectivity(proxy);
          } catch (IOException e) {
            fail("Toxics were already set, should not happen");
          }
        }
      });

      assertFirstQueryThrows(testConnection, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      final String newReader = queryInstanceId(testConnection);
      assertEquals(currWriter, newReader);
    } finally {
      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });
    }
  }

  @Test
  public void test_LostConnectionToReaderInstance() throws SQLException, IOException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(
        currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    } catch (SQLException e) {
      fail(e);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(
        anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      assertFirstQueryThrows(testConnection, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      final String newInstance = queryInstanceId(testConnection);
      assertEquals(currWriter, newInstance);
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
    }
  }

  @Test
  public void test_LostConnectionReadOnly() throws SQLException, IOException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(
        currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(
        anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);

      testConnection.setReadOnly(true);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      assertFirstQueryThrows(testConnection, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      final String newInstance = queryInstanceId(testConnection);
      assertNotEquals(currWriter, newInstance);
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
    }
  }

  @Test
  void test_ValidInvalidValidConnections() throws SQLException {
    final Properties validProp = initDefaultProps();
    validProp.setProperty(PropertyDefinition.USER.name, AURORA_MYSQL_USERNAME);
    validProp.setProperty(PropertyDefinition.PASSWORD.name, AURORA_MYSQL_PASSWORD);
    final Connection validConn = connectToInstance(MYSQL_INSTANCE_1_URL, AURORA_MYSQL_PORT, validProp);
    validConn.close();

    final Properties invalidProp = initDefaultProps();
    invalidProp.setProperty(PropertyDefinition.USER.name, "INVALID_" + AURORA_MYSQL_USERNAME);
    invalidProp.setProperty(PropertyDefinition.PASSWORD.name, "INVALID_" + AURORA_MYSQL_PASSWORD);
    assertThrows(
        SQLException.class,
        () -> connectToInstance(MYSQL_INSTANCE_1_URL, AURORA_MYSQL_PORT, invalidProp)
    );

    final Connection validConn2 = connectToInstance(MYSQL_INSTANCE_1_URL, AURORA_MYSQL_PORT, validProp);
    validConn2.close();
  }

  /** Current writer dies, no available reader instance, connection fails. */
  @Test
  public void test_writerConnectionFailsDueToNoReader()
      throws SQLException, IOException {

    final String currentWriterId = instanceIDs[0];

    Properties props = initDefaultProxiedProps();
    props.setProperty("failoverTimeoutMs", "10000");
    try (Connection conn = connectToInstance(
        currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT,
        props)) {
      // Put all but writer down first
      proxyMap.forEach((instance, proxy) -> {
        if (!instance.equalsIgnoreCase(currentWriterId)) {
          try {
            containerHelper.disableConnectivity(proxy);
          } catch (IOException e) {
            fail("Toxics were already set, should not happen");
          }
        }
      });

      // Crash the writer now
      final Proxy proxyInstance = proxyMap.get(currentWriterId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currentWriterId));
      }

      // All instances should be down, assert exception thrown with SQLState code 08001
      // (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
      assertFirstQueryThrows(conn, SqlState.CONNECTION_UNABLE_TO_CONNECT.getState());
    } finally {
      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });
    }
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, IOException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");
    final String readerNode = instanceIDs[1];

    Properties props = initDefaultProxiedProps();
    try (Connection conn = connectToInstance(
        readerNode + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT,
        props)) {
      // First kill all reader instances except one
      for (int i = 1; i < clusterSize - 1; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
      // instance (Instance1).
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(
          currentConnectionId.equals(instanceIDs[clusterSize - 1]) // Last reader
              || currentConnectionId.equals(instanceIDs[0])); // Writer
    }
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance or failover to a writer.
   */
  @Test
  public void test_failoverBackToThePreviouslyDownReaderOrWriter()
      throws Exception {
    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");

    final String writerInstanceId = instanceIDs[0];
    final String firstReaderInstanceId = instanceIDs[1];

    // Connect to reader (Instance2).
    Properties props = initDefaultProxiedProps();
    props.setProperty(AuroraHostListProvider.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.name, "2000");
    try (Connection conn = connectToInstance(
        firstReaderInstanceId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX,
        MYSQL_PROXY_PORT,
        props)) {
      conn.setReadOnly(true);

      // Start crashing reader (Instance2).
      Proxy proxyInstance = proxyMap.get(firstReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to another reader instance.
      final String secondReaderInstanceId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(secondReaderInstanceId));
      assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

      // Crash the second reader instance.
      proxyInstance = proxyMap.get(secondReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      // Assert that we are connected to the third reader instance.
      final String thirdReaderInstanceId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(thirdReaderInstanceId));
      assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
      assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

      // Grab the id of the fourth reader instance.
      final HashSet<String> readerInstanceIds = new HashSet<>(Arrays.asList(instanceIDs));
      readerInstanceIds.remove(writerInstanceId); // Writer
      readerInstanceIds.remove(firstReaderInstanceId);
      readerInstanceIds.remove(secondReaderInstanceId);
      readerInstanceIds.remove(thirdReaderInstanceId);

      final String fourthInstanceId =
          readerInstanceIds.stream().findFirst().orElseThrow(() -> new Exception("Empty instance Id"));

      // Crash the fourth reader instance.
      proxyInstance = proxyMap.get(fourthInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      // Stop crashing the first and second.
      proxyInstance = proxyMap.get(firstReaderInstanceId);
      containerHelper.enableConnectivity(proxyInstance);

      proxyInstance = proxyMap.get(secondReaderInstanceId);
      containerHelper.enableConnectivity(proxyInstance);

      final String currentInstanceId = queryInstanceId(conn);
      assertEquals(thirdReaderInstanceId, currentInstanceId);

      // Start crashing the third instance.
      proxyInstance = proxyMap.get(thirdReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, SqlState.COMMUNICATION_LINK_CHANGED.getState());

      final String lastInstanceId = queryInstanceId(conn);

      assertTrue(
          firstReaderInstanceId.equals(lastInstanceId)
              || secondReaderInstanceId.equals(lastInstanceId)
              || writerInstanceId.equals(lastInstanceId));
    }
  }

  @Test
  public void testSuccessOpenConnection() throws SQLException {

    final String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        MYSQL_INSTANCE_1_URL,
        String.valueOf(AURORA_MYSQL_PORT),
        AURORA_MYSQL_DB);

    Properties props = new Properties();
    props.setProperty("user", AURORA_MYSQL_USERNAME);
    props.setProperty("password", AURORA_MYSQL_PASSWORD);

    Connection conn = connectToInstanceCustomUrl(url, props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @Test
  public void testSuccessOpenConnectionNoPort() throws SQLException {

    final String url = DB_CONN_STR_PREFIX + MYSQL_INSTANCE_1_URL  + "/" + AURORA_MYSQL_DB;

    Properties props = new Properties();
    props.setProperty("user", AURORA_MYSQL_USERNAME);
    props.setProperty("password", AURORA_MYSQL_PASSWORD);

    Connection conn = connectToInstanceCustomUrl(url, props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @ParameterizedTest
  @MethodSource("testParameters")
  public void testFailedConnection(String url, String user, String password) {

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);

    assertThrows(SQLException.class, () -> connectToInstanceCustomUrl(url, props));
  }

  @Test
  public void testFailedHost() {

    Properties props = new Properties();
    props.setProperty("user", AURORA_MYSQL_USERNAME);
    props.setProperty("password", AURORA_MYSQL_PASSWORD);
    String url = buildConnectionString(
        DB_CONN_STR_PREFIX,
        "",
        String.valueOf(AURORA_MYSQL_PORT),
        AURORA_MYSQL_DB);
    assertThrows(SQLException.class, () -> connectToInstanceCustomUrl(
        url, props));
  }
}
