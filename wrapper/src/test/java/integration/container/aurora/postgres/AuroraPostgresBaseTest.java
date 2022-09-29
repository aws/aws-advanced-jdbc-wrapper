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

package integration.container.aurora.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.container.aurora.TestAuroraHostListProvider;
import integration.util.AuroraTestUtility;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.PGProperty;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.util.StringUtils;

public abstract class AuroraPostgresBaseTest {

  protected static final String AURORA_POSTGRES_USERNAME = System.getenv("AURORA_POSTGRES_USERNAME");
  protected static final String AURORA_POSTGRES_PASSWORD = System.getenv("AURORA_POSTGRES_PASSWORD");
  protected static final String AURORA_POSTGRES_DB =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_DB")) ? System.getenv("AURORA_POSTGRES_DB") : "test";

  protected static final String QUERY_FOR_INSTANCE = "SELECT aurora_db_instance_identifier()";

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX =
      System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXIED_CLUSTER_TEMPLATE =
      System.getenv("PROXIED_CLUSTER_TEMPLATE");

  protected static final String DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:postgresql://";
  protected static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");

  static final String POSTGRES_INSTANCE_1_URL = System.getenv("POSTGRES_INSTANCE_1_URL");
  static final String POSTGRES_INSTANCE_2_URL = System.getenv("POSTGRES_INSTANCE_2_URL");
  static final String POSTGRES_INSTANCE_3_URL = System.getenv("POSTGRES_INSTANCE_3_URL");
  static final String POSTGRES_INSTANCE_4_URL = System.getenv("POSTGRES_INSTANCE_4_URL");
  static final String POSTGRES_INSTANCE_5_URL = System.getenv("POSTGRES_INSTANCE_5_URL");
  static final String POSTGRES_CLUSTER_URL = System.getenv("DB_CLUSTER_CONN");
  static final String POSTGRES_RO_CLUSTER_URL = System.getenv("DB_RO_CLUSTER_CONN");

  static final String DB_CLUSTER_IDENTIFIER =
      !StringUtils.isNullOrEmpty(POSTGRES_CLUSTER_URL)
          ? POSTGRES_CLUSTER_URL.substring(0, POSTGRES_CLUSTER_URL.indexOf('.'))
          : null;
  protected static final String AURORA_POSTGRES_DB_REGION =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_DB_REGION"))
          ? System.getenv("AURORA_POSTGRES_DB_REGION")
          : "us-east-1";

  protected static final int AURORA_POSTGRES_PORT = Integer.parseInt(System.getenv("AURORA_POSTGRES_PORT"));
  protected static final int POSTGRES_PROXY_PORT = Integer.parseInt(System.getenv("POSTGRES_PROXY_PORT"));

  protected static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
  protected static final String TOXIPROXY_CLUSTER_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_CLUSTER_NETWORK_ALIAS");
  protected static final String TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS =
      System.getenv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS");
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static ToxiproxyClient toxiproxyClientInstance_1;
  protected static ToxiproxyClient toxiproxyClientInstance_2;
  protected static ToxiproxyClient toxiproxyClientInstance_3;
  protected static ToxiproxyClient toxiproxyClientInstance_4;
  protected static ToxiproxyClient toxiproxyClientInstance_5;
  protected static ToxiproxyClient toxiproxyCluster;
  protected static ToxiproxyClient toxiproxyReadOnlyCluster;

  protected static Proxy proxyInstance_1;
  protected static Proxy proxyInstance_2;
  protected static Proxy proxyInstance_3;
  protected static Proxy proxyInstance_4;
  protected static Proxy proxyInstance_5;
  protected static Proxy proxyCluster;
  protected static Proxy proxyReadOnlyCluster;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected String[] instanceIDs; // index 0 is always writer!
  protected int clusterSize = 0;

  protected final ContainerHelper containerHelper = new ContainerHelper();
  protected final AuroraTestUtility auroraUtil = new AuroraTestUtility(AURORA_POSTGRES_DB_REGION);

  protected final RdsClient rdsClient =
      RdsClient.builder()
          .region(auroraUtil.getRegion(AURORA_POSTGRES_DB_REGION))
          .credentialsProvider(DefaultCredentialsProvider.create())
          .build();

  private static final int CP_MIN_IDLE = 5;
  private static final int CP_MAX_IDLE = 10;
  private static final int CP_MAX_OPEN_PREPARED_STATEMENTS = 100;
  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";
  protected static final int IS_VALID_TIMEOUT = 5;

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    toxiproxyClientInstance_1 =
        new ToxiproxyClient(TOXIPROXY_INSTANCE_1_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_2 =
        new ToxiproxyClient(TOXIPROXY_INSTANCE_2_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_3 =
        new ToxiproxyClient(TOXIPROXY_INSTANCE_3_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_4 =
        new ToxiproxyClient(TOXIPROXY_INSTANCE_4_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_5 =
        new ToxiproxyClient(TOXIPROXY_INSTANCE_5_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyCluster = new ToxiproxyClient(TOXIPROXY_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyReadOnlyCluster =
        new ToxiproxyClient(TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);

    proxyInstance_1 = getProxy(toxiproxyClientInstance_1, POSTGRES_INSTANCE_1_URL, AURORA_POSTGRES_PORT);
    proxyInstance_2 = getProxy(toxiproxyClientInstance_2, POSTGRES_INSTANCE_2_URL, AURORA_POSTGRES_PORT);
    proxyInstance_3 = getProxy(toxiproxyClientInstance_3, POSTGRES_INSTANCE_3_URL, AURORA_POSTGRES_PORT);
    proxyInstance_4 = getProxy(toxiproxyClientInstance_4, POSTGRES_INSTANCE_4_URL, AURORA_POSTGRES_PORT);
    proxyInstance_5 = getProxy(toxiproxyClientInstance_5, POSTGRES_INSTANCE_5_URL, AURORA_POSTGRES_PORT);
    proxyCluster = getProxy(toxiproxyCluster, POSTGRES_CLUSTER_URL, AURORA_POSTGRES_PORT);
    proxyReadOnlyCluster = getProxy(toxiproxyReadOnlyCluster, POSTGRES_RO_CLUSTER_URL, AURORA_POSTGRES_PORT);

    proxyMap.put(
        POSTGRES_INSTANCE_1_URL.substring(0, POSTGRES_INSTANCE_1_URL.indexOf('.')), proxyInstance_1);
    proxyMap.put(
        POSTGRES_INSTANCE_2_URL.substring(0, POSTGRES_INSTANCE_2_URL.indexOf('.')), proxyInstance_2);
    proxyMap.put(
        POSTGRES_INSTANCE_3_URL.substring(0, POSTGRES_INSTANCE_3_URL.indexOf('.')), proxyInstance_3);
    proxyMap.put(
        POSTGRES_INSTANCE_4_URL.substring(0, POSTGRES_INSTANCE_4_URL.indexOf('.')), proxyInstance_4);
    proxyMap.put(
        POSTGRES_INSTANCE_5_URL.substring(0, POSTGRES_INSTANCE_5_URL.indexOf('.')), proxyInstance_5);
    proxyMap.put(POSTGRES_CLUSTER_URL, proxyCluster);
    proxyMap.put(POSTGRES_RO_CLUSTER_URL, proxyReadOnlyCluster);

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  protected static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port)
      throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @BeforeEach
  public void setUpEach() throws InterruptedException, SQLException {
    proxyMap.forEach((instance, proxy) -> containerHelper.enableConnectivity(proxy));

    // Always get the latest topology info with writer as first
    List<String> latestTopology = getTopologyIds();
    instanceIDs = new String[latestTopology.size()];
    latestTopology.toArray(instanceIDs);

    clusterSize = instanceIDs.length;
    assertTrue(
        clusterSize >= 2); // many tests assume that cluster contains at least a writer and a reader
    assertTrue(isDBInstanceWriter(instanceIDs[0]));
    makeSureInstancesUp(instanceIDs);
    TestAuroraHostListProvider.clearCache();
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), AURORA_POSTGRES_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), AURORA_POSTGRES_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());
    props.setProperty(PropertyDefinition.PLUGINS.name, "failover");

    return props;
  }

  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "5");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "5");

    return props;
  }

  protected Properties initDefaultProxiedProps() {
    final Properties props = initDefaultProps();
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props, PROXIED_CLUSTER_TEMPLATE);

    return props;
  }

  protected Properties initFailoverDisabledProps() {
    final Properties props = initDefaultProps();
    FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER.set(props, "false");

    return props;
  }

  protected Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    return connectToInstance(instanceUrl, port, initDefaultProxiedProps());
  }

  protected Connection connectToInstance(String instanceUrl, int port, Properties props)
      throws SQLException {
    final String url = DB_CONN_STR_PREFIX + instanceUrl + ":" + port + "/" + AURORA_POSTGRES_DB;
    return DriverManager.getConnection(url, props);
  }

  protected Connection connectToInstanceCustomUrl(String url, Properties props)
      throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  protected String hostToIP(String hostname) throws UnknownHostException {
    final InetAddress inet = InetAddress.getByName(hostname);
    return inet.getHostAddress();
  }

  // Return list of instance endpoints.
  // Writer instance goes first.
  protected List<String> getTopologyEndpoints() throws SQLException {
    final String dbConnHostBase =
        DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;

    final String url =
        DB_CONN_STR_PREFIX + POSTGRES_INSTANCE_1_URL + ":" + AURORA_POSTGRES_PORT + "/" + AURORA_POSTGRES_DB;
    return this.containerHelper.getAuroraInstanceEndpoints(
        url, AURORA_POSTGRES_USERNAME, AURORA_POSTGRES_PASSWORD, dbConnHostBase);
  }

  // Return list of instance Ids.
  // Writer instance goes first.
  protected List<String> getTopologyIds() throws SQLException {
    final String url =
        DB_CONN_STR_PREFIX + POSTGRES_INSTANCE_1_URL + ":" + AURORA_POSTGRES_PORT + "/" + AURORA_POSTGRES_DB;
    return this.containerHelper.getAuroraInstanceIds(
        url,
        AURORA_POSTGRES_USERNAME,
        AURORA_POSTGRES_PASSWORD,
        "postgres");
  }

  /* Helper functions. */
  protected String queryInstanceId(Connection connection) throws SQLException {
    try (final Statement myStmt = connection.createStatement()) {
      return executeInstanceIdQuery(myStmt);
    }
  }

  protected String executeInstanceIdQuery(Statement stmt) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery(QUERY_FOR_INSTANCE)) {
      if (rs.next()) {
        return rs.getString("aurora_db_instance_identifier");
      }
    }
    return null;
  }

  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.
  protected void assertFirstQueryThrows(Connection connection, String expectedSQLErrorCode) {
    final SQLException exception =
        assertThrows(SQLException.class, () -> queryInstanceId(connection));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  protected void assertFirstQueryThrows(Statement stmt, String expectedSQLErrorCode) {
    final SQLException exception = assertThrows(SQLException.class, () -> executeInstanceIdQuery(stmt));
    assertEquals(expectedSQLErrorCode, exception.getSQLState(), "Unexpected SQL Exception: " + exception.getMessage());
  }

  protected Connection createDataSourceConnectionWithFailoverUsingInstanceId(String instanceID) throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    // Configure the property names for the underlying driver-specific data source:
    ds.setJdbcProtocol("jdbc:postgresql:");
    ds.setDatabasePropertyName("databaseName");
    ds.setServerPropertyName("serverName");
    ds.setPortPropertyName("port");

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    // Configure the driver-specific data source:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", instanceID + DB_CONN_STR_SUFFIX);
    targetDataSourceProps.setProperty("databaseName", AURORA_POSTGRES_DB);
    targetDataSourceProps.setProperty("wrapperPlugins", "failover");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    return ds.getConnection(AURORA_POSTGRES_USERNAME, AURORA_POSTGRES_PASSWORD);
  }

  protected DBCluster getDBCluster() {
    final DescribeDbClustersResponse dbClustersResult =
        rdsClient.describeDBClusters(
            (builder) -> builder.dbClusterIdentifier(DB_CLUSTER_IDENTIFIER));
    final List<DBCluster> dbClusterList = dbClustersResult.dbClusters();
    return dbClusterList.get(0);
  }

  protected List<DBClusterMember> getDBClusterMemberList() {
    final DBCluster dbCluster = getDBCluster();
    return dbCluster.dbClusterMembers();
  }

  protected DBClusterMember getMatchedDBClusterMember(String instanceId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> dbClusterMember.dbInstanceIdentifier().equals(instanceId))
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_SUCH_CLUSTER_MEMBER + instanceId);
    }
    return matchedMemberList.get(0);
  }

  protected String getDBClusterWriterInstanceId() {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(DBClusterMember::isClusterWriter).collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_WRITER_AVAILABLE);
    }
    // Should be only one writer at index 0.
    return matchedMemberList.get(0).dbInstanceIdentifier();
  }

  protected Boolean isDBInstanceWriter(String instanceId) {
    return getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected Boolean isDBInstanceReader(String instanceId) {
    return !getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected void makeSureInstancesUp(String[] instances) throws InterruptedException {
    makeSureInstancesUp(instances, true);
  }

  protected void makeSureInstancesUp(String[] instances, boolean finalCheck)
      throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final ConcurrentHashMap<String, Boolean> remainingInstances = new ConcurrentHashMap<>();
    Arrays.asList(instances).forEach((k) -> remainingInstances.put(k, true));

    for (final String id : instances) {
      executorService.submit(
          () -> {
            while (true) {
              try (final Connection conn =
                  connectToInstance(
                      id + DB_CONN_STR_SUFFIX, AURORA_POSTGRES_PORT, initFailoverDisabledProps())) {
                remainingInstances.remove(id);
                break;
              } catch (final SQLException ex) {
                ex.printStackTrace();
                // Continue waiting until instance is up.
              } catch (final Exception ex) {
                System.out.println("Exception: " + ex);
                break;
              }
              TimeUnit.MILLISECONDS.sleep(1000);
            }
            return null;
          });
    }
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.MINUTES);

    if (finalCheck) {
      assertTrue(
          remainingInstances.isEmpty(),
          "The following instances are still down: \n" + String.join("\n", remainingInstances.keySet()));
    }
  }

  // Helpers
  protected void failoverClusterAndWaitUntilWriterChanged(String clusterWriterId)
      throws InterruptedException {
    failoverCluster();
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  protected void failoverCluster() throws InterruptedException {
    waitUntilClusterHasRightState();
    while (true) {
      try {
        rdsClient.failoverDBCluster((builder) -> builder.dbClusterIdentifier(DB_CLUSTER_IDENTIFIER));
        break;
      } catch (final Exception e) {
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
  }

  protected void waitUntilWriterInstanceChanged(String initialWriterInstanceId)
      throws InterruptedException {
    String nextClusterWriterId = getDBClusterWriterInstanceId();
    while (initialWriterInstanceId.equals(nextClusterWriterId)) {
      TimeUnit.MILLISECONDS.sleep(3000);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId();
    }
  }

  protected void waitUntilClusterHasRightState() throws InterruptedException {
    String status = getDBCluster().status();
    while (!"available".equalsIgnoreCase(status)) {
      TimeUnit.MILLISECONDS.sleep(1000);
      status = getDBCluster().status();
    }
  }

  protected void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterWriterId,
      String targetInstanceId) throws InterruptedException {
    failoverClusterWithATargetInstance(targetInstanceId);
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  protected void failoverClusterWithATargetInstance(String targetInstanceId)
      throws InterruptedException {
    waitUntilClusterHasRightState();

    while (true) {
      try {
        rdsClient.failoverDBCluster(
            (builder) -> builder.dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
                .targetDBInstanceIdentifier(targetInstanceId));
        break;
      } catch (final Exception e) {
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
  }
}
