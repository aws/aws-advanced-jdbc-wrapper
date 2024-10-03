package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.DatabaseEngineDeployment;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY})
@EnableOnNumOfInstances(min = 3)
@MakeSureFirstInstanceWriter
@Order(16)
public class CustomEndpointTest {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointTest.class.getName());
  protected static final String oneInstanceEndpointId = "test-endpoint-1";
  protected static final String twoInstanceEndpointId = "test-endpoint-2";
  protected static final Map<String, DBClusterEndpoint> endpoints = new HashMap<String, DBClusterEndpoint>() {{
    put(oneInstanceEndpointId, null);
    put(twoInstanceEndpointId, null);
  }};

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected static final boolean reuseExistingEndpoint = true;

  protected String currentWriter;

  @BeforeAll
  public static void createEndpoints() {
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    String clusterId = envInfo.getAuroraClusterName();
    String region = envInfo.getRegion();

    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
      if (reuseExistingEndpoint) {
        waitUntilEndpointsAvailable(client, clusterId);
        return;
      }

      // Delete pre-existing custom endpoints in case they weren't cleaned up in a previous run.
      deleteEndpoints(client);

      List<TestInstanceInfo> instances = envInfo.getDatabaseInfo().getInstances();
      createEndpoint(client, clusterId, oneInstanceEndpointId, instances.subList(0, 1));
      createEndpoint(client, clusterId, twoInstanceEndpointId, instances.subList(0, 2));
      waitUntilEndpointsAvailable(client, clusterId);
    }
  }

  private static void deleteEndpoints(RdsClient client) {
    for (String endpointId : endpoints.keySet() ) {
      try {
        client.deleteDBClusterEndpoint((builder) -> builder.dbClusterEndpointIdentifier(endpointId));
      } catch (DbClusterEndpointNotFoundException e) {
        // Custom endpoint already does not exist - do nothing.
      }
    }

    waitUntilEndpointsDeleted(client);
  }

  private static void waitUntilEndpointsDeleted(RdsClient client) {
    String clusterId = TestEnvironment.getCurrent().getInfo().getAuroraClusterName();
    long deleteTimeoutNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
    boolean allEndpointsDeleted = false;

    while (!allEndpointsDeleted && System.nanoTime() < deleteTimeoutNano) {
      Filter customEndpointFilter =
          Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
      DescribeDbClusterEndpointsResponse endpointsResponse = client.describeDBClusterEndpoints(
          (builder) ->
              builder.dbClusterIdentifier(clusterId).filters(customEndpointFilter));
      List<String> responseIDs = endpointsResponse.dbClusterEndpoints().stream()
          .map(DBClusterEndpoint::dbClusterEndpointIdentifier).collect(Collectors.toList());

      allEndpointsDeleted = endpoints.keySet().stream().noneMatch(responseIDs::contains);
    }

    if (!allEndpointsDeleted) {
      throw new RuntimeException(
          "The test setup step timed out while attempting to delete pre-existing test custom endpoints.");
    }
  }

  private static void createEndpoint(
      RdsClient client, String clusterId, String endpointId, List<TestInstanceInfo> instances) {
    List<String> instanceIDs = instances.stream().map(TestInstanceInfo::getInstanceId).collect(Collectors.toList());
    client.createDBClusterEndpoint((builder) ->
      builder.dbClusterEndpointIdentifier(endpointId)
          .dbClusterIdentifier(clusterId)
          .endpointType("ANY")
          .staticMembers(instanceIDs));
  }

  public static void waitUntilEndpointsAvailable(RdsClient client, String clusterId) {
    long timeoutEndNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
    boolean allEndpointsAvailable = false;

    while (!allEndpointsAvailable && System.nanoTime() < timeoutEndNano) {
      Filter customEndpointFilter =
          Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
      DescribeDbClusterEndpointsResponse endpointsResponse = client.describeDBClusterEndpoints(
          (builder) ->
              builder.dbClusterIdentifier(clusterId).filters(customEndpointFilter));
      List<DBClusterEndpoint> responseEndpoints = endpointsResponse.dbClusterEndpoints();

      int numAvailableEndpoints = 0;
      for (int i = 0; i < responseEndpoints.size() && numAvailableEndpoints < endpoints.size(); i++) {
        DBClusterEndpoint endpoint = responseEndpoints.get(i);
        String endpointId = endpoint.dbClusterEndpointIdentifier();
        if (endpoints.containsKey(endpointId)) {
          endpoints.put(endpointId, endpoint);
          if ("available".equals(endpoint.status())) {
            numAvailableEndpoints++;
          }
        }
      }

      allEndpointsAvailable = numAvailableEndpoints == endpoints.size();
    }

    if (!allEndpointsAvailable) {
      throw new RuntimeException(
          "The test setup step timed out while waiting for the new custom endpoints to become available.");
    }
  }

  public static void waitUntilEndpointHasCorrectState(RdsClient client, String endpointId, List<String> membersList) {
    long start = System.nanoTime();

    // Convert to set for later comparison.
    Set<String> members = new HashSet<>(membersList);
    long timeoutEndNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(20);
    boolean hasCorrectState = false;
    while (!hasCorrectState && System.nanoTime() < timeoutEndNano) {
      DescribeDbClusterEndpointsResponse response = client.describeDBClusterEndpoints(
          (builder) ->
              builder.dbClusterEndpointIdentifier(endpointId));
      if (response.dbClusterEndpoints().size() != 1) {
        fail("Unexpected number of endpoints returned while waiting for custom endpoint to have the specified list of "
            + "members. Expected 1, got " + response.dbClusterEndpoints().size());
      }

      DBClusterEndpoint endpoint = response.dbClusterEndpoints().get(0);
      // Compare sets to ignore order when checking for members equality.
      Set<String> responseMembers = new HashSet<>(endpoint.staticMembers());
      hasCorrectState = responseMembers.equals(members) && "available".equals(endpoint.status());
    }

    if (!hasCorrectState) {
      fail("Timed out while waiting for the custom endpoint to stabilize");
    }

    System.out.println("asdf waitUntilEndpointHasMembers took " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds");
  }

  @BeforeEach
  public void identifyWriter() {
    this.currentWriter =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0)
            .getInstanceId();
  }

  @AfterAll
  public static void cleanup() {
    if (reuseExistingEndpoint) {
      return;
    }

    String region = TestEnvironment.getCurrent().getInfo().getRegion();
    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
      deleteEndpoints(client);
    }
  }

  protected Properties initDefaultProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "customEndpoint,readWriteSplitting,failover");
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");
    return props;
  }

  @TestTemplate
  public void testCustomEndpointFailover() throws SQLException, InterruptedException {
    // The single-instance endpoint will be used for this test.
    final DBClusterEndpoint endpoint = endpoints.get(oneInstanceEndpointId);
    final TestDatabaseInfo dbInfo = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(endpoint.endpoint(), port, dbInfo.getDefaultDbName()),
        props)) {
      List<String> endpointMembers = endpoint.staticMembers();
      String instanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointMembers.contains(instanceId));

      // Use failover API to break connection.
      if (instanceId.equals(this.currentWriter)) {
        auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      } else {
        auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(this.currentWriter, instanceId);
      }

      assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));

      String newInstanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointMembers.contains(newInstanceId));
    }
  }

  @TestTemplate
  public void testCustomEndpointReadWriteSplitting() throws SQLException, InterruptedException {
    // The two-instance custom endpoint will be used for this test.
    final DBClusterEndpoint testEndpoint = endpoints.get(twoInstanceEndpointId);
    final TestDatabaseInfo dbInfo = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();

    if (!testEndpoint.staticMembers().contains(currentWriter)) {
      // For this test, we want one instance in the custom endpoint to be the writer, and one to be a reader.
      String newWriter = testEndpoint.staticMembers().get(0);
      auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(currentWriter, newWriter);
      currentWriter = newWriter;
    }

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(testEndpoint.endpoint(), port, dbInfo.getDefaultDbName()),
        props)) {
      List<String> endpointMembers = testEndpoint.staticMembers();
      String instanceId1 = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointMembers.contains(instanceId1));

      // Switch to an instance of the opposite role.
      boolean newReadOnlyValue = currentWriter.equals(instanceId1);
      LOGGER.fine(newReadOnlyValue ? "Testing switch to reader..." : "Testing switch to writer...");
      conn.setReadOnly(newReadOnlyValue);

      String instanceId2 = auroraUtil.queryInstanceId(conn);
      if (instanceId1.equals(instanceId2)) {
        System.out.println("Error: both instances were " + instanceId1);
      }

      assertNotEquals(instanceId1, instanceId2);
      assertTrue(endpointMembers.contains(instanceId2));
    }
  }

  @TestTemplate
  public void testCustomEndpointReadWriteSplitting_testCustomEndpointChanges() throws SQLException {
    // The one-instance custom endpoint will be used for this test.
    final DBClusterEndpoint testEndpoint = endpoints.get(oneInstanceEndpointId);
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    final TestDatabaseInfo dbInfo = envInfo.getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();
    // This setting is not required for the test, but it allows us to also test recreation of expired monitors since it
    // takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
    props.setProperty("customEndpointMonitorExpirationMs", "30000");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(testEndpoint.endpoint(), port, dbInfo.getDefaultDbName()),
                  props);
         final RdsClient client = RdsClient.builder().region(Region.of(envInfo.getRegion())).build()) {
      List<String> endpointMembers = testEndpoint.staticMembers();
      String originalInstanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointMembers.contains(originalInstanceId));

      // Attempt to switch to an instance of the opposite role. This should fail since the custom endpoint consists only
      // of the current host.
      boolean newReadOnlyValue = currentWriter.equals(originalInstanceId);
      assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(newReadOnlyValue));

      String newMember;
      if (currentWriter.equals(originalInstanceId)) {
        newMember = dbInfo.getInstances().get(1).getInstanceId();
      } else {
        newMember = currentWriter;
      }

      client.modifyDBClusterEndpoint(
          builder ->
              builder.dbClusterEndpointIdentifier(oneInstanceEndpointId).staticMembers(originalInstanceId, newMember));

      try {
        waitUntilEndpointHasCorrectState(client, oneInstanceEndpointId, Arrays.asList(originalInstanceId, newMember));
        assertDoesNotThrow(() -> conn.setReadOnly(newReadOnlyValue));
        conn.setReadOnly(!newReadOnlyValue);
      } finally {
        client.modifyDBClusterEndpoint(
            builder ->
                builder.dbClusterEndpointIdentifier(oneInstanceEndpointId).staticMembers(originalInstanceId));
        waitUntilEndpointHasCorrectState(client, oneInstanceEndpointId, Collections.singletonList(originalInstanceId));
      }

      assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(newReadOnlyValue));
    }
  }
}
