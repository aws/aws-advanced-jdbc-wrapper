package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.List;
import java.util.Properties;
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
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointResponse;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DbClusterEndpointNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;

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
  protected static final List<String> endpointIDs = Arrays.asList(oneInstanceEndpointId, twoInstanceEndpointId);
  protected static CreateDbClusterEndpointResponse oneInstanceEndpointInfo;
  protected static CreateDbClusterEndpointResponse twoInstanceEndpointInfo;
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  protected String currentWriter;

  @BeforeAll
  public static void createEndpoints() {
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    String region = envInfo.getRegion();

    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
      // Delete pre-existing custom endpoints in case they weren't cleaned up in a previous run.
      deleteEndpoints(client);

      List<TestInstanceInfo> instances = envInfo.getDatabaseInfo().getInstances();
      String clusterId = envInfo.getAuroraClusterName();

      oneInstanceEndpointInfo = createEndpoint(
          client, clusterId, oneInstanceEndpointId, instances.subList(0, 1));
      twoInstanceEndpointInfo = createEndpoint(
          client, clusterId, twoInstanceEndpointId, instances.subList(0, 2));

      waitUntilEndpointsAvailable(client, clusterId);
    }
  }

  private static void deleteEndpoints(RdsClient client) {
    for (String endpointId : endpointIDs) {
      try {
        client.deleteDBClusterEndpoint((builder) -> builder.dbClusterEndpointIdentifier(endpointId));
      } catch (DbClusterEndpointNotFoundException e) {
        // Custom endpoint already does not exist - do nothing
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

      allEndpointsDeleted = endpointIDs.stream().noneMatch(responseIDs::contains);
    }

    if (!allEndpointsDeleted) {
      throw new RuntimeException(
          "The test setup step timed out while attempting to delete pre-existing test custom endpoints.");
    }
  }

  private static CreateDbClusterEndpointResponse createEndpoint(
      RdsClient client, String clusterId, String endpointId, List<TestInstanceInfo> instances) {
    List<String> instanceIDs = instances.stream().map(TestInstanceInfo::getInstanceId).collect(Collectors.toList());
    return client.createDBClusterEndpoint((builder) ->
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
      List<DBClusterEndpoint> endpoints = endpointsResponse.dbClusterEndpoints();

      int availableEndpoints = 0;
      for (int i = 0; i < endpoints.size() && availableEndpoints < endpointIDs.size(); i++) {
        DBClusterEndpoint endpoint = endpoints.get(i);
        if (endpointIDs.contains(endpoint.dbClusterEndpointIdentifier())
            && "available".equals(endpoint.status())) {
          availableEndpoints++;
        }
      }

      allEndpointsAvailable = availableEndpoints == endpointIDs.size();
    }

    if (!allEndpointsAvailable) {
      throw new RuntimeException(
          "The test setup step timed out while waiting for the new custom endpoints to become available.");
    }
  }

  @BeforeEach
  public void identifyWriter() {
    this.currentWriter =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0)
            .getInstanceId();
  }

  @AfterAll
  public static void cleanup() {
    String region = TestEnvironment.getCurrent().getInfo().getRegion();
    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
      deleteEndpoints(client);
    }
  }

  @TestTemplate
  public void testCustomEndpointReaderFailover() throws SQLException, InterruptedException {
    final TestDatabaseInfo dbInfo = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();
    props.setProperty("failoverMode", "reader-or-writer");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(
                     oneInstanceEndpointInfo.endpoint(),
                     port,
                     TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      String instanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(oneInstanceEndpointInfo.staticMembers().contains(instanceId));

      // Use failover API to break connection
      if (instanceId.equals(this.currentWriter)) {
        auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      } else {
        auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(this.currentWriter, instanceId);
      }

      assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));

      String newInstanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(oneInstanceEndpointInfo.staticMembers().contains(newInstanceId));
    }
  }

  protected Properties initDefaultProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,customEndpoint");
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "10000");
    return props;
  }
}
