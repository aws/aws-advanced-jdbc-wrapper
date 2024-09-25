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
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
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
  protected static final String customEndpointId = "test-endpoint-1";
  protected static DBClusterEndpoint endpointInfo;
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  protected String currentWriter;

  @BeforeAll
  public static void createCustomEndpoints() {
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    String region = envInfo.getRegion();
    List<TestInstanceInfo> instances = envInfo.getDatabaseInfo().getInstances();
    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
//       // Delete pre-existing custom endpoint in case it wasn't cleaned up in a previous run.
//       try {
//         client.deleteDBClusterEndpoint((builder) -> builder.dbClusterEndpointIdentifier(customEndpointId));
//         long deleteTimeoutNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
//         boolean deleted = false;
//
//         // TODO: should this be moved outside the try-catch? If the cleanup from the last test run sent a
//         //  delete-endpoint request but it hasn't completed yet, we need to wait until it has.
//         while (!deleted && System.nanoTime() < deleteTimeoutNano) {
//           Filter customEndpointFilter =
//               Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
//           DescribeDbClusterEndpointsResponse endpointsResponse = client.describeDBClusterEndpoints(
//               (builder) ->
//                   builder.dbClusterEndpointIdentifier(customEndpointId).filters(customEndpointFilter));
//           deleted = endpointsResponse.dbClusterEndpoints().isEmpty();
//         }
//
//         if (!deleted) {
//           throw new RuntimeException(
//               "The test setup step timed out while attempting to delete a pre-existing custom endpoint.");
//         }
//       } catch (DbClusterEndpointNotFoundException e){
//           // Custom endpoint already does not exist - do nothing
//       }
//
//       client.createDBClusterEndpoint((builder) ->
//         builder.dbClusterEndpointIdentifier(customEndpointId)
//             .dbClusterIdentifier(envInfo.getAuroraClusterName())
//             .endpointType("ANY")
//             .staticMembers(instances.get(0).getInstanceId()));
//
      long timeoutEndNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
      String status = null;

      while (!"available".equals(status) && System.nanoTime() < timeoutEndNano) {
        Filter customEndpointFilter =
            Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
        DescribeDbClusterEndpointsResponse endpointsResponse = client.describeDBClusterEndpoints(
            (builder) ->
                builder.dbClusterEndpointIdentifier(customEndpointId).filters(customEndpointFilter));

        List<DBClusterEndpoint> endpoints = endpointsResponse.dbClusterEndpoints();
        if (endpoints.size() != 1) {
          List<String> endpointURLs = endpoints.stream().map(DBClusterEndpoint::endpoint).collect(Collectors.toList());
          throw new RuntimeException(
              "Unexpected number of custom endpoints with endpoint identifier " + customEndpointId
                  + " in region " + region + ". Expected 1, but found " + endpoints.size()
                  + ". Endpoints: [" + endpointURLs + "].");

        }

        endpointInfo = endpoints.get(0);
        status = endpointInfo.status();
      }

      if (!"available".equals(status)) {
        throw new RuntimeException(
            "The test setup step timed out while waiting for the new custom endpoint to become available.");
      }
    }
  }

  @BeforeEach
  public void setUpEach() {
    this.currentWriter =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0)
            .getInstanceId();
  }

  @AfterAll
  public static void deleteCustomEndpoints() {
//     TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
//     String region = envInfo.getRegion();
//     try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
//       client.deleteDBClusterEndpoint((builder) -> builder.dbClusterEndpointIdentifier(customEndpointId));
//     }

    // TODO: do we need to wait until custom endpoints are fully deleted?
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
                     endpointInfo.endpoint(),
                     port,
                     TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
                 props)) {

      String instanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointInfo.staticMembers().contains(instanceId));

      // Use failover API to break connection
      if (instanceId.equals(this.currentWriter)) {
        auroraUtil.failoverClusterAndWaitUntilWriterChanged();
      } else {
        auroraUtil.failoverClusterToATargetAndWaitUntilWriterChanged(this.currentWriter, instanceId);
      }

      assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));

      String newInstanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointInfo.staticMembers().contains(newInstanceId));
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
