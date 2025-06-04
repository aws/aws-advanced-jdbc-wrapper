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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
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
@EnableOnDatabaseEngineDeployment({DatabaseEngineDeployment.AURORA})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@EnableOnNumOfInstances(min = 3)
@MakeSureFirstInstanceWriter
@Order(16)
public class CustomEndpointTest {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointTest.class.getName());
  protected static final String endpointId = "test-endpoint-1-" + UUID.randomUUID();
  protected static DBClusterEndpoint endpointInfo;

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected static final boolean reuseExistingEndpoint = false;

  protected String currentWriter;

  @BeforeAll
  public static void setupEndpoint() {
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    String clusterId = envInfo.getRdsDbName();
    String region = envInfo.getRegion();

    try (RdsClient client = RdsClient.builder().region(Region.of(region)).build()) {
      if (reuseExistingEndpoint) {
        waitUntilEndpointAvailable(client);
        return;
      }

      List<TestInstanceInfo> instances = envInfo.getDatabaseInfo().getInstances();
      createEndpoint(client, clusterId, instances.subList(0, 1));
      waitUntilEndpointAvailable(client);
    }
  }

  private static void createEndpoint(RdsClient client, String clusterId, List<TestInstanceInfo> instances) {
    List<String> instanceIDs = instances.stream().map(TestInstanceInfo::getInstanceId).collect(Collectors.toList());
    client.createDBClusterEndpoint((builder) ->
        builder.dbClusterEndpointIdentifier(CustomEndpointTest.endpointId)
            .dbClusterIdentifier(clusterId)
            .endpointType("ANY")
            .staticMembers(instanceIDs));
  }

  public static void waitUntilEndpointAvailable(RdsClient client) {
    long timeoutEndNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
    boolean available = false;

    while (System.nanoTime() < timeoutEndNano) {
      Filter customEndpointFilter =
          Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
      DescribeDbClusterEndpointsResponse endpointsResponse = client.describeDBClusterEndpoints(
          (builder) ->
              builder.dbClusterEndpointIdentifier(endpointId).filters(customEndpointFilter));
      List<DBClusterEndpoint> responseEndpoints = endpointsResponse.dbClusterEndpoints();
      if (responseEndpoints.size() != 1) {
        try {
          // Endpoint needs more time to get created
          TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      DBClusterEndpoint responseEndpoint = responseEndpoints.get(0);
      endpointInfo = responseEndpoint;
      available = "available".equals(responseEndpoint.status());
      if (available) {
        break;
      }

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    if (!available) {
      throw new RuntimeException(
          "The test setup step timed out while waiting for the custom endpoint to become available: '"
              + endpointId + "'.");
    }
  }

  public static void waitUntilEndpointHasMembers(RdsClient client, String endpointId, List<String> membersList) {
    long start = System.nanoTime();

    // Convert to set for later comparison.
    Set<String> members = new HashSet<>(membersList);
    long timeoutEndNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(20);
    boolean hasCorrectState = false;
    while (System.nanoTime() < timeoutEndNano) {
      DescribeDbClusterEndpointsResponse response = client.describeDBClusterEndpoints(
          (builder) ->
              builder.dbClusterEndpointIdentifier(endpointId));
      if (response.dbClusterEndpoints().size() != 1) {
        fail("Unexpected number of endpoints returned while waiting for custom endpoint to have the specified list of "
            + "members. Expected 1, got " + response.dbClusterEndpoints().size() + ".");
      }

      DBClusterEndpoint endpoint = response.dbClusterEndpoints().get(0);
      // Compare sets to ignore order when checking for members equality.
      Set<String> responseMembers = new HashSet<>(endpoint.staticMembers());
      hasCorrectState = responseMembers.equals(members) && "available".equals(endpoint.status());
      if (hasCorrectState) {
        break;
      }

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    if (!hasCorrectState) {
      fail("Timed out while waiting for the custom endpoint to stabilize: '" + endpointId + "'.");
    }

    LOGGER.fine("waitUntilEndpointHasCorrectState took "
        + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) + " seconds");
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
      deleteEndpoint(client);
    }
  }

  private static void deleteEndpoint(RdsClient client) {
    try {
      client.deleteDBClusterEndpoint((builder) -> builder.dbClusterEndpointIdentifier(endpointId));
    } catch (DbClusterEndpointNotFoundException e) {
      // Custom endpoint already does not exist - do nothing.
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
    final TestDatabaseInfo dbInfo = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();
    props.setProperty("failoverMode", "reader-or-writer");

    try (final Connection conn = DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(endpointInfo.endpoint(), port, dbInfo.getDefaultDbName()),
        props)) {
      List<String> endpointMembers = endpointInfo.staticMembers();
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
  public void testCustomEndpointReadWriteSplitting_withCustomEndpointChanges() throws SQLException {
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    final TestDatabaseInfo dbInfo = envInfo.getDatabaseInfo();
    final int port = dbInfo.getClusterEndpointPort();
    final Properties props = initDefaultProps();
    // This setting is not required for the test, but it allows us to also test re-creation of expired monitors since it
    // takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
    props.setProperty("customEndpointMonitorExpirationMs", "30000");

    try (final Connection conn =
             DriverManager.getConnection(
                 ConnectionStringHelper.getWrapperUrl(endpointInfo.endpoint(), port, dbInfo.getDefaultDbName()),
                  props);
         final RdsClient client = RdsClient.builder().region(Region.of(envInfo.getRegion())).build()) {
      List<String> endpointMembers = endpointInfo.staticMembers();
      String instanceId1 = auroraUtil.queryInstanceId(conn);
      assertTrue(endpointMembers.contains(instanceId1));

      // Attempt to switch to an instance of the opposite role. This should fail since the custom endpoint consists only
      // of the current host.
      boolean newReadOnlyValue = currentWriter.equals(instanceId1);
      if (newReadOnlyValue) {
        // We are connected to the writer. Attempting to switch to the reader will not work but will intentionally not
        // throw an exception. In this scenario we log a warning and purposefully stick with the writer.
        LOGGER.fine("Initial connection is to the writer. Attempting to switch to reader...");
        conn.setReadOnly(newReadOnlyValue);
        String newInstanceId = auroraUtil.queryInstanceId(conn);
        assertEquals(instanceId1, newInstanceId);
      } else {
        // We are connected to the reader. Attempting to switch to the writer will throw an exception.
        LOGGER.fine("Initial connection is to a reader. Attempting to switch to writer...");
        assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(newReadOnlyValue));
      }

      String newMember;
      if (currentWriter.equals(instanceId1)) {
        newMember = dbInfo.getInstances().get(1).getInstanceId();
      } else {
        newMember = currentWriter;
      }

      client.modifyDBClusterEndpoint(
          builder ->
              builder.dbClusterEndpointIdentifier(endpointId).staticMembers(instanceId1, newMember));

      try {
        waitUntilEndpointHasMembers(client, endpointId, Arrays.asList(instanceId1, newMember));

        // We should now be able to switch to newMember.
        assertDoesNotThrow(() -> conn.setReadOnly(newReadOnlyValue));
        String instanceId2 = auroraUtil.queryInstanceId(conn);
        assertEquals(instanceId2, newMember);

        // Switch back to original instance.
        conn.setReadOnly(!newReadOnlyValue);
      } finally {
        client.modifyDBClusterEndpoint(
            builder ->
                builder.dbClusterEndpointIdentifier(endpointId).staticMembers(instanceId1));
        waitUntilEndpointHasMembers(client, endpointId, Collections.singletonList(instanceId1));
      }

      // We should not be able to switch again because newMember was removed from the custom endpoint.
      if (newReadOnlyValue) {
        // We are connected to the writer. Attempting to switch to the reader will not work but will intentionally not
        // throw an exception. In this scenario we log a warning and purposefully stick with the writer.
        conn.setReadOnly(newReadOnlyValue);
        String newInstanceId = auroraUtil.queryInstanceId(conn);
        assertEquals(instanceId1, newInstanceId);
      } else {
        // We are connected to the reader. Attempting to switch to the writer will throw an exception.
        assertThrows(ReadWriteSplittingSQLException.class, () -> conn.setReadOnly(newReadOnlyValue));
      }
    }
  }
}
