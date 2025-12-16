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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Demonstrates performance bug: AuroraConnectionTrackerPlugin executes alias queries on EVERY
 * plugin.connect() call for cluster endpoints, even when reusing the same physical connection.
 *
 * <p><b>Bug location:</b> AuroraConnectionTrackerPlugin.connect() lines 91-95 unconditionally calls
 * resetAliases() + fillAliases() for cluster endpoints.
 *
 * <p><b>Impact:</b> With internal pooling (HikariPooledConnectionProvider), the plugin chain runs
 * on every getConnection(). This causes 4-5 extra SQL queries per borrow.
 *
 * <p><b>Why external pooling is unaffected:</b> External HikariCP only invokes the plugin chain
 * when creating NEW physical connections, not on pool borrows.
 */
class AuroraConnectionTrackerPluginAliasQueryBugTest {

  private static final String CUSTOM_CLUSTER_HOST =
      "my-cluster.cluster-custom-xyz123.us-east-1.rds.amazonaws.com";

  @Mock Connection mockConnection;
  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock PluginService mockPluginService;
  @Mock Statement mockStatement;
  @Mock ResultSet mockResultSet;
  @Mock OpenedConnectionTracker mockTracker;

  private final AtomicInteger fillAliasesCallCount = new AtomicInteger(0);

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    fillAliasesCallCount.set(0);

    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);
    when(mockDialect.getHostAliasQuery()).thenReturn("SELECT CONCAT(@@hostname, ':', @@port)");
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockPluginService.getTargetDriverDialect()).thenReturn(mockTargetDriverDialect);
    when(mockTargetDriverDialect.getNetworkBoundMethodNames(any())).thenReturn(new HashSet<>());

    doAnswer(invocation -> {
      fillAliasesCallCount.incrementAndGet();
      return null;
    }).when(mockPluginService).fillAliases(any(Connection.class), any(HostSpec.class));
  }

  /**
   * BUG: Repeated plugin.connect() calls with same Connection trigger alias queries every time.
   * This simulates internal pooling where the same physical connection is returned on each borrow.
   */
  @Test
  void testAliasQueriesRunOnEveryConnectCallForClusterEndpoint() throws SQLException {
    HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(CUSTOM_CLUSTER_HOST)
        .port(3306)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    assertEquals(RdsUrlType.RDS_CUSTOM_CLUSTER, rdsUtils.identifyRdsType(CUSTOM_CLUSTER_HOST));
    assertTrue(rdsUtils.identifyRdsType(CUSTOM_CLUSTER_HOST).isRdsCluster());

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService, new Properties(), rdsUtils, mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    // Simulate 5 getConnection() calls that return the same pooled connection
    for (int i = 0; i < 5; i++) {
      plugin.connect("jdbc:mysql://", hostSpec, new Properties(), i == 0, connectFunc);
    }

    // BUG: fillAliases called 5 times instead of 1
    assertEquals(5, fillAliasesCallCount.get(),
        "BUG: fillAliases() called on every connect() even for same connection. " +
        "Expected: 1 (first call only). Actual: " + fillAliasesCallCount.get());
  }

  /**
   * Correct behavior: Instance endpoints do NOT trigger alias queries.
   */
  @Test
  void testInstanceEndpointDoesNotTriggerAliasQueries() throws SQLException {
    String instanceHost = "my-instance.xyz123.us-east-1.rds.amazonaws.com";
    HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(instanceHost)
        .port(3306)
        .build();

    RdsUtils rdsUtils = new RdsUtils();
    assertEquals(RdsUrlType.RDS_INSTANCE, rdsUtils.identifyRdsType(instanceHost));

    AuroraConnectionTrackerPlugin plugin = new AuroraConnectionTrackerPlugin(
        mockPluginService, new Properties(), rdsUtils, mockTracker);

    JdbcCallable<Connection, SQLException> connectFunc = () -> mockConnection;

    for (int i = 0; i < 5; i++) {
      plugin.connect("jdbc:mysql://", hostSpec, new Properties(), i == 0, connectFunc);
    }

    assertEquals(0, fillAliasesCallCount.get(), "Instance endpoints should not trigger alias queries");
  }
}
