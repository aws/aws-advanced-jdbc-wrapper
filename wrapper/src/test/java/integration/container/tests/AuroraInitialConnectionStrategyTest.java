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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPlugin;

/**
 * Integration tests for the {@link AuroraInitialConnectionStrategyPlugin} round-robin reader
 * distribution during concurrent connection establishment.
 *
 * <p>Reproduces the scenario reported in
 * <a href="https://github.com/aws/aws-advanced-jdbc-wrapper/issues/1968">issue #1968</a>: when many
 * connections are opened simultaneously (e.g. a connection pool prefill) before the cluster topology
 * has been discovered, all connections used to be routed to a single reader because the plugin fell
 * back to connecting via the DNS-resolved reader cluster endpoint. Setting
 * {@code waitForInitialTopologyMs} makes the concurrent connections block on the shared per-cluster
 * topology monitor until topology is available, after which the {@code roundRobin} strategy can
 * distribute them across the available readers.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@MakeSureFirstInstanceWriter
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(17)
public class AuroraInitialConnectionStrategyTest {

  private static final Logger LOGGER =
      Logger.getLogger(AuroraInitialConnectionStrategyTest.class.getName());

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static final int CONCURRENT_CONNECTIONS = 20;

  protected Properties getProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.PLUGINS.name, "initialConnection");
    props.setProperty(
        AuroraInitialConnectionStrategyPlugin.READER_HOST_SELECTOR_STRATEGY.name,
        RoundRobinHostSelector.STRATEGY_ROUND_ROBIN);
    return props;
  }

  /**
   * Opens {@link #CONCURRENT_CONNECTIONS} connections simultaneously against the reader cluster
   * endpoint with {@code waitForInitialTopologyMs} enabled and asserts that the connections are
   * distributed across more than one reader instance.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  public void test_concurrentConnections_distributeAcrossReaders() throws Exception {
    // Start from a cold topology cache so concurrent connections race exactly as during a real
    // connection-pool prefill at application startup.
    RoundRobinHostSelector.clearCache();

    final Properties props = getProps();
    props.setProperty(AuroraInitialConnectionStrategyPlugin.WAIT_FOR_INITIAL_TOPOLOGY_MS.name, "30000");

    final String url = ConnectionStringHelper.getWrapperReaderClusterUrl();

    final Map<String, LongAdder> connectionsPerInstance =
        openConnectionsConcurrently(url, props, CONCURRENT_CONNECTIONS);

    LOGGER.info("Concurrent connection distribution: " + connectionsPerInstance);

    assertTrue(
        connectionsPerInstance.size() > 1,
        "Expected concurrent connections to be distributed across more than one reader instance, "
            + "but they all landed on a single instance: " + connectionsPerInstance);
  }

  /**
   * Opens {@link #CONCURRENT_CONNECTIONS} connections simultaneously against the reader cluster
   * endpoint with default settings (i.e. {@code waitForInitialTopologyMs} not set) and verifies the
   * legacy behavior reported in issue #1968: because the plugin falls back to the DNS-resolved reader
   * cluster endpoint before topology has been discovered, all concurrent connections land on a single
   * reader instance.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 3)
  public void test_concurrentConnections_defaultBehaviorLandsOnSingleReader() throws Exception {
    // Start from a cold topology cache to mirror a real connection-pool prefill at application startup.
    RoundRobinHostSelector.clearCache();

    // Default properties: the new waitForInitialTopologyMs parameter is intentionally NOT set.
    final Properties props = getProps();

    final String url = ConnectionStringHelper.getWrapperReaderClusterUrl();

    final Map<String, LongAdder> connectionsPerInstance =
        openConnectionsConcurrently(url, props, CONCURRENT_CONNECTIONS);

    LOGGER.info("Default (no waitForInitialTopologyMs) connection distribution: " + connectionsPerInstance);

    // All connections succeeded.
    long totalConnections = connectionsPerInstance.values().stream().mapToLong(LongAdder::sum).sum();
    assertEquals(
        CONCURRENT_CONNECTIONS,
        totalConnections,
        "Expected all concurrent connections to succeed with default settings.");

    // All connections should have landed on reader instances (the writer is excluded by the
    // reader cluster endpoint / reader host selection).
    final String writerInstanceId =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().get(0).getInstanceId();
    assertFalse(
        connectionsPerInstance.containsKey(writerInstanceId),
        "Expected no connections to land on the writer instance '" + writerInstanceId
            + "', but found: " + connectionsPerInstance);

    // Legacy behavior: without waitForInitialTopologyMs, all concurrent connections fall back to the
    // DNS-resolved reader cluster endpoint and land on a single reader instance.
    assertEquals(
        1,
        connectionsPerInstance.size(),
        "Expected all concurrent connections to land on a single reader instance with default "
            + "settings, but they were distributed across: " + connectionsPerInstance);
  }

  private Map<String, LongAdder> openConnectionsConcurrently(
      final String url, final Properties props, final int numConnections) throws InterruptedException {

    final Map<String, LongAdder> connectionsPerInstance = new ConcurrentHashMap<>();
    final AtomicInteger failureCount = new AtomicInteger(0);
    final ExecutorService executor = Executors.newFixedThreadPool(numConnections, r -> {
      final Thread thread = new Thread(r);
      thread.setDaemon(true);
      return thread;
    });

    // Release all threads at the same time to maximize contention on the cold topology cache.
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numConnections);

    try {
      for (int i = 0; i < numConnections; i++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            try (final Connection conn = DriverManager.getConnection(url, props)) {
              final String instanceId = auroraUtil.queryInstanceId(conn);
              connectionsPerInstance
                  .computeIfAbsent(instanceId, k -> new LongAdder())
                  .increment();
            }
          } catch (final Exception ex) {
            failureCount.incrementAndGet();
            LOGGER.warning("Failed to open or query a concurrent connection: " + ex.getMessage());
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      if (!doneLatch.await(2, TimeUnit.MINUTES)) {
        fail("Timed out waiting for concurrent connections to complete.");
      }
    } finally {
      executor.shutdownNow();
    }

    if (failureCount.get() > 0) {
      fail(failureCount.get() + " out of " + numConnections + " concurrent connections failed.");
    }

    return connectionsPerInstance;
  }
}
