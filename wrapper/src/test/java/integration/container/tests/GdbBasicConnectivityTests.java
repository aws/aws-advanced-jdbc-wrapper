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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTags;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;

/**
 * Reaches every region of an Aurora global database, and checks each is what it claims to be.
 *
 * <p>The first thing worth testing about a global database, and the cheapest: before any failover or
 * read/write-splitting behaviour means anything, the environment has to actually be multi-region and every
 * region has to be reachable from where the tests run. Both are easy to get silently wrong. A secondary region
 * whose security group does not allow the runner provisions healthily and times out; a topology published with
 * the primary's endpoint suffix for every region produces hostnames that do not resolve.
 *
 * <p>It also pins the property that makes a secondary region a secondary region: it holds no writer. Every test
 * class after this one depends on that - reader-to-reader failover, read/write splitting reaching across regions
 * - and asserting it here means those classes fail for their own reasons rather than this one.
 *
 * <h2>Why these connections bypass the plugins</h2>
 *
 * <p>Mostly no plugins, deliberately. This class is checking the <em>environment</em>: that the endpoints exist,
 * resolve, accept a connection and report the region and role expected of them. Bringing the GDB plugins in
 * would test the driver at the same time, and when something failed there would be two candidates for the cause.
 * The plugin behaviour has its own classes.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA_GLOBAL)
@EnableOnTestFeature(TestEnvironmentFeatures.GLOBAL_DATABASE)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Tag(TestTags.GDB)
@Order(3)
public class GdbBasicConnectivityTests {

  private static final Logger LOGGER = Logger.getLogger(GdbBasicConnectivityTests.class.getName());

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(
        info,
        "This environment declares GLOBAL_DATABASE but published no topology, so there is nothing to reach.");
    return info;
  }

  @TestTemplate
  public void test_topologySpansMoreThanOneRegion() {
    final TestGlobalDatabaseInfo info = global();

    LOGGER.info("Global database " + info.getGlobalClusterIdentifier() + " spans " + info.getRegionNames()
        + ", primary " + info.getPrimaryRegion());

    assertTrue(info.getRegionNames().size() >= 2,
        "A global database environment must span at least two regions; this one reports "
            + info.getRegionNames());
    assertFalse(info.getSecondaryRegions().isEmpty(), "no secondary region was published");
    assertNotNull(info.getPrimaryRegion());

    // Each region needs its own suffix, and they have to differ. Equal suffixes would mean the topology was
    // published from one region's endpoints, which produces host templates that resolve to the wrong region -
    // or to nothing.
    for (final TestRegionalClusterInfo cluster : info.getRegions()) {
      assertNotNull(cluster.getInstanceEndpointSuffix(),
          "region " + cluster.getRegion() + " published no instance endpoint suffix");
      assertTrue(cluster.getInstanceEndpointSuffix().contains(cluster.getRegion()),
          "the instance endpoint suffix for " + cluster.getRegion() + " is "
              + cluster.getInstanceEndpointSuffix() + ", which does not name that region");
    }
  }

  @TestTemplate
  public void test_hostPatternsCoverEveryRegion() {
    final TestGlobalDatabaseInfo info = global();
    final String patterns = info.getInstanceHostPatterns();

    LOGGER.info("globalClusterInstanceHostPatterns=" + patterns);

    // The value every GDB plugin configuration needs. One template per region, or the driver cannot address
    // instances it has only seen the identifiers of - and the failure looks like broken topology discovery.
    assertEquals(info.getRegions().size(), patterns.split(",").length,
        "one host pattern per region was expected, got " + patterns);

    for (final TestRegionalClusterInfo cluster : info.getRegions()) {
      assertTrue(patterns.contains("?." + cluster.getInstanceEndpointSuffix()),
          "the patterns " + patterns + " do not cover " + cluster.getRegion());
    }
  }

  @TestTemplate
  public void test_everyRegionAcceptsAConnectionOnItsWriterEndpoint(final TestDriver testDriver)
      throws SQLException {

    // Every region's writer endpoint answers, including the secondaries: that is what makes a switchover
    // invisible to a connection string. A test that expected a secondary's writer endpoint to refuse would be
    // asserting something Aurora does not do.
    for (final TestRegionalClusterInfo cluster : global().getRegions()) {
      assertRegionAnswers(testDriver, cluster, cluster.getClusterEndpoint());
    }
  }

  @TestTemplate
  public void test_everyRegionAcceptsAConnectionOnItsReaderEndpoint(final TestDriver testDriver)
      throws SQLException {

    for (final TestRegionalClusterInfo cluster : global().getRegions()) {
      assertRegionAnswers(testDriver, cluster, cluster.getClusterReadOnlyEndpoint());
    }
  }

  @TestTemplate
  public void test_everyInstanceIsReachableInItsOwnRegion(final TestDriver testDriver) throws SQLException {
    // The check that catches a security group missing in a secondary region, and the reason it is per instance
    // rather than per cluster: a cluster endpoint resolves to whichever instance is currently behind it, so a
    // single unreachable instance can hide behind a healthy one.
    for (final TestRegionalClusterInfo cluster : global().getRegions()) {
      for (final String instance : cluster.getInstanceIdentifiers()) {
        assertRegionAnswers(testDriver, cluster, cluster.getInstanceEndpoint(instance));
      }
    }
  }

  @TestTemplate
  public void test_secondaryRegionsHoldNoWriter(final TestDriver testDriver) throws SQLException {
    // The defining property of a secondary region, and the assumption every later GDB class rests on. Asserted
    // by asking the server rather than by reading the topology, because the topology is a provisioning-time
    // snapshot and this is about what the database will actually do with a write.
    for (final TestRegionalClusterInfo cluster : global().getSecondaryRegions()) {
      try (Connection conn = connect(testDriver, cluster, cluster.getClusterEndpoint())) {
        assertTrue(isReadOnly(conn),
            "the writer endpoint in the secondary region " + cluster.getRegion()
                + " reports a writable server, so this environment's primary region is not what it claims");
      }
    }
  }

  @TestTemplate
  public void test_theWrapperConnectsWithThePublishedHostPatterns(final TestDriver testDriver)
      throws SQLException {

    // What the published patterns are for, and the reason this test exists at all: connecting to a global
    // database through the wrapper fails without them, with "Parameter globalClusterInstanceHostPatterns is
    // required for Aurora Global Database" - and it fails that way even with no plugins enabled, because the
    // requirement comes from topology discovery rather than from any plugin. So a value that looks right is not
    // enough; something has to connect with it.
    final TestGlobalDatabaseInfo info = global();

    for (final TestRegionalClusterInfo cluster : info.getRegions()) {
      final Properties props = ConnectionStringHelper.getDefaultProperties();
      GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
          props, info.getInstanceHostPatterns());
      DriverHelper.setConnectTimeout(testDriver, props, 20, TimeUnit.SECONDS);
      DriverHelper.setSocketTimeout(testDriver, props, 20, TimeUnit.SECONDS);

      final String url = ConnectionStringHelper.getWrapperUrl(
          cluster.getClusterEndpoint(),
          cluster.getPort(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      LOGGER.finest("Connecting through the wrapper to " + url);

      try (Connection conn = DriverManager.getConnection(url, props);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery("SELECT 1")) {

        rs.next();
        assertEquals(1, rs.getInt(1), "the wrapper could not query " + cluster.getRegion());
      }
    }
  }

  private void assertRegionAnswers(
      final TestDriver testDriver, final TestRegionalClusterInfo cluster, final String host)
      throws SQLException {

    LOGGER.finest("Connecting to " + host + " in " + cluster.getRegion());

    try (Connection conn = connect(testDriver, cluster, host);
        Statement statement = conn.createStatement()) {

      assertTrue(conn.isValid(5), host + " in " + cluster.getRegion() + " did not report a valid connection");

      try (ResultSet rs = statement.executeQuery("SELECT 1")) {
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  /**
   * Opens a connection with the target driver, not through the wrapper.
   *
   * <p>Deliberately the plain driver. This class asks whether the <em>environment</em> is right - whether every
   * endpoint in every region exists, resolves and accepts a connection - and the wrapper cannot answer that
   * question without also being configured for a global database. It refuses to connect at all until
   * {@code globalClusterInstanceHostPatterns} is set, so a wrapper connection here would conflate "the region is
   * unreachable" with "the driver was not told about the topology". Those are tested separately, one method
   * apart.
   */
  private Connection connect(
      final TestDriver testDriver, final TestRegionalClusterInfo cluster, final String host)
      throws SQLException {

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 20, TimeUnit.SECONDS);

    return DriverManager.getConnection(
        ConnectionStringHelper.getUrl(
            testDriver,
            host,
            cluster.getPort(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()),
        props);
  }

  /**
   * Reports whether the server behind this connection refuses writes.
   *
   * <p>Asked per engine because the two say it differently, and neither answers through JDBC metadata: a
   * secondary Aurora cluster accepts the connection and rejects the write, so {@code Connection.isReadOnly}
   * reports the driver's own flag rather than the server's state.
   */
  private boolean isReadOnly(final Connection conn) throws SQLException {
    final String query =
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
            ? "SELECT pg_is_in_recovery()"
            : "SELECT @@innodb_read_only";

    try (Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      rs.next();
      final String value = rs.getString(1);
      return "t".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value) || "1".equals(value);
    }
  }
}
