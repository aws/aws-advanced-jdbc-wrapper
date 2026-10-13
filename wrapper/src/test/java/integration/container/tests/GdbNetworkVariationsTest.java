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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTags;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.DialectCodes;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.gdbfailover.GlobalDbFailoverConnectionPlugin;

/**
 * What the driver does when some regions of a global database are not usable.
 *
 * <p>A cross-region deployment has a failure mode a single-region one does not: the database is healthy and the
 * network is not. A peering link is down, a region is firewalled off, an application is deployed somewhere that is
 * only allowed to talk to its own region. The database's topology still lists every region, so unless the driver is
 * told otherwise it will happily reconnect to a host it cannot reach - and the symptom is a hung connection during
 * a failover, which is the worst possible time to be diagnosing routing.
 *
 * <p>{@code gdbAccessibleRegions} is how that is expressed, and it is worth stressing that it is a statement about
 * the <em>network</em> rather than about the database. Nothing about the global database changes when it is set;
 * only the set of hosts the driver considers usable does.
 *
 * <h2>Two kinds of test here</h2>
 *
 * <p>Most of these are configuration tests: the driver is told which regions it may reach and the assertion is
 * that it honours that - connecting normally when the restriction permits it, and refusing up front rather than at
 * failover time when the configuration cannot be satisfied.
 *
 * <p>The last one is not. It makes a region genuinely unreachable at the packet level and asserts what the driver
 * does about it, which is a different question: configuration tests prove the driver obeys an instruction, and
 * this proves it survives the situation the instruction describes. It works because the transparent gateway is the
 * container's default route, so every packet to every region already passes through it - all that was needed was a
 * name per region to impair by, which {@code ProxyHelper.disableRegionConnectivity} now provides.
 *
 * <p>The complementary case - a restricted driver losing a host inside its own region - is covered by
 * {@code GdbSecondaryInRegionFailoverTest}, which takes a single instance away for real.
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
@Order(70)
public class GdbNetworkVariationsTest {

  private static final Logger LOGGER = Logger.getLogger(GdbNetworkVariationsTest.class.getName());

  /** A region no test environment is built in, used to name somewhere the database demonstrably is not. */
  private static final String UNRELATED_REGION = "eu-west-1";

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  @TestTemplate
  public void test_noRestriction_usesTheWholeTopology(final TestDriver testDriver) throws SQLException {
    final TestRegionalClusterInfo home = global().getFirstSecondaryRegion();

    // The default, stated as a test so the others have a baseline. Leaving gdbAccessibleRegions unset means "every
    // region is reachable", which is the right default for a deployment with working peering and the reason the
    // restriction has to be opted into.
    final Properties props = gdbProps(testDriver, home.getRegion());

    try (Connection conn = connect(home, props)) {
      assertTrue(conn.isValid(10));
      LOGGER.finest("Connected without a region restriction, on " + auroraUtil.queryInstanceId(conn));
    }
  }

  @TestTemplate
  public void test_restrictedToItsOwnRegion_connectsNormally(final TestDriver testDriver) throws SQLException {
    final TestRegionalClusterInfo home = global().getFirstSecondaryRegion();

    // The realistic deployment: an application that can only reach the region it runs in. It must be able to work
    // normally - a restriction that made ordinary operation fail would be unusable, and the failure would look
    // like a broken database rather than a narrowed host list.
    final Properties props = gdbProps(testDriver, home.getRegion());
    props.setProperty(PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, home.getRegion());

    try (Connection conn = connect(home, props)) {
      final String instanceId = auroraUtil.queryInstanceId(conn);
      assertTrue(home.getInstanceIdentifiers().contains(instanceId),
          "a connection restricted to " + home.getRegion() + " must be in it, but it is on " + instanceId);
    }
  }

  @TestTemplate
  public void test_restrictionExcludingTheHomeRegion_isRefusedAtConnect(final TestDriver testDriver) {
    final TestRegionalClusterInfo home = global().getFirstSecondaryRegion();
    final String elsewhere = global().getPrimaryRegion();

    // A configuration that cannot be satisfied: the region this connection calls home is not among the regions it
    // is allowed to reach. Refusing at connect time is the whole value - the alternative is a driver that works
    // until the first failover and then has nowhere legal to go, at which point the cause is two config files away
    // from the symptom.
    final Properties props = gdbProps(testDriver, home.getRegion());
    props.setProperty(PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, elsewhere);

    final SQLException e = assertThrows(SQLException.class, () -> {
      try (Connection conn = connect(home, props)) {
        conn.isValid(10);
      }
    });

    LOGGER.finest("Refused, as it should be: " + e.getMessage());
  }

  @TestTemplate
  public void test_restrictionNamingOnlyARegionTheDatabaseDoesNotSpan_isRefused(final TestDriver testDriver) {
    final TestRegionalClusterInfo home = global().getFirstSecondaryRegion();

    // The same contradiction arrived at by a different mistake: a region list that is not wrong so much as
    // irrelevant, which is what a copied configuration looks like. It should fail the same way rather than, say,
    // being treated as "no usable regions, carry on".
    final Properties props = gdbProps(testDriver, home.getRegion());
    props.setProperty(PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, UNRELATED_REGION);

    assertThrows(SQLException.class, () -> {
      try (Connection conn = connect(home, props)) {
        conn.isValid(10);
      }
    });
  }

  @TestTemplate
  public void test_restrictionListingEveryRegion_isTheSameAsNoRestriction(final TestDriver testDriver)
      throws SQLException {

    final TestRegionalClusterInfo home = global().getFirstSecondaryRegion();

    // Listing everything should be equivalent to saying nothing. Worth pinning because the two are handled by
    // different code paths - a null list short-circuits the filter, a full list runs it - and a bug in the filter
    // would show up here and nowhere else.
    final Properties props = gdbProps(testDriver, home.getRegion());
    props.setProperty(
        PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, String.join(",", global().getRegionNames()));

    try (Connection conn = connect(home, props)) {
      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_theWriterRegionBecomingUnreachable_isReportedNotHidden(final TestDriver testDriver)
      throws SQLException {

    final TestGlobalDatabaseInfo info = global();
    final TestRegionalClusterInfo home = info.getFirstSecondaryRegion();
    final String writerRegion = info.getPrimaryRegion();

    // A reader in a secondary region, allowed to fail over anywhere. Then the region holding the writer stops
    // answering entirely - the cross-region equivalent of losing a host, and the case that has never been
    // testable here before.
    final Properties props = gdbProps(testDriver, home.getRegion());

    try (Connection conn = connect(home, props)) {
      final String before = auroraUtil.queryInstanceId(conn);
      assertTrue(home.getInstanceIdentifiers().contains(before),
          "expected to start in " + home.getRegion() + ", but the connection is on " + before);

      ProxyHelper.disableRegionConnectivity(writerRegion);
      try {
        // The connection itself is in a region that is still up, so it must keep working. Losing sight of the
        // writer's region is not a reason to drop a healthy local reader, and a driver that tore this connection
        // down would turn a remote outage into a local one.
        final String after = auroraUtil.queryInstanceId(conn);
        LOGGER.info("With " + writerRegion + " unreachable the connection is still on " + after);

        assertTrue(home.getInstanceIdentifiers().contains(after),
            "the connection should have stayed in the reachable region " + home.getRegion()
                + ", but it is on " + after);

      } finally {
        // Restored inside the test rather than in an @AfterEach: the impairment is this test's own, and the
        // classes that run after it connect to this region expecting it to answer.
        ProxyHelper.enableRegionConnectivity(writerRegion);
      }
    }
  }

  private Connection connect(final TestRegionalClusterInfo cluster, final Properties props) throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl(
        cluster.getClusterReadOnlyEndpoint(),
        cluster.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    LOGGER.finest("Connecting to " + url);
    return DriverManager.getConnection(url, props);
  }

  /** The GDB failover plugin is the one that reads the region restriction, so it is the one configured here. */
  private Properties gdbProps(final TestDriver testDriver, final String homeRegion) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.PLUGINS.set(props, "initialConnection,gdbFailover,efm2");
    DialectManager.DIALECT.set(props, dialect());

    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());
    props.setProperty(GlobalDbFailoverConnectionPlugin.FAILOVER_HOME_REGION.name, homeRegion);

    DriverHelper.setConnectTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    return props;
  }

  private static String dialect() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? DialectCodes.GLOBAL_AURORA_PG
        : DialectCodes.GLOBAL_AURORA_MYSQL;
  }
}
