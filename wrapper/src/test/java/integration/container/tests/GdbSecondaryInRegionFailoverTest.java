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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
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
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.gdbfailover.GlobalDbFailoverConnectionPlugin;

/**
 * Fails a connection over <em>within</em> a secondary region.
 *
 * <p>The case a single-region suite has no equivalent of. A secondary region of a global database holds only
 * readers, so failing over inside one is reader to reader, and the interesting question is not whether the driver
 * reconnects but <em>where</em>: staying in the region keeps latency predictable, and leaving it silently is the
 * behaviour applications configure {@code failoverHomeRegion} to prevent.
 *
 * <p>Home region is the secondary the connection is in, not the region holding the writer. That is the
 * configuration the plugin's documentation describes for a reader deployment - an application in region B reading
 * locally from a database whose primary is region A - and it means the writer being elsewhere is the
 * <em>steady state</em> rather than the exception. So {@code inactiveHomeFailoverMode} is the mode under test
 * here, where {@link GdbCrossRegionSwitchoverTest} exercises the transition into it.
 *
 * <p>Needs two instances in the secondary region, which is why the environment provisions them: with one, a
 * reader-only failover has nowhere to go and {@code strict-home-reader} would be asserting the failure path.
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
@Order(30)
public class GdbSecondaryInRegionFailoverTest {

  private static final Logger LOGGER = Logger.getLogger(GdbSecondaryInRegionFailoverTest.class.getName());

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  /** How long a rebooted reader is given to come back before the next test would trip over it. */
  private static final Duration REBOOT_RECOVERY_TIMEOUT = Duration.ofMinutes(10);

  /**
   * How long the connection is given to notice that its instance went away.
   *
   * <p>Needed because a reboot is asynchronous: {@code RebootDBInstance} returns as soon as RDS accepts the
   * request, and the instance keeps answering for some seconds afterwards. Asserting on the very first query after
   * the request therefore tests nothing - it usually succeeds against a server that has not restarted yet - so the
   * query is retried until it fails, which is what an application would experience.
   */
  private static final Duration FAILOVER_DETECTION_TIMEOUT = Duration.ofMinutes(5);

  private String takenAwayRegion;
  private String takenAwayInstance;

  /**
   * Takes one instance away, so the connection on it has to go somewhere else.
   *
   * <p>Remembered rather than restored here: the test still has assertions to make while the instance is down,
   * and waiting for it to come back is {@link #restoreTakenAwayInstance}'s job.
   */
  private void takeAway(final String region, final String instanceId) {
    LOGGER.info("Rebooting " + instanceId + " in " + region);
    this.takenAwayRegion = region;
    this.takenAwayInstance = instanceId;

    try {
      auroraUtil.rebootInstanceIn(region, instanceId);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while rebooting " + instanceId, e);
    }
  }

  /**
   * Uses the connection until it fails, and returns the failure.
   *
   * <p>The alternative - one query, one assertion - is what the first version of these tests did, and it failed
   * for the wrong reason: the reboot had been requested but not yet performed, so the query succeeded and the
   * assertion reported "nothing was thrown" as though the driver had missed the outage. Retrying makes the
   * assertion about <em>what</em> the driver does when the host goes away rather than about how quickly RDS acts
   * on an API call.
   *
   * @param conn the connection whose instance is being taken away
   * @return the first exception the connection raised
   */
  private SQLException awaitConnectionFailure(final Connection conn) {
    final long deadline = System.nanoTime() + FAILOVER_DETECTION_TIMEOUT.toNanos();
    int attempts = 0;

    while (System.nanoTime() < deadline) {
      attempts++;
      try {
        auroraUtil.queryInstanceId(conn);
      } catch (final SQLException e) {
        LOGGER.finest("The connection failed on attempt " + attempts + ": "
            + e.getClass().getSimpleName() + ": " + e.getMessage());
        return e;
      }

      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for the connection to fail.", e);
      }
    }

    return fail("The connection kept working for " + FAILOVER_DETECTION_TIMEOUT.toMinutes()
        + " minutes after " + this.takenAwayInstance + " was rebooted. Either the reboot did not take effect or "
        + "the driver never noticed the host had gone.");
  }

  /**
   * Waits for the rebooted instance before letting the next test run.
   *
   * <p>These tests share a small pool of readers, and each one reconnects to a named instance. Without this, the
   * second test would connect to a host that is still restarting and fail for a reason that has nothing to do
   * with failover. Best-effort, and warned about rather than asserted: the instance being slow to return is not a
   * finding about the driver.
   */
  @AfterEach
  public void restoreTakenAwayInstance() {
    if (this.takenAwayInstance == null) {
      return;
    }

    try {
      auroraUtil.waitUntilInstanceAvailableIn(
          this.takenAwayRegion, this.takenAwayInstance, REBOOT_RECOVERY_TIMEOUT);
    } catch (final RuntimeException e) {
      LOGGER.warning("Instance " + this.takenAwayInstance + " did not come back: " + e.getMessage());
    } finally {
      this.takenAwayInstance = null;
      this.takenAwayRegion = null;
    }
  }

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  @TestTemplate
  public void test_strictHomeReader_staysInTheSecondaryRegion(final TestDriver testDriver) throws SQLException {
    final TestGlobalDatabaseInfo info = global();
    final TestRegionalClusterInfo home = info.getFirstSecondaryRegion();

    assertTrue(home.getInstanceIdentifiers().size() >= 2,
        "this test needs at least two instances in " + home.getRegion() + " to fail over between; it has "
            + home.getInstanceIdentifiers());

    final Properties props = gdbProps(testDriver, info, home.getRegion());
    // Both modes strict-home-reader: wherever the writer is, this connection wants a reader in its own region.
    // The distinction between the two modes is not what this test is about - that is the switchover test - so
    // pinning both makes the assertion about region containment alone.
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-home-reader");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-home-reader");

    final String connectedTo = home.getInstanceIdentifiers().get(0);

    try (Connection conn = connectToInstance(home, connectedTo, props)) {
      final String before = auroraUtil.queryInstanceId(conn);
      LOGGER.info("Connected to " + before + " in " + home.getRegion());

      takeAway(home.getRegion(), before);

      final SQLException failure = awaitConnectionFailure(conn);
      assertTrue(failure instanceof FailoverSuccessSQLException,
          "the driver should have failed over to another reader and reported success, but it raised "
              + failure.getClass().getSimpleName() + ": " + failure.getMessage());

      final String after = auroraUtil.queryInstanceId(conn);
      LOGGER.info("After failover the connection is on " + after);

      assertTrue(home.getInstanceIdentifiers().contains(after),
          "strict-home-reader must keep the connection in " + home.getRegion() + ", but it moved to " + after
              + ", which is not one of " + home.getInstanceIdentifiers());
    }
  }

  @TestTemplate
  public void test_strictOutOfHomeReader_leavesTheSecondaryRegion(final TestDriver testDriver)
      throws SQLException {

    final TestGlobalDatabaseInfo info = global();
    final TestRegionalClusterInfo home = info.getFirstSecondaryRegion();

    final Properties props = gdbProps(testDriver, info, home.getRegion());
    // The mirror image, and worth testing because it is the assertion that proves the region filter is doing
    // something: the same event, the opposite mode, and the connection has to end up somewhere else.
    props.setProperty(
        GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-out-of-home-reader");
    props.setProperty(
        GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-out-of-home-reader");

    final String connectedTo = home.getInstanceIdentifiers().get(0);

    try (Connection conn = connectToInstance(home, connectedTo, props)) {
      final String before = auroraUtil.queryInstanceId(conn);
      takeAway(home.getRegion(), before);

      final SQLException failure = awaitConnectionFailure(conn);
      assertTrue(failure instanceof FailoverSuccessSQLException,
          "the driver should have failed over out of the home region and reported success, but it raised "
              + failure.getClass().getSimpleName() + ": " + failure.getMessage());

      final String after = auroraUtil.queryInstanceId(conn);
      LOGGER.info("After failover the connection is on " + after + ", expected outside " + home.getRegion());

      assertTrue(!home.getInstanceIdentifiers().contains(after),
          "strict-out-of-home-reader must leave " + home.getRegion() + ", but the connection is on " + after);
    }
  }

  @TestTemplate
  public void test_accessibleRegions_excludingEverySecondaryLeavesNowhereToGo(final TestDriver testDriver)
      throws SQLException {

    final TestGlobalDatabaseInfo info = global();
    final TestRegionalClusterInfo home = info.getFirstSecondaryRegion();

    // A network restriction expressed as configuration: the application can reach only its own region. With
    // strict-out-of-home-reader that is a contradiction - the only readers it may use are the ones it may not
    // reach - and the plugin has to say so rather than connecting somewhere it was told not to.
    final Properties props = gdbProps(testDriver, info, home.getRegion());
    props.setProperty(
        GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-out-of-home-reader");
    props.setProperty(
        GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-out-of-home-reader");
    props.setProperty(PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, home.getRegion());

    final String connectedTo = home.getInstanceIdentifiers().get(0);

    try (Connection conn = connectToInstance(home, connectedTo, props)) {
      final String before = auroraUtil.queryInstanceId(conn);
      takeAway(home.getRegion(), before);

      final SQLException e = awaitConnectionFailure(conn);

      // The assertion is that failover did not *succeed*, rather than that it failed with one particular type.
      // Which exception a driver with no legal candidate raises is an implementation detail - it could report a
      // failed failover, or refuse before attempting one - but reconnecting somewhere it was told it could not
      // reach and reporting success is the outcome gdbAccessibleRegions exists to prevent, and that is what this
      // pins down.
      assertFalse(e instanceof FailoverSuccessSQLException,
          "failover should not have succeeded: every reader it was allowed to use is in a region it was told "
              + "it cannot reach, but it reported success");

      LOGGER.finest("Failover correctly did not succeed: " + e.getClass().getSimpleName()
          + ": " + e.getMessage());
    }
  }

  /**
   * Connects to one named instance of one region.
   *
   * <p>An instance endpoint rather than a cluster endpoint, because this test is about what happens to a
   * connection on a <em>particular</em> host: a reader cluster endpoint would resolve to whichever instance the
   * region felt like, and the test would not know which one to take away.
   */
  private Connection connectToInstance(
      final TestRegionalClusterInfo cluster,
      final String instanceIdentifier,
      final Properties props) throws SQLException {

    final String url = ConnectionStringHelper.getWrapperUrl(
        cluster.getInstanceEndpoint(instanceIdentifier),
        cluster.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    LOGGER.finest("Connecting to " + url);
    return DriverManager.getConnection(url, props);
  }

  /**
   * The properties every connection to a global database needs.
   *
   * <p>See {@link GdbCrossRegionSwitchoverTest} for why each one is required.
   */
  private Properties gdbProps(
      final TestDriver testDriver, final TestGlobalDatabaseInfo info, final String homeRegion) {

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.PLUGINS.set(props, "initialConnection,gdbFailover,efm2");
    DialectManager.DIALECT.set(props, dialect());

    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, info.getInstanceHostPatterns());
    props.setProperty(GlobalDbFailoverConnectionPlugin.FAILOVER_HOME_REGION.name, homeRegion);

    DriverHelper.setConnectTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    return props;
  }

  private String dialect() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? DialectCodes.GLOBAL_AURORA_PG
        : DialectCodes.GLOBAL_AURORA_MYSQL;
  }
}
