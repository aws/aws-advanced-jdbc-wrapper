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
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
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
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.gdbfailover.GlobalDbFailoverConnectionPlugin;

/**
 * Moves an Aurora global database's writer to another region under an open connection.
 *
 * <p>The behaviour the {@code gdbFailover} plugin exists for, and the one thing no single-region environment can
 * test: the writer leaves the region the connection is in. Everything else the plugin does - home and out-of-home
 * modes, region-filtered reader selection - is a decision made <em>about</em> this event, so this is the test the
 * rest are shaped by.
 *
 * <p>A planned switchover rather than an unplanned failover, deliberately. {@code SwitchoverGlobalCluster} waits
 * for the target to catch up and then swaps the roles, so no data is lost and the topology survives; an
 * unplanned {@code FailoverGlobalCluster} detaches the old primary and leaves it to be rebuilt, which a shared
 * environment can absorb once and then not again. Testing the graceful path repeatedly is worth more than
 * testing the destructive one once.
 *
 * <h2>Ordered last, and put back afterwards</h2>
 *
 * <p>This class changes the environment: after it runs, the region that was primary is not. Every other GDB class
 * assumes the layout the topology was published with - which region holds the writer, and therefore which
 * regions hold only readers - so this runs after them and switches back when it is done. The switch back is
 * best-effort: if it fails, the environment is still valid, just mirrored, and it is deleted at the end of the
 * run anyway.
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
@Order(90)
public class GdbCrossRegionSwitchoverTest {

  private static final Logger LOGGER = Logger.getLogger(GdbCrossRegionSwitchoverTest.class.getName());

  /**
   * How long a switchover may take.
   *
   * <p>Generous because the operation is not only a role swap: RDS stops writes and waits for the target region
   * to catch up first, and how long that takes depends on replication lag across a continent. A tighter timeout
   * would fail runs for being slow rather than for being wrong.
   */
  private static final Duration SWITCHOVER_TIMEOUT = Duration.ofMinutes(20);

  /**
   * How long to wait for the home region to be able to serve a writer again after the switch back.
   *
   * <p>Shorter than {@link #SWITCHOVER_TIMEOUT} because this is not waiting for the switchover, which has
   * already been confirmed by the time this applies - only for the regional clusters to finish following it.
   */
  private static final Duration HOME_WRITABLE_TIMEOUT = Duration.ofMinutes(10);

  private static final String TABLE = "gdb_switchover_test";

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  @TestTemplate
  public void test_switchover_movesTheWriterToAnotherRegion(final TestDriver testDriver) throws SQLException {
    final TestGlobalDatabaseInfo info = global();
    final String homeRegion = info.getPrimaryRegion();
    final TestRegionalClusterInfo target = info.getFirstSecondaryRegion();

    LOGGER.info("Switching " + info.getGlobalClusterIdentifier() + " from " + homeRegion
        + " to " + target.getRegion());

    // strict-writer in both modes: this connection wants a writer wherever it ends up, which is the
    // configuration a write workload uses and the one that makes the assertion below meaningful. The home region
    // is the region we start in, so the switchover takes us out of home - inactiveHomeFailoverMode is the mode
    // that then applies.
    final Properties props = gdbProps(testDriver, info, homeRegion);
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-writer");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-writer");

    final String url = ConnectionStringHelper.getWrapperUrl(
        info.getRegion(homeRegion).getClusterEndpoint(),
        info.getRegion(homeRegion).getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    try (Connection conn = DriverManager.getConnection(url, props)) {
      final String before = auroraUtil.queryInstanceId(conn);
      assertTrue(info.getRegion(homeRegion).getInstanceIdentifiers().contains(before),
          "expected to start on an instance in " + homeRegion + ", but " + before + " is not one of "
              + info.getRegion(homeRegion).getInstanceIdentifiers());

      switchover(info, target.getRegion());

      // The first use of the connection after the writer moved must report failover rather than silently
      // continuing against a server that is now a reader. That is the plugin's contract, and the reason a
      // driver is needed here at all: the endpoint did not change, only what is behind it.
      assertThrows(FailoverSuccessSQLException.class, () -> auroraUtil.queryInstanceId(conn));

      final String after = auroraUtil.queryInstanceId(conn);
      LOGGER.info("After switchover the connection is on " + after);

      assertTrue(target.getInstanceIdentifiers().contains(after),
          "after the switchover the connection should be on an instance in " + target.getRegion()
              + ", but it is on " + after);

      // And it must be usable as a writer, which is what strict-writer asked for. A connection that failed over
      // to a reader would pass every assertion above and fail the first write a real application made.
      assertWritable(conn);

    } finally {
      switchBack(info, homeRegion, testDriver);
    }
  }

  /**
   * Builds the properties a connection to a global database needs.
   *
   * <p>{@code globalClusterInstanceHostPatterns} is not optional: without it the driver refuses to connect to a
   * global database at all, because it cannot turn the instance identifiers the topology query returns into
   * addressable hosts. The dialect is named explicitly rather than left to detection, since that is what the
   * plugin's own documentation configures for a global database.
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

  /**
   * Switches the writer to another region, retrying while RDS refuses to start.
   *
   * <p>The retrying is not decoration. This test runs once per driver, and the second pass's forward
   * switchover arrives while the first pass's switch-back is still settling - RDS answers "a switchover is
   * currently in progress" or "the source DB cluster is in the modifying state" for a while after the previous
   * one reports complete, because the global cluster's state changes before its members' do. A single attempt
   * therefore fails on every multi-driver run, and only the second driver's pass, which reads like a driver
   * difference rather than the scheduling artifact it is.
   */
  private void switchover(final TestGlobalDatabaseInfo info, final String toRegion) {
    final String targetArn =
        auroraUtil.getGlobalClusterMemberArn(info.getGlobalClusterIdentifier(), toRegion);

    final long deadline = System.nanoTime() + SWITCHOVER_TIMEOUT.toNanos();
    RuntimeException lastRefusal = null;

    while (System.nanoTime() < deadline) {
      try {
        auroraUtil.switchoverGlobalCluster(info.getGlobalClusterIdentifier(), targetArn);
        auroraUtil.waitUntilGlobalClusterPrimaryRegionIs(
            info.getGlobalClusterIdentifier(), toRegion, SWITCHOVER_TIMEOUT);
        return;

      } catch (final RuntimeException e) {
        lastRefusal = e;
        LOGGER.info("Switchover to " + toRegion + " not possible yet: " + e.getMessage());
      }

      try {
        TimeUnit.SECONDS.sleep(15);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting to switch over to " + toRegion, e);
      }
    }

    throw new RuntimeException("Could not switch " + info.getGlobalClusterIdentifier() + " over to "
        + toRegion + " within " + SWITCHOVER_TIMEOUT.toMinutes() + " minutes.", lastRefusal);
  }

  /**
   * Returns the primary region to where it started.
   *
   * <p>Best-effort in the end - the environment is valid either way, and failing teardown here would replace
   * whatever the test found with a message about a switchover - but persistent about it, because a mirrored
   * topology is not as harmless as it first looked. Teardown deletes region by region, primary last, and RDS
   * refuses to delete the actual master's last instance while a replica exists; on a run where this method gave
   * up, that refusal ate the whole teardown budget and left clusters billing in two regions.
   *
   * <p>The retrying matters for a specific reason: the switchover this test performed reports complete on the
   * <em>global</em> cluster while the regional clusters are still {@code modifying}, and RDS refuses a new
   * switchover until they settle - "the source DB cluster is in the modifying state". A single immediate attempt
   * therefore fails essentially every time, which is exactly how this was found.
   */
  private void switchBack(
      final TestGlobalDatabaseInfo info, final String homeRegion, final TestDriver testDriver) {

    final long deadline = System.nanoTime() + SWITCHOVER_TIMEOUT.toNanos();
    String lastRefusal = "not attempted";

    while (System.nanoTime() < deadline) {
      try {
        if (homeRegion.equals(auroraUtil.getGlobalClusterPrimaryRegion(info.getGlobalClusterIdentifier()))) {
          LOGGER.info(info.getGlobalClusterIdentifier() + " is back on " + homeRegion);
          awaitHomeRegionWritable(info, homeRegion, testDriver);
          return;
        }
        LOGGER.info("Switching " + info.getGlobalClusterIdentifier() + " back to " + homeRegion);
        switchover(info, homeRegion);
        awaitHomeRegionWritable(info, homeRegion, testDriver);
        return;

      } catch (final RuntimeException e) {
        // Almost always the modifying-state refusal above; anything rarer is equally well served by waiting
        // and asking again, since the loop re-reads the primary region on every pass.
        lastRefusal = e.getMessage();
        LOGGER.info("Switch-back not possible yet: " + lastRefusal);
      }

      try {
        TimeUnit.SECONDS.sleep(15);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    LOGGER.warning("Could not switch " + info.getGlobalClusterIdentifier() + " back to " + homeRegion
        + " within " + SWITCHOVER_TIMEOUT.toMinutes() + " minutes; last refusal: " + lastRefusal
        + ". Teardown must not assume the provisioning-time primary still holds the writer.");
  }

  /**
   * Waits until the home region's cluster endpoint actually fronts a writer.
   *
   * <p>The control-plane flag is not that signal, and the difference is what made this test fail on its second
   * driver. {@code DescribeGlobalClusters} reports the role swap as soon as RDS records it, while the regional
   * clusters are still {@code modifying} and the home region's writer cluster endpoint still answers as a
   * reader. Returning on the flag alone hands the next parameterized case a topology that has not settled: it
   * opens its first connection through that endpoint with the {@code initialConnection} plugin demanding a
   * writer, gets none, and gives up after the plugin's default 30 seconds - which reads as a difference between
   * the two drivers and is really this test's own leftovers.
   *
   * <p>Probed with a plain connection rather than the wrapper, because the wrapper is the thing under test. A
   * driver that resolved this correctly would mask an environment that had not settled, and one that did not
   * would be blamed for the environment.
   *
   * <p>Best-effort, like the switch back it completes: a slow region is not a finding about the driver, and
   * throwing here would replace whatever the test found with a message about an endpoint.
   */
  private void awaitHomeRegionWritable(
      final TestGlobalDatabaseInfo info, final String homeRegion, final TestDriver testDriver) {

    final TestRegionalClusterInfo home = info.getRegion(homeRegion);
    final String url = ConnectionStringHelper.getUrl(
        testDriver,
        home.getClusterEndpoint(),
        home.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    final Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    DriverHelper.setConnectTimeout(testDriver, props, 10, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 10, TimeUnit.SECONDS);

    final long deadline = System.nanoTime() + HOME_WRITABLE_TIMEOUT.toNanos();
    String lastReason = "not attempted";

    while (System.nanoTime() < deadline) {
      try (Connection conn = DriverManager.getConnection(url, props);
          Statement statement = conn.createStatement();
          ResultSet rs = statement.executeQuery(readOnlyProbe())) {

        if (rs.next() && !rs.getBoolean(1)) {
          LOGGER.info(homeRegion + " serves a writer again");
          return;
        }
        lastReason = "the cluster endpoint still answers as a reader";

      } catch (final SQLException e) {
        lastReason = e.getMessage();
      }

      LOGGER.info("Waiting for " + homeRegion + " to serve a writer: " + lastReason);
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    LOGGER.warning(homeRegion + " still did not serve a writer after " + HOME_WRITABLE_TIMEOUT.toMinutes()
        + " minutes; last reason: " + lastReason + ". A test that connects there next may fail for that "
        + "reason rather than its own.");
  }

  /**
   * Returns a query that answers whether the server it runs on is read-only, which is to say a reader.
   */
  private String readOnlyProbe() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? "SELECT pg_is_in_recovery()"
        : "SELECT @@innodb_read_only";
  }

  private void assertWritable(final Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      statement.execute("DROP TABLE IF EXISTS " + TABLE);
      statement.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
      statement.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");

      try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
        rs.next();
        assertEquals(1, rs.getInt(1), "the connection could not write after the switchover");
      }

      statement.execute("DROP TABLE IF EXISTS " + TABLE);
    }
  }
}
