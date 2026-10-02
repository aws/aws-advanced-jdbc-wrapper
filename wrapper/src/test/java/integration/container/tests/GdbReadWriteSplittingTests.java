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
import integration.TestEnvironmentFeatures;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTags;
import integration.container.ConnectionStringHelper;
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
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;

/**
 * Read/write splitting from inside a secondary region of a global database.
 *
 * <p>Where read/write splitting stops being a load-balancing feature and becomes a question about regions. In a
 * single-region cluster, {@code setReadOnly(false)} always has somewhere to go. In a secondary region of a global
 * database there is no writer at all, so the plugin has to decide between three different things - refuse, cross a
 * continent, or forward the write locally - and which one it picks is configuration, not topology.
 *
 * <p>Home region is the secondary region and is set explicitly rather than inferred. {@code GdbSettings} will
 * parse it out of the connection host name when it can, but a test that relied on that would be asserting against
 * hostname parsing as much as against splitting behaviour, and would change meaning if the environment moved.
 *
 * <h2>Four plugins, one set of tests</h2>
 *
 * <p>Subclasses swap the plugin and leave the assertions alone, the way {@link ReadWriteSplittingTests} and its
 * subclasses already do. The four GDB splitting plugins differ in how they find hosts (topology versus configured
 * endpoints) and in what triggers a switch (SQL inspection versus {@code setReadOnly}), but the region rules under
 * test here are shared - they live in {@code GdbSettings} and {@code GdbWriterResolver}, below all four. Running
 * the same assertions against each is what proves that.
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
@Order(40)
public class GdbReadWriteSplittingTests {

  private static final Logger LOGGER = Logger.getLogger(GdbReadWriteSplittingTests.class.getName());

  /**
   * How many independent connections the reader-containment test opens.
   *
   * <p>More than one because the reader is chosen at random from those available: a single connection landing in
   * the right region proves very little, where several in a row landing there is evidence the region filter is
   * being applied rather than being lucky.
   */
  private static final int READER_SAMPLES = 4;

  /** The table the write-forwarding test creates in the primary region and drops again. */
  private static final String GWF_TABLE = "gdb_write_forwarding_test";

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  protected static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  /** The region these tests treat as home: one that holds readers only. */
  protected static TestRegionalClusterInfo home() {
    return global().getFirstSecondaryRegion();
  }

  @TestTemplate
  public void test_setReadOnlyTrue_keepsTheConnectionInTheHomeRegion() throws SQLException {
    final TestRegionalClusterInfo home = home();

    for (int sample = 0; sample < READER_SAMPLES; sample++) {
      try (Connection conn = connect(getProps())) {
        conn.setReadOnly(true);

        final String readerId = auroraUtil.queryInstanceId(conn);
        LOGGER.finest("Reader " + (sample + 1) + " of " + READER_SAMPLES + ": " + readerId);

        assertTrue(home.getInstanceIdentifiers().contains(readerId),
            "gdbRwRestrictReaderToHomeRegion is on by default, so the reader must be in " + home.getRegion()
                + ", but the connection is on " + readerId + ", which is not one of "
                + home.getInstanceIdentifiers());
      }
    }
  }

  @TestTemplate
  public void test_setReadOnlyFalse_isRefusedWhenTheWriterIsInAnotherRegion() throws SQLException {
    // The default configuration, stated explicitly because it is the whole point of the test: restrict the writer
    // to the home region, and do not allow write forwarding. In a secondary region that combination has no legal
    // answer, and saying so is better than silently opening a cross-region write path an application did not ask
    // for and will not have budgeted latency for.
    final Properties props = getProps();
    GdbSettings.RESTRICT_WRITER_TO_HOME_REGION.set(props, "true");
    GdbSettings.ENABLE_GWF.set(props, "false");

    try (Connection conn = connect(props)) {
      conn.setReadOnly(true);
      final String readerId = auroraUtil.queryInstanceId(conn);

      final SQLException e = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      LOGGER.finest("setReadOnly(false) was refused with: " + e.getMessage());

      // And the refusal has to leave the connection alone. A plugin that failed the switch but abandoned the
      // reader would leave the application holding something unusable, which is worse than the switch failing.
      assertEquals(readerId, auroraUtil.queryInstanceId(conn),
          "after a refused switch the connection should still be on the reader it was using");
    }
  }

  @TestTemplate
  public void test_setReadOnlyFalse_staysLocalWhenWriteForwardingIsEnabled() throws SQLException {
    final TestRegionalClusterInfo home = home();

    // Global Write Forwarding is the other answer to the same situation: rather than refusing, keep the local
    // secondary-region connection and let Aurora carry the writes to the primary region. From the driver's side
    // the observable decision is that it does not reconnect anywhere.
    final Properties props = getProps();
    GdbSettings.RESTRICT_WRITER_TO_HOME_REGION.set(props, "true");
    GdbSettings.ENABLE_GWF.set(props, "true");

    try (Connection conn = connect(props)) {
      conn.setReadOnly(true);
      final String readerId = auroraUtil.queryInstanceId(conn);

      conn.setReadOnly(false);

      final String afterId = auroraUtil.queryInstanceId(conn);
      assertEquals(readerId, afterId,
          "with gdbEnableGlobalWriteForwarding the connection should stay where it is, not move to the writer");
      assertTrue(home.getInstanceIdentifiers().contains(afterId),
          "the connection should still be in " + home.getRegion() + ", but it is on " + afterId);

      // And then the write must actually land. Asserting only that the driver stayed put would pass equally well
      // against a configuration where forwarding is off, where the identical decision leaves the application
      // holding a read-only connection it believes is writable - the failure this whole branch exists to avoid.
      assertForwardedWriteSucceeds(conn);
    }
  }

  /**
   * Writes through a secondary-region connection and reads the row back.
   *
   * <p>Shaped by what write forwarding actually carries, which the first version of this method got wrong twice
   * and a real run caught both times. Forwarding carries <em>DML only</em> - DDL is explicitly unsupported - so
   * the table is created and dropped on the primary directly, and only the {@code INSERT} travels through the
   * forwarded connection. And the consistency parameter is per engine: PostgreSQL calls it
   * {@code apg_write_forward.consistency_mode} where MySQL calls it {@code aurora_replica_read_consistency};
   * sending MySQL's name to PostgreSQL fails with "unrecognized configuration parameter", which reads like
   * forwarding being broken rather than a wrong spelling.
   *
   * <p>The consistency level is {@code session}, not {@code eventual}, because the assertion is read-your-own-
   * write: session consistency makes the read wait for this session's forwarded writes to replicate back, which
   * is exactly the guarantee the assertion needs, while eventual explicitly permits the read to see stale data
   * and would make this test flaky by design.
   */
  private void assertForwardedWriteSucceeds(final Connection conn) throws SQLException {
    final TestRegionalClusterInfo primary = global().getRegion(global().getPrimaryRegion());

    // The plain target driver, not the wrapper: this connection exists only to run DDL somewhere writable, and
    // the wrapper refuses to connect to a global database without topology configuration this helper does not
    // need.
    final Properties ddlProps = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    final String ddlUrl = ConnectionStringHelper.getUrl(
        primary.getClusterEndpoint(),
        primary.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    try (Connection primaryConn = DriverManager.getConnection(ddlUrl, ddlProps)) {
      try (Statement ddl = primaryConn.createStatement()) {
        ddl.execute("DROP TABLE IF EXISTS " + GWF_TABLE);
        ddl.execute("CREATE TABLE " + GWF_TABLE + " (id INT NOT NULL PRIMARY KEY)");
      }

      try (Statement statement = conn.createStatement()) {
        statement.execute(consistencyModeSql());

        // The table has to replicate before the secondary can accept a statement naming it: the INSERT is
        // parsed against the secondary's own catalog before it is forwarded, and session consistency only
        // waits for this session's forwarded writes - the CREATE TABLE was neither. Without this wait the
        // INSERT fails with "relation does not exist" a few milliseconds after the table was created, which
        // reads like forwarding being broken rather than like replication lag.
        awaitTableVisible(statement);

        statement.executeUpdate("INSERT INTO " + GWF_TABLE + " (id) VALUES (1)");

        try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + GWF_TABLE)) {
          assertTrue(rs.next(), "the count query returned no row");
          assertEquals(1, rs.getInt(1),
              "the write was routed through Global Write Forwarding but the row is not there");
        }
      } finally {
        try (Statement ddl = primaryConn.createStatement()) {
          ddl.execute("DROP TABLE IF EXISTS " + GWF_TABLE);
        }
      }
    }
  }

  /**
   * Waits until this connection's region has replicated the test table.
   *
   * <p>Probed through the catalog rather than by retrying the INSERT, so a genuine forwarding failure still
   * surfaces as itself: an INSERT retried until it stops saying "relation does not exist" would also swallow a
   * forwarding path that is actually broken for that long.
   */
  private void awaitTableVisible(final Statement statement) throws SQLException {
    final boolean pg =
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG;
    final String probe = pg
        ? "SELECT to_regclass('" + GWF_TABLE + "')"
        : "SELECT COUNT(*) FROM information_schema.tables"
            + " WHERE table_schema = DATABASE() AND table_name = '" + GWF_TABLE + "'";

    final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
    while (System.nanoTime() < deadline) {
      try (ResultSet rs = statement.executeQuery(probe)) {
        if (rs.next() && (pg ? rs.getString(1) != null : rs.getInt(1) > 0)) {
          return;
        }
      }
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted while waiting for " + GWF_TABLE + " to replicate.", e);
      }
    }

    throw new SQLException("Table " + GWF_TABLE + " did not replicate to this region within 3 minutes, so "
        + "the forwarded write cannot be attempted. That is a replication problem, not a driver one.");
  }

  /** The session parameter that engages write forwarding, under whichever name this engine gives it. */
  private String consistencyModeSql() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? "SET apg_write_forward.consistency_mode = 'session'"
        : "SET aurora_replica_read_consistency = 'session'";
  }

  @TestTemplate
  public void test_homeRegionOutsideAccessibleRegions_isRejected() {
    final TestRegionalClusterInfo home = home();
    final String elsewhere = global().getPrimaryRegion();

    // A contradiction the plugin should catch at initialisation rather than at the first switch: the home region
    // is not among the regions it is allowed to reach. Configuration mistakes of this shape are easy to make when
    // gdbAccessibleRegions is maintained separately from the deployment, and finding out on connect is much
    // cheaper than finding out during a failover.
    final Properties props = getProps();
    GdbSettings.RW_HOME_REGION.set(props, home.getRegion());
    props.setProperty(PropertyDefinition.GDB_ACCESSIBLE_REGIONS.name, elsewhere);

    final SQLException e = assertThrows(SQLException.class, () -> {
      try (Connection conn = connect(props)) {
        conn.setReadOnly(true);
        auroraUtil.queryInstanceId(conn);
      }
    });

    LOGGER.finest("Rejected as expected: " + e.getMessage());
  }

  /**
   * Connects to the home region, which for these tests means a reader endpoint in a secondary region.
   *
   * <p>The reader cluster endpoint rather than an instance endpoint: it is what an application in a secondary
   * region would actually put in its configuration, and these tests care which region they land in, not which
   * host.
   */
  protected Connection connect(final Properties props) throws SQLException {
    final TestRegionalClusterInfo home = home();
    final String url = ConnectionStringHelper.getWrapperUrl(
        home.getClusterReadOnlyEndpoint(),
        home.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    LOGGER.finest("Connecting to " + url);
    return DriverManager.getConnection(url, props);
  }

  /**
   * The properties common to every GDB splitting variant.
   *
   * <p>Subclasses add the plugin and, for the endpoint-based variants, the endpoints. The home region is pinned
   * here so that all four variants are answering the same question.
   */
  protected Properties getDefaultProps() {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(
        PropertyDefinition.SOCKET_TIMEOUT.name, String.valueOf(TimeUnit.SECONDS.toMillis(10)));
    props.setProperty(
        PropertyDefinition.CONNECT_TIMEOUT.name, String.valueOf(TimeUnit.SECONDS.toMillis(20)));

    DialectManager.DIALECT.set(props, dialect());
    GdbSettings.RW_HOME_REGION.set(props, home().getRegion());
    return props;
  }

  /**
   * The topology-based variant: hosts come from the global database's own topology.
   *
   * <p>Which is why {@code globalClusterInstanceHostPatterns} is set - the topology query returns instance
   * identifiers, and without one template per region the driver cannot turn them into addresses.
   */
  protected Properties getProps() {
    final Properties props = getDefaultProps();
    PropertyDefinition.PLUGINS.set(props, "gdbReadWriteSplitting");
    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());
    return props;
  }

  protected static String dialect() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? DialectCodes.GLOBAL_AURORA_PG
        : DialectCodes.GLOBAL_AURORA_MYSQL;
  }
}
