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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPlugin;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Connecting through a global database's own endpoint, with the {@code initialConnection} plugin.
 *
 * <p>An Aurora global database publishes one address that is not a cluster endpoint:
 * {@code <name>.global-<account>.global.rds.amazonaws.com}. It differs from every regional endpoint in two ways
 * that matter to a driver. It follows the writer, so it keeps pointing at the right place across a switchover
 * without the connection string changing. And it carries no region in its DNS name, so anything a driver normally
 * derives from the hostname's region has to come from somewhere else.
 *
 * <p>The driver knows this and treats it as its own URL type, with its own branches: the
 * {@code initialConnection} plugin always substitutes the writer and always verifies the connection is a writer,
 * where for a regional writer endpoint it has to compare regions first to work out whether the endpoint is the
 * active one. Those branches had no test. Neither did the classification they depend on - the DNS pattern was
 * only ever matched against a hand-written string in a unit test, never against an endpoint AWS actually issued.
 *
 * <p>So this class connects through the real thing. Two of the tests need no AWS operation at all and are here
 * because they pin down the cheap failures that make the expensive ones unreadable: that the endpoint AWS gave us
 * is the shape the driver recognises, and that configurations contradicting a writer-only endpoint are refused.
 *
 * <p>Skipped rather than failed when the environment publishes no global endpoint. Older engine versions do not
 * have one, and that is a property of the deployment rather than a defect in the driver.
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
@Order(20)
public class GdbGlobalEndpointTest {

  private static final Logger LOGGER = Logger.getLogger(GdbGlobalEndpointTest.class.getName());

  private static final String TABLE = "gdb_global_endpoint_test";

  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  /**
   * Returns the global endpoint, skipping the test when there is none.
   *
   * <p>An assumption rather than an assertion. A global cluster on an older engine version publishes no global
   * endpoint, and reporting that as a failure would blame the driver for the environment's engine choice.
   */
  private static String globalEndpoint() {
    final String endpoint = global().getGlobalEndpoint();
    assumeTrue(endpoint != null && !endpoint.trim().isEmpty(),
        "this global database published no global endpoint, so there is nothing to connect through");
    return endpoint;
  }

  @TestTemplate
  public void test_theProvisionedEndpointIsRecognisedAsAGlobalWriterEndpoint() {
    final String endpoint = globalEndpoint();

    // Cheap, and the first thing worth knowing. Every other test in this class depends on the driver classifying
    // this hostname as a global endpoint - if it does not, they all still pass by exercising the ordinary cluster
    // path, and nothing says so. The existing unit coverage matches the pattern against a hand-written string,
    // which cannot catch AWS issuing a shape the pattern does not expect.
    final RdsUrlType type = new RdsUtils().identifyRdsType(endpoint);

    LOGGER.info("Global endpoint " + endpoint + " classified as " + type);
    assertEquals(RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER, type,
        "the endpoint AWS published for this global database is not recognised as a global writer endpoint, so "
            + "every global-specific branch in the driver would be skipped");

    // No region in the name is the property the driver's region-derived logic has to cope with, so it is worth
    // stating rather than leaving implied by the type.
    assertTrue(!type.hasRegion(), "a global endpoint should carry no region");
  }

  @TestTemplate
  public void test_initialConnectionResolvesTheGlobalEndpointToTheWriter(final TestDriver testDriver)
      throws SQLException {

    final TestGlobalDatabaseInfo info = global();
    final TestRegionalClusterInfo primary = info.getRegion(info.getPrimaryRegion());

    try (Connection conn = connect(testDriver, props(testDriver))) {
      final String instanceId = auroraUtil.queryInstanceId(conn);
      LOGGER.info("The global endpoint resolved to " + instanceId);

      // For a global endpoint the plugin substitutes the writer unconditionally - there is no region comparison
      // to make, because the endpoint names no region. So the connection must land on an instance, not stay on
      // the endpoint, and that instance must be in whichever region currently holds the writer.
      assertTrue(primary.getInstanceIdentifiers().contains(instanceId),
          "the global endpoint should resolve to an instance in the writer's region " + primary.getRegion()
              + ", but it resolved to " + instanceId + ", which is not one of "
              + primary.getInstanceIdentifiers());

      // And it must really be the writer, which is what the plugin verifies rather than assumes. A reader would
      // satisfy every assertion above and fail the first write.
      assertWritable(conn);
    }
  }

  @TestTemplate
  public void test_askingForAReaderThroughTheGlobalEndpointIsRefused(final TestDriver testDriver) {
    globalEndpoint();

    // A global endpoint is a writer endpoint by definition, so asking the plugin to substitute a reader is a
    // contradiction. Refusing it at connect time is worth pinning: the alternative is a connection that appears
    // to honour the setting and hands back a writer anyway.
    final Properties props = props(testDriver);
    props.setProperty(AuroraInitialConnectionStrategyPlugin.ENDPOINT_SUBSTITUTION_ROLE.name, "reader");

    final SQLException e = assertThrows(SQLException.class, () -> {
      try (Connection conn = connect(testDriver, props)) {
        conn.isValid(10);
      }
    });

    LOGGER.finest("Refused as expected: " + e.getMessage());
  }

  @TestTemplate
  public void test_verifyingAReaderThroughTheGlobalEndpointIsRefused(final TestDriver testDriver) {
    globalEndpoint();

    // The same contradiction from the verification side, which is a separate code path with its own validation.
    final Properties props = props(testDriver);
    props.setProperty(AuroraInitialConnectionStrategyPlugin.VERIFY_OPENED_CONNECTION_ROLE.name, "reader");

    assertThrows(SQLException.class, () -> {
      try (Connection conn = connect(testDriver, props)) {
        conn.isValid(10);
      }
    });
  }

  @TestTemplate
  public void test_substitutionCanBeTurnedOffAndTheEndpointUsedDirectly(final TestDriver testDriver)
      throws SQLException {

    // The opposite setting, and a legitimate one: an application that wants the endpoint's own DNS resolution
    // rather than a resolved instance. It should connect and be usable - the global endpoint resolves to the
    // writer by itself, which is the point of it - so this also shows the substitution is an optimisation of
    // something that already works rather than a requirement.
    final Properties props = props(testDriver);
    props.setProperty(AuroraInitialConnectionStrategyPlugin.ENDPOINT_SUBSTITUTION_ROLE.name, "none");
    props.setProperty(AuroraInitialConnectionStrategyPlugin.VERIFY_OPENED_CONNECTION_ROLE.name, "none");

    try (Connection conn = connect(testDriver, props)) {
      assertTrue(conn.isValid(10));
      assertWritable(conn);
    }
  }

  private Connection connect(final TestDriver testDriver, final Properties props) throws SQLException {
    final String url = ConnectionStringHelper.getWrapperUrl(
        globalEndpoint(),
        global().getRegion(global().getPrimaryRegion()).getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    LOGGER.finest("Connecting to " + url);
    return DriverManager.getConnection(url, props);
  }

  /**
   * The properties for a global-endpoint connection.
   *
   * <p>No dialect is set, unlike every other GDB test here, and that is deliberate: a global endpoint is enough
   * for the driver to select the global Aurora dialect by itself. Naming it would mask a regression in that
   * detection, and the detection is what makes this endpoint usable without special configuration.
   *
   * <p>{@code globalClusterInstanceHostPatterns} is still required. It belongs to the host list provider the
   * dialect installs, not to the endpoint, so choosing the dialect automatically does not remove the need for one
   * host template per region.
   */
  private Properties props(final TestDriver testDriver) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.PLUGINS.set(props, "initialConnection");

    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());

    DriverHelper.setConnectTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(testDriver, props, 20, TimeUnit.SECONDS);
    return props;
  }

  private void assertWritable(final Connection conn) throws SQLException {
    try (Statement statement = conn.createStatement()) {
      statement.execute("DROP TABLE IF EXISTS " + TABLE);
      statement.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
      statement.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");

      try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
        assertTrue(rs.next(), "the count query returned no row");
        assertEquals(1, rs.getInt(1), "the connection could not write through the global endpoint");
      }

      statement.execute("DROP TABLE IF EXISTS " + TABLE);
    }
  }
}
