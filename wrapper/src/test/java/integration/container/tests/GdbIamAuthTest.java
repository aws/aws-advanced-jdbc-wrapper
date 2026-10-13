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
import integration.TestEnvironmentFeatures;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTags;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
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
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;

/**
 * IAM authentication against a secondary region of a global database.
 *
 * <p>IAM is the one authentication mechanism a global database can break without anything looking broken. A token
 * is signed for a specific region, and every region of a global database accepts connections, so pointing a
 * correctly generated token at the wrong region produces a plain "access denied" - indistinguishable from a bad
 * password, and traceable to nothing in the connection string.
 *
 * <p>The database side needs nothing special: the IAM-mapped user is created on the primary region's writer and
 * arrives in the secondaries by replication, which is the only way it could work, since a secondary is read-only
 * and could not be granted anything directly. So what is left to test is entirely about the region the token is
 * signed for.
 *
 * <p>Instance endpoints rather than cluster endpoints throughout, because this test needs to be certain which
 * region it is talking to. A cluster endpoint would be equally valid for an application and would test the same
 * thing, but a failure would leave "which host did it actually reach" as an open question.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA_GLOBAL)
@EnableOnTestFeature({TestEnvironmentFeatures.IAM, TestEnvironmentFeatures.GLOBAL_DATABASE})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Tag(TestTags.GDB)
@Order(50)
// The MariaDB driver cannot be made to use mysql_clear_password, so it hashes the IAM token and every IAM test
// fails with "access denied". Disabled for the same reason as in AwsIamIntegrationTest.
public class GdbIamAuthTest {

  private static final Logger LOGGER = Logger.getLogger(GdbIamAuthTest.class.getName());

  @BeforeEach
  public void beforeEach() {
    // Tokens are cached per host and region. Without clearing, a test that expects a rejection could be served a
    // token minted by an earlier test for a different region and pass or fail for the wrong reason.
    IamAuthConnectionPlugin.clearCache();
  }

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iam_authenticatesInASecondaryRegion() throws SQLException {
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();

    final Properties props = iamProps(secondary.getRegion());

    try (Connection conn = connectToFirstInstance(secondary, props)) {
      assertTrue(conn.isValid(10),
          "an IAM token signed for " + secondary.getRegion() + " should authenticate there");
    }
  }

  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iam_aTokenSignedForAnotherRegionIsRejected() {
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();
    final String wrongRegion = global().getPrimaryRegion();

    // The mistake this class exists for: the region left at whatever the environment's default is, while the host
    // is in a different one. It is easy to write - the region is usually a single global setting - and on a
    // single-region deployment it is not even wrong.
    final Properties props = iamProps(wrongRegion);

    final SQLException e = assertThrows(SQLException.class, () -> {
      try (Connection conn = connectToFirstInstance(secondary, props)) {
        conn.isValid(10);
      }
    });

    LOGGER.finest("A token signed for " + wrongRegion + " was rejected by " + secondary.getRegion()
        + ", as it should be: " + e.getMessage());
  }

  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iam_authenticatesInThePrimaryRegion() throws SQLException {
    final TestRegionalClusterInfo primary = global().getRegion(global().getPrimaryRegion());

    // The control. Without it, a failure in the secondary-region test could equally mean the IAM user was never
    // created or the account's IAM policy does not permit connections at all.
    final Properties props = iamProps(primary.getRegion());

    try (Connection conn = connectToFirstInstance(primary, props)) {
      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iam_wrongUserIsRejectedInASecondaryRegion() {
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();

    final Properties props = iamProps(secondary.getRegion());
    props.setProperty(
        PropertyDefinition.USER.name,
        "WRONG_" + TestEnvironment.getCurrent().getInfo().getIamUsername() + "_USER");

    assertThrows(SQLException.class, () -> {
      try (Connection conn = connectToFirstInstance(secondary, props)) {
        conn.isValid(10);
      }
    });
  }

  /**
   * Builds IAM properties for one region.
   *
   * <p>No password, deliberately: it is the only way to be sure the token is what authenticated. With a valid
   * password present, every test here would pass whether the token was accepted or silently ignored.
   */
  private Properties iamProps(final String region) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.PLUGINS.set(props, "iam");
    props.setProperty(IamAuthConnectionPlugin.IAM_REGION.name, region);
    props.setProperty(
        PropertyDefinition.USER.name, TestEnvironment.getCurrent().getInfo().getIamUsername());
    props.setProperty(PropertyDefinition.PASSWORD.name, "");
    props.setProperty(PropertyDefinition.TCP_KEEP_ALIVE.name, "false");

    // Any wrapper connection to a global database needs these two, whatever the connection is for: the driver
    // identifies the deployment as global and then refuses to proceed without one host template per region.
    DialectManager.DIALECT.set(props, dialect());
    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());
    return props;
  }

  private Connection connectToFirstInstance(
      final TestRegionalClusterInfo cluster, final Properties props) throws SQLException {

    final String host = cluster.getInstanceEndpoint(cluster.getInstanceIdentifiers().get(0));
    final String url = ConnectionStringHelper.getWrapperUrl(
        host,
        cluster.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

    LOGGER.finest("Connecting to " + url);
    return DriverManager.getConnection(url, props);
  }

  private static String dialect() {
    return TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine() == DatabaseEngine.PG
        ? DialectCodes.GLOBAL_AURORA_PG
        : DialectCodes.GLOBAL_AURORA_MYSQL;
  }
}
