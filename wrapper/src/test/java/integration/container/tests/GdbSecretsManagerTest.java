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
import integration.TestEnvironmentInfo;
import integration.TestGlobalDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTags;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.DialectCodes;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin2;
import software.amazon.jdbc.util.StringUtils;

/**
 * Fetching database credentials from Secrets Manager while connected to a secondary region.
 *
 * <p>The interesting property is that a secret does not need to live where the database does. A global database's
 * regional clusters all share the master credentials the primary was created with - a secondary inherits them and
 * cannot be given its own - so one secret is correct for every region, and the region in the connection properties
 * describes where the <em>secret</em> is, not where the database is. That is easy to misread, and reading it as
 * "the database's region" produces a secret-not-found error in a region that has a perfectly good database in it.
 *
 * <p>So these tests deliberately cross: the secret is created in the primary region and used to authenticate
 * against instances in a secondary one. A test that kept both in one region would pass without ever exercising the
 * distinction.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.AURORA_GLOBAL)
@EnableOnTestFeature({
    TestEnvironmentFeatures.SECRETS_MANAGER,
    // As in the non-global test, IAM stands in for "this environment has real AWS credentials", which is what
    // creating the secret needs.
    TestEnvironmentFeatures.IAM,
    TestEnvironmentFeatures.GLOBAL_DATABASE})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Tag(TestTags.GDB)
@Order(60)
public class GdbSecretsManagerTest {

  private static final Logger LOGGER = Logger.getLogger(GdbSecretsManagerTest.class.getName());

  private static SecretsManagerClient secretsManagerClient;
  private static String secretId;
  private static String secretArn;

  /** The region the secret is created in: the primary, which is the region the environment is built around. */
  private static String secretRegion;

  @BeforeAll
  public static void setUpSecret() {
    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    secretRegion = StringUtils.isNullOrEmpty(info.getRegion()) ? "us-east-2" : info.getRegion();

    secretsManagerClient = SecretsManagerClient.builder()
        .region(Region.of(secretRegion))
        .credentialsProvider(credentialsProvider(info))
        .build();

    // The master credentials, which every region of the global database shares. This is the fact the class is
    // about, expressed as the fixture: one secret, and it is not created per region.
    secretId = "aws-jdbc-wrapper-it-gdb-sm-" + UUID.randomUUID();
    final String secretString = String.format(
        "{\"username\":\"%s\",\"password\":\"%s\"}",
        info.getDatabaseInfo().getUsername(),
        info.getDatabaseInfo().getPassword());

    secretArn = secretsManagerClient.createSecret(CreateSecretRequest.builder()
        .name(secretId)
        .secretString(secretString)
        .build()).arn();

    LOGGER.finest("Created test secret " + secretId + " in " + secretRegion);
  }

  @AfterAll
  public static void tearDownSecret() {
    if (secretsManagerClient == null || secretId == null) {
      return;
    }
    try {
      // Force-deleted rather than scheduled: a recovery window would leave the name taken, and these are created
      // per run with a fresh name precisely so nothing accumulates in the account.
      secretsManagerClient.deleteSecret(DeleteSecretRequest.builder()
          .secretId(secretId)
          .forceDeleteWithoutRecovery(true)
          .build());
      LOGGER.finest("Deleted test secret " + secretId);
    } catch (final Exception e) {
      LOGGER.warning("Failed to delete test secret " + secretId + ": " + e.getMessage());
    } finally {
      secretsManagerClient.close();
    }
  }

  @BeforeEach
  public void beforeEach() {
    AwsSecretsManagerConnectionPlugin2.clearCache();
  }

  private static TestGlobalDatabaseInfo global() {
    final TestGlobalDatabaseInfo info = TestEnvironment.getCurrent().getInfo().getGlobalDatabaseInfo();
    assertNotNull(info, "this environment published no global database topology");
    return info;
  }

  @TestTemplate
  public void test_secretsManager_authenticatesInASecondaryRegionFromAnotherRegionsSecret()
      throws SQLException {

    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();
    final Properties props = secretProps(secretId, secretRegion);

    try (Connection conn = connectToFirstInstance(secondary, props)) {
      assertTrue(conn.isValid(10),
          "a secret in " + secretRegion + " holds the credentials for every region of the global database, "
              + "including " + secondary.getRegion());
    }
  }

  @TestTemplate
  public void test_secretsManager_theArnCarriesTheRegion() throws SQLException {
    assertNotNull(secretArn);
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();

    // No region property at all: an ARN already names one, and the plugin reads it from there. Worth its own test
    // on a global database, because this is the form that cannot be got wrong - there is no second place for the
    // region to come from and disagree.
    final Properties props = secretProps(secretArn, null);

    try (Connection conn = connectToFirstInstance(secondary, props)) {
      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void test_secretsManager_lookingForTheSecretInTheDatabasesRegionFails() {
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();

    // The misreading spelled out: name the region the database is in rather than the region the secret is in. It
    // looks more correct than the working configuration, which is exactly why it is worth pinning as a failure.
    final Properties props = secretProps(secretId, secondary.getRegion());

    final SQLException e = assertThrows(SQLException.class, () -> {
      try (Connection conn = connectToFirstInstance(secondary, props)) {
        conn.isValid(10);
      }
    });

    LOGGER.finest("As expected, the secret was not found in " + secondary.getRegion() + ": " + e.getMessage());
  }

  @TestTemplate
  public void test_secretsManager_unknownSecretFails() {
    final TestRegionalClusterInfo secondary = global().getFirstSecondaryRegion();
    final Properties props =
        secretProps("aws-jdbc-wrapper-it-gdb-sm-missing-" + UUID.randomUUID(), secretRegion);

    assertThrows(SQLException.class, () -> {
      try (Connection conn = connectToFirstInstance(secondary, props)) {
        conn.isValid(10);
      }
    });
  }

  /**
   * Builds properties that can only connect if the secret is fetched.
   *
   * <p>The user and password are removed rather than left in place. With them present every test here would pass
   * whether the secret was read or not, which would make the whole class a test of nothing.
   *
   * @param secret the secret id or ARN
   * @param region the region to look in, or null to let an ARN speak for itself
   */
  private Properties secretProps(final String secret, final String region) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.PLUGINS.set(props, "awsSecretsManager2");
    props.setProperty(AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.name, secret);
    if (region != null) {
      props.setProperty(AwsSecretsManagerConnectionPlugin.REGION_PROPERTY.name, region);
    }
    props.remove(PropertyDefinition.USER.name);
    props.remove(PropertyDefinition.PASSWORD.name);

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

  private static AwsCredentialsProvider credentialsProvider(final TestEnvironmentInfo info) {
    if (StringUtils.isNullOrEmpty(info.getAwsAccessKeyId())) {
      return DefaultCredentialsProvider.create();
    }
    return StaticCredentialsProvider.create(
        StringUtils.isNullOrEmpty(info.getAwsSessionToken())
            ? AwsBasicCredentials.create(info.getAwsAccessKeyId(), info.getAwsSecretAccessKey())
            : AwsSessionCredentials.create(
                info.getAwsAccessKeyId(), info.getAwsSecretAccessKey(), info.getAwsSessionToken()));
  }
}
