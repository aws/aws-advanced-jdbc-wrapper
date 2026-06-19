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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
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
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin2;
import software.amazon.jdbc.util.StringUtils;

/**
 * Integration tests for the {@code awsSecretsManager2} (Stale-While-Revalidate) plugin.
 *
 * <p>A secret holding the test database credentials is provisioned in AWS Secrets Manager before
 * the tests run and deleted afterwards. The tests exercise the real end-to-end flow: fetching
 * credentials from Secrets Manager and using them to open a database connection.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.SECRETS_MANAGER)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(3)
public class AwsSecretsManager2IntegrationTest {

  private static final Logger LOGGER =
      Logger.getLogger(AwsSecretsManager2IntegrationTest.class.getName());

  private static SecretsManagerClient secretsManagerClient;
  private static String secretId;
  private static String secretArn;
  private static String region;

  @BeforeAll
  public static void setUpSecret() {
    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    region = info.getRegion();
    secretsManagerClient = SecretsManagerClient.builder()
        .region(Region.of(region))
        .credentialsProvider(getCredentialsProvider(info))
        .build();

    // Create a secret holding the test database credentials.
    secretId = "aws-jdbc-wrapper-it-sm2-" + UUID.randomUUID();
    final String secretString = String.format(
        "{\"username\":\"%s\",\"password\":\"%s\"}",
        info.getDatabaseInfo().getUsername(),
        info.getDatabaseInfo().getPassword());

    secretArn = secretsManagerClient.createSecret(CreateSecretRequest.builder()
        .name(secretId)
        .secretString(secretString)
        .build()).arn();
    LOGGER.finest("Created test secret: " + secretId);
  }

  @AfterAll
  public static void tearDownSecret() {
    if (secretsManagerClient != null && secretId != null) {
      try {
        secretsManagerClient.deleteSecret(DeleteSecretRequest.builder()
            .secretId(secretId)
            .forceDeleteWithoutRecovery(true)
            .build());
        LOGGER.finest("Deleted test secret: " + secretId);
      } catch (final Exception e) {
        LOGGER.warning("Failed to delete test secret " + secretId + ": " + e.getMessage());
      } finally {
        secretsManagerClient.close();
      }
    }
  }

  @BeforeEach
  public void beforeEach() {
    // Each test starts from a clean cache so cache-miss/cache-hit behavior is deterministic.
    AwsSecretsManagerConnectionPlugin2.clearCache();
  }

  /** First connection (cache miss) fetches credentials synchronously and connects successfully. */
  @TestTemplate
  public void test_awsSecretsManager2_connectsWithFetchedCredentials() throws SQLException {
    final Properties props = initProps(secretId);

    try (final Connection conn =
             DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
         final Statement stmt = conn.createStatement()) {
      assertTrue(conn.isValid(10));
      assertTrue(stmt.execute("SELECT 1"));
    }
  }

  /** A second connection is served from the shared cache and still connects successfully. */
  @TestTemplate
  public void test_awsSecretsManager2_secondConnectionUsesCache() throws SQLException {
    final Properties props = initProps(secretId);
    final String url = ConnectionStringHelper.getWrapperUrl();

    try (final Connection conn = DriverManager.getConnection(url, props)) {
      assertTrue(conn.isValid(10));
    }

    // Second connection should reuse cached credentials and succeed.
    try (final Connection conn = DriverManager.getConnection(url, props)) {
      assertNotNull(conn);
      assertTrue(conn.isValid(10));
    }
  }

  /** The secret ARN can be used directly; the region is parsed from the ARN. */
  @TestTemplate
  public void test_awsSecretsManager2_connectsUsingSecretArn() throws SQLException {
    assertNotNull(secretArn);

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "awsSecretsManager2");
    props.setProperty(AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.name, secretArn);
    props.remove(PropertyDefinition.USER.name);
    props.remove(PropertyDefinition.PASSWORD.name);

    try (final Connection conn =
             DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
      assertTrue(conn.isValid(10));
    }
  }

  /** A non-existent secret cannot be fetched and the connection attempt fails. */
  @TestTemplate
  public void test_awsSecretsManager2_unknownSecretFails() {
    final Properties props = initProps("aws-jdbc-wrapper-it-sm2-missing-" + UUID.randomUUID());

    assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));
  }

  /** A datasource-style connection preserves the fetched credentials. */
  @TestTemplate
  public void test_awsSecretsManager2_validConnection() {
    final Properties props = initProps(secretId);
    assertDoesNotThrow(() -> {
      try (final Connection conn =
               DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props)) {
        assertTrue(conn.isValid(10));
      }
    });
  }

  private Properties initProps(final String secret) {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "awsSecretsManager2");
    props.setProperty(AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.name, secret);
    props.setProperty(AwsSecretsManagerConnectionPlugin.REGION_PROPERTY.name, region);
    // Credentials must come from the secret, not from the default properties.
    props.remove(PropertyDefinition.USER.name);
    props.remove(PropertyDefinition.PASSWORD.name);
    return props;
  }

  private static AwsCredentialsProvider getCredentialsProvider(final TestEnvironmentInfo info) {
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
