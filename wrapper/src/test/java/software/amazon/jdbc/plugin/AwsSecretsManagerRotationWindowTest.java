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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.REGION_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_CONNECT_RETRY_INTERVAL_MS_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_CONNECT_RETRY_TIMEOUT_MS_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.exceptions.PgExceptionHandler;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.Secret;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

/**
 * End-to-end simulation of the AWS Secrets Manager rotation window reported in
 * <a href="https://github.com/aws/aws-advanced-go-wrapper/issues/470">go-wrapper issue 470</a>.
 *
 * <p>Rather than stubbing "throw a login error N times", these tests model the two independent
 * pieces of state a rotation moves through and let the real plugin logic drive them:
 *
 * <ul>
 *   <li>the password the <b>database</b> accepts, which changes at {@code setSecret};</li>
 *   <li>the password {@code GetSecretValue} returns for {@code AWSCURRENT}, which only changes at
 *       {@code finishSecret}.</li>
 * </ul>
 *
 * <p>The gap between those two events is the window in which no obtainable credential can log in.
 */
@SuppressWarnings({"resource", "unchecked"})
public class AwsSecretsManagerRotationWindowTest {

  private static final String TEST_PG_PROTOCOL = "jdbc:aws-wrapper:postgresql:";
  private static final String TEST_REGION = "us-east-2";
  private static final String TEST_SECRET_ID = "secretId";
  private static final String TEST_USERNAME = "rotatingUser";
  private static final String PASSWORD_BEFORE_ROTATION = "passwordBeforeRotation";
  private static final String PASSWORD_AFTER_ROTATION = "passwordAfterRotation";

  /** PostgreSQL {@code invalid_password}, what the server returns while the window is open. */
  private static final String PG_INVALID_PASSWORD_STATE = "28P01";

  private static final String LOGIN_FAILED_MESSAGE = "password authentication failed for user";

  /** AWSCURRENT is never promoted for the duration of the test. */
  private static final int NEVER_PROMOTED = Integer.MAX_VALUE;

  private static final Pair<String, String> SECRET_CACHE_KEY = Pair.create(TEST_SECRET_ID, TEST_REGION);
  private static final HostSpec TEST_HOSTSPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("test-domain").port(5432).build();

  private AutoCloseable closeable;
  private Properties props;
  private RotationSimulator rotation;

  @Mock FullServicesContainer mockServicesContainer;
  @Mock SecretsManagerClient mockSecretsManagerClient;
  @Mock GetSecretValueRequest mockGetValueRequest;
  @Mock StorageService mockStorageService;
  @Mock ConnectionPluginManager mockConnectionPluginManager;
  @Mock PluginServiceImpl mockService;
  @Mock Dialect mockDialect;
  @Mock DialectManager mockDialectManager;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  @Mock SessionStateService mockSessionStateService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;
  @Mock Connection mockConnection;

  private final ConfigurationProfile configurationProfile =
      ConfigurationProfileBuilder.get().withName("test").build();

  @BeforeEach
  void init() throws SQLException {
    this.closeable = MockitoAnnotations.openMocks(this);

    this.props = new Properties();
    REGION_PROPERTY.set(this.props, TEST_REGION);
    SECRET_ID_PROPERTY.set(this.props, TEST_SECRET_ID);

    when(mockServicesContainer.getConnectionPluginManager()).thenReturn(mockConnectionPluginManager);
    when(mockServicesContainer.getPluginService()).thenReturn(mockService);
    when(mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    when(mockDialectManager.getDialect(anyString(), anyString(), any(Properties.class))).thenReturn(mockDialect);
    // A real exception handler is required so the plugin can recognize the server's login failure.
    when(mockDialect.getExceptionHandler()).thenReturn(new PgExceptionHandler());
  }

  @AfterEach
  void cleanUp() throws Exception {
    this.closeable.close();
    CoreServicesContainer.getInstance().getStorageService().clear(Secret.class);
  }

  /**
   * The reported failure. The application holds valid cached credentials, {@code setSecret} has
   * already changed the database password, and {@code AWSCURRENT} has not been promoted yet.
   *
   * <p>The default configuration gets exactly two connection attempts out of one forced re-fetch,
   * both of which present the pre-rotation password, and then gives up. Every new physical
   * connection fails this way until the rotation finishes.
   */
  @Test
  void testRotationWindowBreaksNewConnectionsByDefault() throws SQLException {
    givenRotation(NEVER_PROMOTED);
    givenCachedSecret(unexpiredSecret(PASSWORD_BEFORE_ROTATION));
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin();

    final SQLException exception = assertThrows(
        SQLException.class,
        () -> plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc()));

    assertEquals(PG_INVALID_PASSWORD_STATE, exception.getSQLState());
    assertEquals(2, this.rotation.connectAttempts(), "the cached attempt plus a single retry");
    assertEquals(1, this.rotation.secretFetches(), "exactly one forced re-fetch");
  }

  /**
   * The same window, bridged. {@code AWSCURRENT} is promoted while the plugin is still polling, so
   * a later attempt picks up the post-rotation password and the connection succeeds.
   */
  @Test
  void testRotationWindowBridgedByRetryBudget() throws SQLException {
    givenRotation(2);
    givenCachedSecret(unexpiredSecret(PASSWORD_BEFORE_ROTATION));
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin(5000, 1);

    final Connection connection =
        plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc());

    assertSame(this.mockConnection, connection);
    assertEquals(PASSWORD_AFTER_ROTATION, PropertyDefinition.PASSWORD.getString(this.props));
    assertEquals(TEST_USERNAME, PropertyDefinition.USER.getString(this.props));
    // Cached attempt, then one attempt per re-fetch: the 2nd and 3rd fetches still resolve to the
    // pre-rotation password, the 4th attempt gets the promoted one. Unreachable with a single retry.
    assertEquals(4, this.rotation.connectAttempts());
    assertEquals(3, this.rotation.secretFetches());
  }

  /**
   * The cold-cache case: a JVM or Lambda instance starting up inside the window. The first attempt
   * already fetches from the service, which is precisely the situation the plugin's original
   * {@code secretWasFetched} guard treated as "nothing left to try".
   */
  @Test
  void testRotationWindowBridgedOnFirstEverConnection() throws SQLException {
    givenRotation(2);
    givenCachedSecret(null);
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin(5000, 1);

    final Connection connection =
        plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc());

    assertSame(this.mockConnection, connection);
    assertEquals(PASSWORD_AFTER_ROTATION, PropertyDefinition.PASSWORD.getString(this.props));
    // Every attempt fetches here, since there was nothing cached to start from.
    assertEquals(3, this.rotation.connectAttempts());
    assertEquals(3, this.rotation.secretFetches());
  }

  /**
   * Same cold start, default configuration: a single attempt, no retry at all.
   */
  @Test
  void testFirstEverConnectionInWindowGetsNoRetryByDefault() throws SQLException {
    givenRotation(NEVER_PROMOTED);
    givenCachedSecret(null);
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin();

    final SQLException exception = assertThrows(
        SQLException.class,
        () -> plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc()));

    assertEquals(PG_INVALID_PASSWORD_STATE, exception.getSQLState());
    assertEquals(1, this.rotation.connectAttempts());
  }

  /**
   * A budget shorter than the window still fails, and reports the login error rather than a timeout
   * of its own. Retrying is a mitigation with a bound, not a guarantee.
   */
  @Test
  void testRotationWindowOutlastsRetryBudget() throws SQLException {
    givenRotation(NEVER_PROMOTED);
    givenCachedSecret(unexpiredSecret(PASSWORD_BEFORE_ROTATION));
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin(150, 20);

    final SQLException exception = assertThrows(
        SQLException.class,
        () -> plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc()));

    assertEquals(PG_INVALID_PASSWORD_STATE, exception.getSQLState());
    assertEquals(LOGIN_FAILED_MESSAGE, exception.getMessage());
    assertTrue(this.rotation.connectAttempts() >= 2,
        "should have retried within the budget, attempts: " + this.rotation.connectAttempts());
  }

  /**
   * Once the rotation has completed, the default single retry is sufficient on its own: the stale
   * cached password fails, the forced re-fetch returns the promoted secret, and the retry succeeds.
   * This is the boundary of the reported problem and must keep working unchanged.
   */
  @Test
  void testStaleCachedPasswordRecoversAfterRotationCompletes() throws SQLException {
    givenRotation(0);
    givenCachedSecret(unexpiredSecret(PASSWORD_BEFORE_ROTATION));
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin();

    final Connection connection =
        plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc());

    assertSame(this.mockConnection, connection);
    assertEquals(2, this.rotation.connectAttempts());
    assertEquals(PASSWORD_AFTER_ROTATION, PropertyDefinition.PASSWORD.getString(this.props));
  }

  /**
   * The re-fetched secret is written back to the shared cache, so a connection opened after the
   * window closes does not have to rediscover the new password.
   */
  @Test
  void testPostRotationSecretIsCached() throws SQLException {
    givenRotation(2);
    final AtomicReference<Secret> cache = givenCachedSecret(unexpiredSecret(PASSWORD_BEFORE_ROTATION));
    final AwsSecretsManagerConnectionPlugin plugin = createPlugin(5000, 1);

    plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, this.props, true, this.rotation.connectFunc());

    final Secret cached = cache.get();
    assertNotNull(cached);
    assertEquals(PASSWORD_AFTER_ROTATION, cached.getPassword());
  }

  private void givenRotation(final int promoteAwsCurrentAfterFetches) {
    this.rotation = new RotationSimulator(
        PASSWORD_BEFORE_ROTATION, PASSWORD_AFTER_ROTATION, promoteAwsCurrentAfterFetches,
        this.props, this.mockConnection);

    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenAnswer(invocation -> GetSecretValueResponse.builder()
            .secretString(this.rotation.getSecretValue())
            .build());
  }

  /**
   * Backs the mocked storage service with a real reference so that writes performed by the plugin
   * are visible to subsequent reads, as they would be in the shared credential cache.
   *
   * @param initial the secret already cached, or null to simulate an empty cache.
   * @return the backing reference, for asserting on what ended up cached.
   */
  private AtomicReference<Secret> givenCachedSecret(final Secret initial) {
    final AtomicReference<Secret> cache = new AtomicReference<>(initial);
    when(this.mockStorageService.get(eq(Secret.class), eq(SECRET_CACHE_KEY))).thenAnswer(i -> cache.get());
    doAnswer(invocation -> {
      cache.set(invocation.getArgument(1));
      return null;
    }).when(this.mockStorageService).set(eq(SECRET_CACHE_KEY), any());
    return cache;
  }

  private static Secret unexpiredSecret(final String password) {
    return new Secret(TEST_USERNAME, password, Instant.now().plusSeconds(600));
  }

  /** Builds a plugin with the default configuration, where connect retries are disabled. */
  private AwsSecretsManagerConnectionPlugin createPlugin() throws SQLException {
    return buildPlugin();
  }

  private AwsSecretsManagerConnectionPlugin createPlugin(
      final long retryTimeoutMs, final long retryIntervalMs) throws SQLException {
    SECRETS_MANAGER_CONNECT_RETRY_TIMEOUT_MS_PROPERTY.set(this.props, String.valueOf(retryTimeoutMs));
    SECRETS_MANAGER_CONNECT_RETRY_INTERVAL_MS_PROPERTY.set(this.props, String.valueOf(retryIntervalMs));
    return buildPlugin();
  }

  private AwsSecretsManagerConnectionPlugin buildPlugin() throws SQLException {
    final PluginServiceImpl pluginService = new PluginServiceImpl(
        this.mockServicesContainer,
        new ExceptionManager(),
        this.props,
        "url",
        TEST_PG_PROTOCOL,
        this.mockDialectManager,
        this.mockTargetDriverDialect,
        this.configurationProfile,
        this.mockSessionStateService);
    when(this.mockServicesContainer.getPluginService()).thenReturn(pluginService);

    return new AwsSecretsManagerConnectionPlugin(
        this.mockServicesContainer,
        this.props,
        (host, region) -> this.mockSecretsManagerClient,
        (secretId) -> this.mockGetValueRequest);
  }

  /**
   * Models one AWS Secrets Manager rotation from the driver's point of view.
   *
   * <p>The database switches to the post-rotation password immediately, standing in for
   * {@code setSecret} having already run. {@code AWSCURRENT} keeps resolving to the pre-rotation
   * password until {@code finishSecret}, simulated here as happening after a fixed number of
   * {@code GetSecretValue} calls.
   */
  private static final class RotationSimulator {
    private final String passwordBeforeRotation;
    private final String passwordAfterRotation;
    private final int promoteAwsCurrentAfterFetches;
    private final Properties props;
    private final Connection connection;
    private final AtomicInteger fetches = new AtomicInteger();
    private final AtomicInteger attempts = new AtomicInteger();

    RotationSimulator(
        final String passwordBeforeRotation,
        final String passwordAfterRotation,
        final int promoteAwsCurrentAfterFetches,
        final Properties props,
        final Connection connection) {
      this.passwordBeforeRotation = passwordBeforeRotation;
      this.passwordAfterRotation = passwordAfterRotation;
      this.promoteAwsCurrentAfterFetches = promoteAwsCurrentAfterFetches;
      this.props = props;
      this.connection = connection;
    }

    /** Answers a {@code GetSecretValue} call with whatever AWSCURRENT resolves to right now. */
    String getSecretValue() {
      final int fetchNumber = this.fetches.incrementAndGet();
      final String password = fetchNumber > this.promoteAwsCurrentAfterFetches
          ? this.passwordAfterRotation
          : this.passwordBeforeRotation;
      return "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + password + "\"}";
    }

    /**
     * Stands in for the target driver: accepts the connection only when the credentials the plugin
     * placed in the properties are the ones the database currently expects.
     */
    JdbcCallable<Connection, SQLException> connectFunc() {
      return () -> {
        this.attempts.incrementAndGet();
        final String offered = PropertyDefinition.PASSWORD.getString(this.props);
        if (!this.passwordAfterRotation.equals(offered)) {
          throw new SQLException(LOGIN_FAILED_MESSAGE, PG_INVALID_PASSWORD_STATE);
        }
        return this.connection;
      };
    }

    int secretFetches() {
      return this.fetches.get();
    }

    int connectAttempts() {
      return this.attempts.get();
    }
  }
}
