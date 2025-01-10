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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.REGION_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY;

import com.mysql.cj.exceptions.CJException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.ExceptionManager;
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;
import software.amazon.jdbc.exceptions.PgExceptionHandler;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class AwsSecretsManagerConnectionPluginTest {

  private static final String TEST_PG_PROTOCOL = "jdbc:aws-wrapper:postgresql:";
  private static final String TEST_MYSQL_PROTOCOL = "jdbc:aws-wrapper:mysql:";
  private static final String TEST_REGION = "us-east-2";
  private static final String TEST_SECRET_ID = "secretId";
  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String VALID_SECRET_STRING =
      "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
  private static final String INVALID_SECRET_STRING = "{username: invalid, password: invalid}";
  private static final String TEST_HOST = "test-domain";
  private static final String TEST_SQL_ERROR = "SQL exception error message";
  private static final String UNHANDLED_ERROR_CODE = "HY000";
  private static final int TEST_PORT = 5432;
  private static final Pair<String, String> SECRET_CACHE_KEY = Pair.create(TEST_SECRET_ID, TEST_REGION);
  private static final AwsSecretsManagerConnectionPlugin.Secret TEST_SECRET =
      new AwsSecretsManagerConnectionPlugin.Secret("testUser", "testPassword");
  private static final HostSpec TEST_HOSTSPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(TEST_HOST).port(TEST_PORT).build();
  private static final GetSecretValueResponse VALID_GET_SECRET_VALUE_RESPONSE =
      GetSecretValueResponse.builder().secretString(VALID_SECRET_STRING).build();
  private static final GetSecretValueResponse INVALID_GET_SECRET_VALUE_RESPONSE =
      GetSecretValueResponse.builder().secretString(INVALID_SECRET_STRING).build();
  private static final Properties TEST_PROPS = new Properties();
  private AwsSecretsManagerConnectionPlugin plugin;

  private AutoCloseable closeable;

  @Mock SecretsManagerClient mockSecretsManagerClient;
  @Mock GetSecretValueRequest mockGetValueRequest;
  @Mock JdbcCallable<Connection, SQLException> connectFunc;
  @Mock PluginServiceImpl mockService;
  @Mock ConnectionPluginManager mockConnectionPluginManager;
  @Mock Dialect mockTopologyAwareDialect;
  @Mock DialectManager mockDialectManager;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;
  @Mock TargetDriverDialect mockTargetDriverDialect;
  ConfigurationProfile configurationProfile = ConfigurationProfileBuilder.get().withName("test").build();

  @Mock SessionStateService mockSessionStateService;

  @BeforeEach
  public void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    REGION_PROPERTY.set(TEST_PROPS, TEST_REGION);
    SECRET_ID_PROPERTY.set(TEST_PROPS, TEST_SECRET_ID);

    when(mockDialectManager.getDialect(anyString(), anyString(), any(Properties.class)))
        .thenReturn(mockTopologyAwareDialect);

    when(mockService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    this.plugin = new AwsSecretsManagerConnectionPlugin(
        mockService,
        TEST_PROPS,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    when(mockDialectManager.getDialect(anyString(), anyString(), any(Properties.class)))
        .thenReturn(mockTopologyAwareDialect);

    when(mockService.getHostSpecBuilder()).thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    AwsSecretsManagerCacheHolder.clearCache();
    TEST_PROPS.clear();
  }

  /**
   * The plugin will successfully open a connection with a cached secret.
   */
  @Test
  public void testConnectWithCachedSecrets() throws SQLException {
    // Add initial cached secret to be used for a connection.
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, TEST_SECRET);

    this.plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, TEST_PROPS, true, this.connectFunc);

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(this.connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * The plugin will attempt to open a connection with an empty secret cache. The plugin will fetch the secret from the
   * AWS Secrets Manager.
   */
  @Test
  public void testConnectWithNewSecrets() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    this.plugin.connect(TEST_PG_PROTOCOL, TEST_HOSTSPEC, TEST_PROPS, true, this.connectFunc);

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  @ParameterizedTest
  @MethodSource("missingArguments")
  public void testMissingRequiredParameters(final Properties properties) {
    assertThrows(RuntimeException.class, () -> new AwsSecretsManagerConnectionPlugin(
        mockService,
        properties,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with a generic SQL exception.
   * In this case, the plugin will rethrow the error back to the user.
   */
  @Test
  public void testFailedInitialConnectionWithUnhandledError() throws SQLException {
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, TEST_SECRET);
    final SQLException failedFirstConnectionGenericException = new SQLException(TEST_SQL_ERROR, UNHANDLED_ERROR_CODE);
    doThrow(failedFirstConnectionGenericException).when(connectFunc).call();

    final SQLException connectionFailedException = assertThrows(
        SQLException.class,
        () -> this.plugin.connect(
            TEST_PG_PROTOCOL,
            TEST_HOSTSPEC,
            TEST_PROPS,
            true,
            this.connectFunc));

    assertEquals(TEST_SQL_ERROR, connectionFailedException.getMessage());
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * The plugin will attempt to open a connection with a cached secret, but it will fail with an access error. In this
   * case, the plugin will fetch the secret and will retry the connection.
   */
  @ParameterizedTest
  @MethodSource("provideExceptionCodeForDifferentDrivers")
  public void testConnectWithNewSecretsAfterTryingWithCachedSecrets(
      String accessError,
      String protocol,
      ExceptionHandler exceptionHandler) throws SQLException {
    this.plugin = new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(
            mockConnectionPluginManager,
            new ExceptionManager(),
            TEST_PROPS,
            "url",
            protocol,
            mockDialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            mockSessionStateService),
        TEST_PROPS,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    // Fail the initial connection attempt with cached secret.
    // Second attempt should be successful.
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, TEST_SECRET);
    final SQLException failedFirstConnectionAccessException = new SQLException(TEST_SQL_ERROR,
        accessError);
    doThrow(failedFirstConnectionAccessException).when(connectFunc).call();
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    when(mockTopologyAwareDialect.getExceptionHandler()).thenReturn(exceptionHandler);

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(
            TEST_PG_PROTOCOL,
            TEST_HOSTSPEC,
            TEST_PROPS,
            true,
            this.connectFunc));

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc, times(2)).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * The plugin will attempt to open a connection after fetching a secret, but it will fail because the returned secret
   * could not be parsed.
   */
  @Test
  public void testFailedToReadSecrets() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(INVALID_GET_SECRET_VALUE_RESPONSE);

    final SQLException readSecretsFailedException =
        assertThrows(
            SQLException.class,
            () -> this.plugin.connect(
                TEST_PG_PROTOCOL,
                TEST_HOSTSPEC,
                TEST_PROPS,
                true,
                this.connectFunc));

    assertEquals(
        readSecretsFailedException.getMessage(),
        Messages.get(
            "AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
    assertEquals(0, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(this.connectFunc, never()).call();
  }

  /**
   * The plugin will attempt to open a connection after fetching a secret, but it will fail because an exception was
   * thrown by the AWS Secrets Manager.
   */
  @Test
  public void testFailedToGetSecrets() throws SQLException {
    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);

    final SQLException getSecretsFailedException =
        assertThrows(
            SQLException.class,
            () -> this.plugin.connect(
                TEST_PG_PROTOCOL,
                TEST_HOSTSPEC,
                TEST_PROPS,
                true,
                this.connectFunc));

    assertEquals(
        getSecretsFailedException.getMessage(),
        Messages.get(
            "AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
    assertEquals(0, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(this.connectFunc, never()).call();
  }

  @ParameterizedTest
  @ValueSource(strings = {"28000", "28P01"})
  public void testFailedInitialConnectionWithWrappedGenericError(final String accessError) throws SQLException {
    this.plugin = new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(
            mockConnectionPluginManager,
            new ExceptionManager(),
            TEST_PROPS,
            "url",
            TEST_PG_PROTOCOL,
            mockDialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            mockSessionStateService),
        TEST_PROPS,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    // Fail the initial connection attempt with a wrapped exception.
    // Second attempt should be successful.
    final SQLException targetException = new SQLException(TEST_SQL_ERROR, accessError);
    final SQLException wrappedException = new SQLException(targetException);
    doThrow(wrappedException).when(connectFunc).call();
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    when(mockTopologyAwareDialect.getExceptionHandler()).thenReturn(new PgExceptionHandler());

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(
            TEST_PG_PROTOCOL,
            TEST_HOSTSPEC,
            TEST_PROPS,
            true,
            this.connectFunc));

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  @Test
  public void testConnectWithWrappedMySQLException() throws SQLException {
    this.plugin = new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(
            mockConnectionPluginManager,
            new ExceptionManager(),
            TEST_PROPS,
            "url",
            TEST_MYSQL_PROTOCOL,
            mockDialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            mockSessionStateService),
        TEST_PROPS,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    final CJException targetException = new CJException("28000");
    final SQLException wrappedException = new SQLException(targetException);

    doThrow(wrappedException).when(connectFunc).call();
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    when(mockTopologyAwareDialect.getExceptionHandler()).thenReturn(new PgExceptionHandler());

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(
            TEST_MYSQL_PROTOCOL,
            TEST_HOSTSPEC,
            TEST_PROPS,
            true,
            this.connectFunc));

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  @Test
  public void testConnectWithWrappedPostgreSQLException() throws SQLException {
    this.plugin = new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(
            mockConnectionPluginManager,
            new ExceptionManager(),
            TEST_PROPS,
            "url",
            TEST_PG_PROTOCOL,
            mockDialectManager,
            mockTargetDriverDialect,
            configurationProfile,
            mockSessionStateService),
        TEST_PROPS,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    final PSQLException targetException = new PSQLException("login error", PSQLState.INVALID_PASSWORD, null);
    final SQLException wrappedException = new SQLException(targetException);

    doThrow(wrappedException).when(connectFunc).call();
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    when(mockTopologyAwareDialect.getExceptionHandler()).thenReturn(new PgExceptionHandler());

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(
            TEST_PG_PROTOCOL,
            TEST_HOSTSPEC,
            TEST_PROPS,
            true,
            this.connectFunc));

    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, TEST_PROPS.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, TEST_PROPS.get(PropertyDefinition.PASSWORD.name));
  }

  @ParameterizedTest
  @MethodSource("arnArguments")
  public void testConnectViaARN(final String arn, final Region expectedRegionParsedFromARN)
      throws SQLException {
    final Properties props = new Properties();

    SECRET_ID_PROPERTY.set(props, arn);

    this.plugin = spy(new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(mockConnectionPluginManager, props, "url", TEST_PG_PROTOCOL, mockTargetDriverDialect),
        props,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest));

    final Pair<String, String> secret = this.plugin.secretKey;
    assertEquals(expectedRegionParsedFromARN, Region.of(secret.getValue2()));
  }

  @ParameterizedTest
  @MethodSource("arnArguments")
  public void testConnectionWithRegionParameterAndARN(final String arn, final Region regionParsedFromARN)
      throws SQLException {
    final Region expectedRegion = Region.US_ISO_EAST_1;

    final Properties props = new Properties();
    SECRET_ID_PROPERTY.set(props, arn);
    REGION_PROPERTY.set(props, expectedRegion.toString());

    this.plugin = spy(new AwsSecretsManagerConnectionPlugin(
        new PluginServiceImpl(mockConnectionPluginManager, props, "url", TEST_PG_PROTOCOL, mockTargetDriverDialect),
        props,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest));

    final Pair<String, String> secret = this.plugin.secretKey;
    // The region specified in `secretsManagerRegion` should override the region parsed from ARN.
    assertNotEquals(regionParsedFromARN, Region.of(secret.getValue2()));
    assertEquals(expectedRegion, Region.of(secret.getValue2()));
  }

  private static Stream<Arguments> provideExceptionCodeForDifferentDrivers() {
    return Stream.of(
        Arguments.of("28000", TEST_MYSQL_PROTOCOL, new MySQLExceptionHandler()),
        Arguments.of("28P01", TEST_PG_PROTOCOL, new PgExceptionHandler())
    );
  }

  private static Stream<Arguments> arnArguments() {
    return Stream.of(
        Arguments.of("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", Region.US_EAST_2),
        Arguments.of("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", Region.US_WEST_1),
        Arguments.of(
            "arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo",
            Region.US_EAST_2)
    );
  }

  private static Stream<Arguments> missingArguments() {
    final Properties missingId = new Properties();
    REGION_PROPERTY.set(missingId, TEST_REGION);

    final Properties missingRegion = new Properties();
    SECRET_ID_PROPERTY.set(missingRegion, TEST_SECRET_ID);

    return Stream.of(
        Arguments.of(missingId),
        Arguments.of(missingRegion)
    );
  }
}
