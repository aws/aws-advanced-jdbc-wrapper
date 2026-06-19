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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.REGION_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY;
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.Secret;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

@SuppressWarnings({"resource", "unchecked"})
public class AwsSecretsManagerConnectionPlugin2Test {

  private static final String TEST_PROTOCOL = "jdbc:aws-wrapper:postgresql:";
  private static final String TEST_REGION = "us-east-2";
  private static final String TEST_SECRET_ID = "secretId";
  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String VALID_SECRET_STRING =
      "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
  private static final String INVALID_SECRET_STRING = "{\"user\": \"x\", \"pass\": \"y\"}";
  private static final String TEST_HOST = "test-domain";
  private static final int TEST_PORT = 5432;

  private static final Pair<String, String> SECRET_CACHE_KEY = Pair.create(TEST_SECRET_ID, TEST_REGION);
  private static final HostSpec TEST_HOSTSPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(TEST_HOST).port(TEST_PORT).build();
  private static final GetSecretValueResponse VALID_GET_SECRET_VALUE_RESPONSE =
      GetSecretValueResponse.builder().secretString(VALID_SECRET_STRING).build();

  private Properties testProps;
  private AwsSecretsManagerConnectionPlugin2 plugin;
  private AutoCloseable closeable;

  /** A simple in-memory stand-in for the shared credential cache. */
  private final ConcurrentHashMap<Object, Secret> cache = new ConcurrentHashMap<>();

  @Mock FullServicesContainer mockServicesContainer;
  @Mock SecretsManagerClient mockSecretsManagerClient;
  @Mock GetSecretValueRequest mockGetValueRequest;
  @Mock JdbcCallable<Connection, SQLException> connectFunc;
  @Mock PluginServiceImpl mockService;
  @Mock StorageService mockStorageService;
  @Mock ConnectionPluginManager mockConnectionPluginManager;
  @Mock Connection mockConnection;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;

  @BeforeEach
  public void init() {
    closeable = MockitoAnnotations.openMocks(this);

    testProps = new Properties();
    REGION_PROPERTY.set(testProps, TEST_REGION);
    SECRET_ID_PROPERTY.set(testProps, TEST_SECRET_ID);

    when(mockServicesContainer.getConnectionPluginManager()).thenReturn(mockConnectionPluginManager);
    when(mockServicesContainer.getPluginService()).thenReturn(mockService);
    when(mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockConnectionPluginManager.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    // Back the mock StorageService with the in-memory map so the SWR flow can read/write secrets.
    when(mockStorageService.get(eq(Secret.class), any())).thenAnswer(inv -> cache.get(inv.getArgument(1)));
    org.mockito.Mockito.doAnswer(inv -> {
      cache.put(inv.getArgument(0), inv.getArgument(1));
      return null;
    }).when(mockStorageService).set(any(), any());

    this.plugin = new AwsSecretsManagerConnectionPlugin2(
        mockServicesContainer,
        testProps,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    cache.clear();
    AwsSecretsManagerConnectionPlugin2.pendingRefreshes.clear();
  }

  /** Non-expired cached secret: use it as-is, no fetch, no refresh. */
  @Test
  public void testConnectWithNonExpiredCachedSecret() throws SQLException {
    cache.put(SECRET_CACHE_KEY, new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().plusSeconds(300)));
    when(connectFunc.call()).thenReturn(mockConnection);

    final Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /** Cache miss (first connection): fetch synchronously. */
  @Test
  public void testConnectWithEmptyCache() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);
    when(connectFunc.call()).thenReturn(mockConnection);

    final Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    assertEquals(1, cache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /** Stale cache: connect immediately with stale creds, refresh in background. */
  @Test
  public void testConnectWithStaleCache() throws SQLException {
    final String staleUser = "staleUser";
    final String stalePass = "stalePass";
    cache.put(SECRET_CACHE_KEY, new Secret(staleUser, stalePass, Instant.now().minusSeconds(10)));

    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);
    when(connectFunc.call()).thenReturn(mockConnection);

    final Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    // The connection used the stale credentials.
    assertEquals(staleUser, testProps.get(PropertyDefinition.USER.name));
    assertEquals(stalePass, testProps.get(PropertyDefinition.PASSWORD.name));
    verify(connectFunc).call();

    waitForPendingRefreshes();

    final Secret updated = cache.get(SECRET_CACHE_KEY);
    assertNotNull(updated);
    assertEquals(TEST_USERNAME, updated.getUsername());
    assertEquals(TEST_PASSWORD, updated.getPassword());
  }

  /** Secrets Manager down but a (stale) cache entry exists: connect succeeds with stale creds. */
  @Test
  public void testSecretsManagerDownWithCachedEntry() throws SQLException {
    cache.put(SECRET_CACHE_KEY, new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10)));

    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient)
        .getSecretValue(this.mockGetValueRequest);
    when(connectFunc.call()).thenReturn(mockConnection);

    final Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /** Secrets Manager down and no cache entry: connection fails. */
  @Test
  public void testSecretsManagerDownNoCachedEntry() throws SQLException {
    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient)
        .getSecretValue(this.mockGetValueRequest);

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));

    assertEquals(0, cache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc, never()).call();
  }

  /** Login failure with stale credentials triggers a synchronous re-fetch and retry. */
  @Test
  public void testLoginFailureRetryWithFreshCredentials() throws SQLException {
    cache.put(SECRET_CACHE_KEY, new Secret("oldUser", "oldPass", Instant.now().minusSeconds(10)));

    final SQLException loginException = new SQLException("login failed", "28P01");
    when(connectFunc.call()).thenThrow(loginException).thenReturn(mockConnection);
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    final Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(connectFunc, times(2)).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /** Login failure with freshly-fetched (cache-miss) credentials must NOT retry. */
  @Test
  public void testLoginFailureWithFreshlyFetchedCredentialsSkipsRetry() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    final SQLException loginException = new SQLException("login failed", "28P01");
    when(connectFunc.call()).thenThrow(loginException);
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);

    final SQLException thrown = assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));

    assertEquals(loginException, thrown);
    verify(connectFunc, times(1)).call();
    verify(this.mockSecretsManagerClient, times(1)).getSecretValue(this.mockGetValueRequest);
  }

  /** forceConnect follows the same SWR logic. */
  @Test
  public void testForceConnect() throws SQLException {
    cache.put(SECRET_CACHE_KEY, new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().plusSeconds(300)));
    when(connectFunc.call()).thenReturn(mockConnection);

    final Connection result = this.plugin.forceConnect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
  }

  /** Invalid secret JSON propagates as SQLException. */
  @Test
  public void testConnectWithInvalidSecretFormatThrowsSqlException() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(GetSecretValueResponse.builder().secretString(INVALID_SECRET_STRING).build());

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));
    verify(connectFunc, never()).call();
  }

  /** Missing required parameters throw at construction time. */
  @ParameterizedTest
  @MethodSource("missingArguments")
  public void testMissingRequiredParameters(final Properties properties) {
    assertThrows(RuntimeException.class, () -> new AwsSecretsManagerConnectionPlugin2(
        mockServicesContainer,
        properties,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest));
  }

  /** Region is parsed from an ARN secret id. */
  @ParameterizedTest
  @MethodSource("arnArguments")
  public void testArnParsing(final String arn, final Region expectedRegion) {
    final Properties props = new Properties();
    SECRET_ID_PROPERTY.set(props, arn);

    final AwsSecretsManagerConnectionPlugin2 arnPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockServicesContainer,
        props,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    assertEquals(expectedRegion, Region.of(arnPlugin.secretKey.getValue2()));
  }

  /** Expiration time below the minimum is clamped. */
  @Test
  public void testExpirationTimeClampedToMinimum() {
    final Properties props = new Properties();
    REGION_PROPERTY.set(props, TEST_REGION);
    SECRET_ID_PROPERTY.set(props, TEST_SECRET_ID);
    SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY.set(props, "10");

    final AwsSecretsManagerConnectionPlugin2 clampedPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockServicesContainer,
        props,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    assertEquals(AwsSecretsManagerConnectionPlugin2.MIN_EXPIRATION_TIME_SECONDS, clampedPlugin.secretExpirationTime);
  }

  /** Thundering herd: many concurrent stale connects trigger only one Secrets Manager call. */
  @Test
  public void testThunderingHerdPrevention() throws Exception {
    cache.put(SECRET_CACHE_KEY, new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10)));

    final int concurrentCalls = 5;
    final AtomicInteger fetchCount = new AtomicInteger(0);
    final CountDownLatch fetchCanProceed = new CountDownLatch(1);
    final CountDownLatch allConnectsReturned = new CountDownLatch(concurrentCalls);

    final SecretsManagerClient slowClient = new SecretsManagerClient() {
      @Override
      public GetSecretValueResponse getSecretValue(final GetSecretValueRequest request) {
        fetchCount.incrementAndGet();
        try {
          fetchCanProceed.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return VALID_GET_SECRET_VALUE_RESPONSE;
      }

      @Override
      public String serviceName() {
        return "secretsmanager";
      }

      @Override
      public void close() {
      }
    };

    final AwsSecretsManagerConnectionPlugin2 slowPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockServicesContainer,
        testProps,
        (host, r) -> slowClient,
        (id) -> mockGetValueRequest);

    when(connectFunc.call()).thenAnswer(inv -> {
      allConnectsReturned.countDown();
      return mockConnection;
    });

    final ExecutorService executor = Executors.newFixedThreadPool(concurrentCalls);
    final List<Future<Connection>> futures = new ArrayList<>();
    for (int i = 0; i < concurrentCalls; i++) {
      futures.add(executor.submit(() ->
          slowPlugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, copyProps(), true, connectFunc)));
    }

    assertTrue(allConnectsReturned.await(5, TimeUnit.SECONDS), "All connects should have returned");
    fetchCanProceed.countDown();

    for (final Future<Connection> future : futures) {
      assertNotNull(future.get(5, TimeUnit.SECONDS));
    }
    executor.shutdown();
    waitForPendingRefreshes();

    assertEquals(1, fetchCount.get(),
        "Expected exactly one Secrets Manager fetch, but got " + fetchCount.get());
  }

  /** pendingRefreshes is cleaned up after the async refresh completes. */
  @Test
  public void testPendingRefreshesCleanedUpAfterCompletion() throws Exception {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);
    cache.put(SECRET_CACHE_KEY, new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10)));
    when(connectFunc.call()).thenReturn(mockConnection);

    this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    waitForPendingRefreshes();

    final long deadline = System.currentTimeMillis() + 5000;
    while (!AwsSecretsManagerConnectionPlugin2.pendingRefreshes.isEmpty()
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(AwsSecretsManagerConnectionPlugin2.pendingRefreshes.isEmpty(),
        "pendingRefreshes should be empty after async refresh completes");
  }

  /** Interrupt during the synchronous fetch restores the interrupt flag and throws SQLException. */
  @Test
  public void testInterruptedDuringSynchronousFetch() throws Exception {
    final CompletableFuture<Secret> neverCompleting = new CompletableFuture<>();
    AwsSecretsManagerConnectionPlugin2.pendingRefreshes.put(this.plugin.secretKey, neverCompleting);

    final List<Exception> caught = new ArrayList<>();
    final boolean[] wasInterrupted = {false};

    final Thread connectThread = new Thread(() -> {
      try {
        plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, copyProps(), true, connectFunc);
      } catch (final Exception e) {
        synchronized (caught) {
          caught.add(e);
        }
      }
      wasInterrupted[0] = Thread.currentThread().isInterrupted();
    });
    connectThread.start();

    final long deadline = System.currentTimeMillis() + 5000;
    while (connectThread.getState() != Thread.State.TIMED_WAITING
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(Thread.State.TIMED_WAITING, connectThread.getState());

    connectThread.interrupt();
    connectThread.join(5000);

    assertFalse(connectThread.isAlive(), "Thread should have terminated");
    assertEquals(1, caught.size());
    assertTrue(caught.get(0) instanceof SQLException,
        "Exception should be SQLException, got: " + caught.get(0).getClass().getName());
    assertTrue(wasInterrupted[0], "Thread interrupt status should be restored");
  }

  private Properties copyProps() {
    final Properties p = new Properties();
    p.putAll(testProps);
    return p;
  }

  private void waitForPendingRefreshes() {
    for (final CompletableFuture<Secret> future : AwsSecretsManagerConnectionPlugin2.pendingRefreshes.values()) {
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (final Exception e) {
        // ignore
      }
    }
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

  private static Stream<Arguments> arnArguments() {
    return Stream.of(
        Arguments.of("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", Region.US_EAST_2),
        Arguments.of("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", Region.US_WEST_1),
        Arguments.of(
            "arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo",
            Region.US_EAST_2)
    );
  }
}
