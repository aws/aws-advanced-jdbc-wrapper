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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import static software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.Secret;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin2.SecretKey;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.telemetry.GaugeCallable;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

@SuppressWarnings("resource")
public class AwsSecretsManagerConnectionPlugin2Test {

  private static final String TEST_PROTOCOL = "jdbc:aws-wrapper:postgresql:";
  private static final String TEST_REGION = "us-east-2";
  private static final String TEST_SECRET_ID = "secretId";
  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String VALID_SECRET_STRING =
      "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
  private static final String TEST_HOST = "test-domain";
  private static final int TEST_PORT = 5432;
  private static final long EXPIRATION_TIME = 900;
  private static final Pair<String, String> SECRET_CACHE_KEY = Pair.create(TEST_SECRET_ID, TEST_REGION);
  private static final HostSpec TEST_HOSTSPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(TEST_HOST).port(TEST_PORT).build();
  private static final GetSecretValueResponse VALID_GET_SECRET_VALUE_RESPONSE =
      GetSecretValueResponse.builder().secretString(VALID_SECRET_STRING).build();
  private Properties testProps;

  private AwsSecretsManagerConnectionPlugin2 plugin;
  private AutoCloseable closeable;

  @Mock SecretsManagerClient mockSecretsManagerClient;
  @Mock GetSecretValueRequest mockGetValueRequest;
  @Mock JdbcCallable<Connection, SQLException> connectFunc;
  @Mock PluginService mockService;
  @Mock Connection mockConnection;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryGauge mockTelemetryGauge;

  @BeforeEach
  public void init() {
    closeable = MockitoAnnotations.openMocks(this);

    testProps = new Properties();
    REGION_PROPERTY.set(testProps, TEST_REGION);
    SECRET_ID_PROPERTY.set(testProps, TEST_SECRET_ID);

    when(mockService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.openTelemetryContext(eq(null), any())).thenReturn(mockTelemetryContext);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    // noinspection unchecked
    when(mockTelemetryFactory.createGauge(anyString(), any(GaugeCallable.class))).thenReturn(mockTelemetryGauge);

    this.plugin = new AwsSecretsManagerConnectionPlugin2(
        mockService,
        testProps,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
    AwsSecretsManagerCacheHolder.clearCache();
    AwsSecretsManagerConnectionPlugin2.pendingRefreshes.clear();
  }

  /**
   * Connect with a non-expired cached secret. No refresh should be triggered.
   */
  @Test
  public void testConnectWithNonExpiredCachedSecret() throws SQLException {
    final Secret nonExpiredSecret = new Secret(TEST_USERNAME, TEST_PASSWORD,
        Instant.now().plusSeconds(300));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, nonExpiredSecret);
    when(connectFunc.call()).thenReturn(mockConnection);

    Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * Cache miss — first connection. Should fetch synchronously.
   */
  @Test
  public void testConnectWithEmptyCache() throws SQLException {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);
    when(connectFunc.call()).thenReturn(mockConnection);

    Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    assertEquals(1, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * Connect with stale (expired) cache: should use stale credentials immediately without blocking,
   * and trigger an async refresh in the background.
   */
  @Test
  public void testConnectWithStaleCache() throws SQLException {
    final String staleUser = "staleUser";
    final String stalePass = "stalePass";
    final Secret expiredSecret = new Secret(staleUser, stalePass, Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);
    when(connectFunc.call()).thenReturn(mockConnection);

    Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    // Should use stale credentials for this connection
    assertEquals(staleUser, testProps.get(PropertyDefinition.USER.name));
    assertEquals(stalePass, testProps.get(PropertyDefinition.PASSWORD.name));
    verify(connectFunc).call();

    // Wait for async refresh to complete
    waitForPendingRefreshes();

    // After async refresh, cache should be updated with new credentials
    Secret updatedSecret = AwsSecretsManagerCacheHolder.secretsCache.get(SECRET_CACHE_KEY);
    assertNotNull(updatedSecret);
    assertEquals(TEST_USERNAME, updatedSecret.getUsername());
    assertEquals(TEST_PASSWORD, updatedSecret.getPassword());
  }

  /**
   * Secrets Manager is down but cached entry exists — connection should succeed with stale credentials.
   */
  @Test
  public void testSecretsManagerDownWithCachedEntry() throws SQLException {
    final Secret expiredSecret = new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient)
        .getSecretValue(this.mockGetValueRequest);
    when(connectFunc.call()).thenReturn(mockConnection);

    Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(connectFunc).call();
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * Secrets Manager is down and no cached entry — connection should fail.
   */
  @Test
  public void testSecretsManagerDownNoCachedEntry() {
    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient)
        .getSecretValue(this.mockGetValueRequest);

    assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));

    assertEquals(0, AwsSecretsManagerCacheHolder.secretsCache.size());
    verify(this.mockSecretsManagerClient).getSecretValue(this.mockGetValueRequest);
  }

  /**
   * Login failure with stale credentials triggers a synchronous re-fetch and retry.
   */
  @Test
  public void testLoginFailureRetryWithFreshCredentials() throws SQLException {
    final Secret expiredSecret = new Secret("oldUser", "oldPass", Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    final SQLException loginException = new SQLException("login failed", "28P01");
    when(connectFunc.call())
        .thenThrow(loginException)
        .thenReturn(mockConnection);
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    Connection result = this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(connectFunc, times(2)).call();
    // After retry, should have new credentials
    assertEquals(TEST_USERNAME, testProps.get(PropertyDefinition.USER.name));
    assertEquals(TEST_PASSWORD, testProps.get(PropertyDefinition.PASSWORD.name));
  }

  /**
   * Login failure + Secrets Manager down — should throw the original login exception
   * with the fetch failure attached as a suppressed exception.
   */
  @Test
  public void testLoginFailureSecretsManagerDown() throws SQLException {
    final Secret expiredSecret = new Secret("oldUser", "oldPass", Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    final SQLException loginException = new SQLException("login failed", "28P01");
    when(connectFunc.call()).thenThrow(loginException);
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);
    doThrow(SecretsManagerException.class).when(this.mockSecretsManagerClient)
        .getSecretValue(this.mockGetValueRequest);

    final SQLException thrown = assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));

    assertEquals(loginException, thrown, "Should throw the original login exception");
    assertEquals("28P01", thrown.getSQLState());
    assertEquals(1, thrown.getSuppressed().length,
        "Fetch failure should be attached as a suppressed exception");
  }

  /**
   * Thundering herd prevention: multiple concurrent connects with expired cache
   * should trigger only one fetch to Secrets Manager.
   */
  @Test
  public void testThunderingHerdPrevention() throws Exception {
    final Secret expiredSecret = new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    final int concurrentCalls = 5;
    final AtomicInteger fetchCount = new AtomicInteger(0);
    final CountDownLatch fetchCanProceed = new CountDownLatch(1);
    // Wait for all connect calls to complete before releasing the SM client.
    // This ensures all threads have passed through triggerAsyncRefreshIfNeeded
    // (which happens before connectFunc.call()) before the async refresh completes.
    final CountDownLatch allConnectsReturned = new CountDownLatch(concurrentCalls);

    // Create a slow-responding SecretsManagerClient that counts invocations
    SecretsManagerClient slowClient = new SecretsManagerClient() {
      @Override
      public GetSecretValueResponse getSecretValue(GetSecretValueRequest request) {
        fetchCount.incrementAndGet();
        try {
          fetchCanProceed.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
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

    AwsSecretsManagerConnectionPlugin2 slowPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockService,
        testProps,
        (host, r) -> slowClient,
        (id) -> mockGetValueRequest);

    when(connectFunc.call()).thenAnswer(invocation -> {
      allConnectsReturned.countDown();
      return mockConnection;
    });

    // Trigger multiple async refreshes concurrently
    ExecutorService executor = Executors.newFixedThreadPool(concurrentCalls);
    List<Future<Connection>> futures = new ArrayList<>();

    for (int i = 0; i < concurrentCalls; i++) {
      futures.add(executor.submit(() ->
          slowPlugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, new Properties() {{
            putAll(testProps);
          }}, true, connectFunc)));
    }

    // Wait for all connections to have completed connectFunc.call().
    // At this point all threads have passed through triggerAsyncRefreshIfNeeded
    // with stale credentials, so no more compute() calls will happen.
    assertTrue(allConnectsReturned.await(5, TimeUnit.SECONDS),
        "All connects should have returned");

    // Now release the slow SM client
    fetchCanProceed.countDown();

    // Collect all results
    for (Future<Connection> future : futures) {
      assertNotNull(future.get(5, TimeUnit.SECONDS));
    }

    executor.shutdown();

    // Wait for any pending async refreshes
    waitForPendingRefreshes();

    // Only one fetch should have been triggered despite multiple concurrent connects
    assertEquals(1, fetchCount.get(),
        "Expected exactly one Secrets Manager fetch, but got " + fetchCount.get());
  }

  /**
   * Missing required parameters should throw RuntimeException.
   */
  @ParameterizedTest
  @MethodSource("missingArguments")
  public void testMissingRequiredParameters(final Properties properties) {
    assertThrows(RuntimeException.class, () -> new AwsSecretsManagerConnectionPlugin2(
        mockService,
        properties,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest));
  }

  /**
   * ARN parsing: region should be extracted from ARN format.
   */
  @ParameterizedTest
  @MethodSource("arnArguments")
  public void testArnParsing(final String arn, final Region expectedRegion) {
    final Properties props = new Properties();
    SECRET_ID_PROPERTY.set(props, arn);

    AwsSecretsManagerConnectionPlugin2 arnPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockService,
        props,
        (host, r) -> mockSecretsManagerClient,
        (id) -> mockGetValueRequest);

    assertEquals(expectedRegion, Region.of(arnPlugin.secretKey.region));
  }

  /**
   * forceConnect follows the same SWR logic.
   */
  @Test
  public void testForceConnect() throws SQLException {
    final Secret nonExpiredSecret = new Secret(TEST_USERNAME, TEST_PASSWORD,
        Instant.now().plusSeconds(300));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, nonExpiredSecret);
    when(connectFunc.call()).thenReturn(mockConnection);

    Connection result = this.plugin.forceConnect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    assertNotNull(result);
    verify(this.mockSecretsManagerClient, never()).getSecretValue(this.mockGetValueRequest);
    verify(connectFunc).call();
  }

  /**
   * Login failure with freshly fetched credentials should NOT trigger a retry.
   * If credentials were just fetched from Secrets Manager and still fail to authenticate,
   * re-fetching the same credentials would be pointless.
   */
  @Test
  public void testLoginFailureWithFreshlyFetchedCredentialsSkipsRetry() throws SQLException {
    // Empty cache — forces synchronous fetch, so wasFreshlyFetched will be true
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    final SQLException loginException = new SQLException("login failed", "28P01");
    when(connectFunc.call()).thenThrow(loginException);
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);

    final SQLException thrown = assertThrows(
        SQLException.class,
        () -> this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc));

    assertEquals(loginException, thrown, "Should throw the original login exception without retry");
    // connectFunc should be called only once — no retry since credentials were freshly fetched
    verify(connectFunc, times(1)).call();
    // Secrets Manager should also be called only once — no re-fetch
    verify(this.mockSecretsManagerClient, times(1)).getSecretValue(this.mockGetValueRequest);
  }

  /**
   * clearCache() should remove all cached secrets.
   */
  @Test
  public void testClearCache() {
    final Secret secret1 = new Secret("user1", "pass1", Instant.now().plusSeconds(300));
    final Secret secret2 = new Secret("user2", "pass2", Instant.now().plusSeconds(300));
    AwsSecretsManagerCacheHolder.secretsCache.put(Pair.create("secret1", "us-east-1"), secret1);
    AwsSecretsManagerCacheHolder.secretsCache.put(Pair.create("secret2", "us-west-2"), secret2);

    assertEquals(2, AwsSecretsManagerCacheHolder.secretsCache.size());

    AwsSecretsManagerConnectionPlugin2.clearCache();

    assertEquals(0, AwsSecretsManagerCacheHolder.secretsCache.size());
  }

  /**
   * After an async refresh completes, the entry in pendingRefreshes should be cleaned up
   * by the whenComplete callback.
   */
  @Test
  public void testPendingRefreshesCleanedUpAfterCompletion() throws Exception {
    when(this.mockSecretsManagerClient.getSecretValue(this.mockGetValueRequest))
        .thenReturn(VALID_GET_SECRET_VALUE_RESPONSE);

    final Secret expiredSecret = new Secret(TEST_USERNAME, TEST_PASSWORD, Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);
    when(connectFunc.call()).thenReturn(mockConnection);

    this.plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC, testProps, true, connectFunc);

    // Wait for async refresh to complete
    waitForPendingRefreshes();

    // Allow the whenComplete callback to execute
    final long deadline = System.currentTimeMillis() + 5000;
    while (!AwsSecretsManagerConnectionPlugin2.pendingRefreshes.isEmpty()
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }

    assertTrue(AwsSecretsManagerConnectionPlugin2.pendingRefreshes.isEmpty(),
        "pendingRefreshes should be empty after async refresh completes");
  }

  /**
   * When the calling thread is interrupted while waiting for a synchronous fetch,
   * the thread's interrupt status should be restored and a SQLException thrown.
   */
  @Test
  public void testInterruptedDuringSynchronousFetch() throws Exception {
    // Pre-populate pendingRefreshes with a never-completing future so the
    // calling thread blocks on future.get()
    final CompletableFuture<Secret> neverCompletingFuture = new CompletableFuture<>();
    AwsSecretsManagerConnectionPlugin2.pendingRefreshes.put(this.plugin.secretKey, neverCompletingFuture);

    // No cached secret → forces synchronous fetch path → blocks on the never-completing future
    final AtomicReference<Exception> caughtException = new AtomicReference<>();
    final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    final Thread connectThread = new Thread(() -> {
      try {
        plugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC,
            new Properties() {{ putAll(testProps); }}, true, connectFunc);
      } catch (Exception e) {
        caughtException.set(e);
      }
      wasInterrupted.set(Thread.currentThread().isInterrupted());
    });
    connectThread.start();

    // Wait for the thread to be blocking on future.get(timeout)
    final long deadline = System.currentTimeMillis() + 5000;
    while (connectThread.getState() != Thread.State.TIMED_WAITING
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(Thread.State.TIMED_WAITING, connectThread.getState(),
        "Thread should be in TIMED_WAITING state (blocking on future.get with timeout)");

    // Interrupt the thread
    connectThread.interrupt();

    connectThread.join(5000);
    assertFalse(connectThread.isAlive(), "Thread should have terminated");
    assertNotNull(caughtException.get(), "Should have thrown an exception");
    assertTrue(caughtException.get() instanceof SQLException,
        "Exception should be SQLException, got: " + caughtException.get().getClass().getName());
    assertTrue(wasInterrupted.get(), "Thread interrupt status should be restored");
  }

  /**
   * When a login failure triggers a force-fetch while an async refresh is already
   * in flight (from the stale-cache path), the force-fetch should join the existing
   * refresh future rather than starting a new one, resulting in only one Secrets
   * Manager API call.
   */
  @Test
  public void testForceFetchJoinsInFlightAsyncRefresh() throws Exception {
    final Secret expiredSecret = new Secret("oldUser", "oldPass", Instant.now().minusSeconds(10));
    AwsSecretsManagerCacheHolder.secretsCache.put(SECRET_CACHE_KEY, expiredSecret);

    final AtomicInteger fetchCount = new AtomicInteger(0);
    final CountDownLatch fetchStarted = new CountDownLatch(1);
    final CountDownLatch fetchCanProceed = new CountDownLatch(1);
    final CountDownLatch loginFailureOccurred = new CountDownLatch(1);

    // Create a slow-responding SecretsManagerClient that counts invocations
    SecretsManagerClient slowClient = new SecretsManagerClient() {
      @Override
      public GetSecretValueResponse getSecretValue(GetSecretValueRequest request) {
        fetchCount.incrementAndGet();
        fetchStarted.countDown();
        try {
          fetchCanProceed.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
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

    AwsSecretsManagerConnectionPlugin2 slowPlugin = new AwsSecretsManagerConnectionPlugin2(
        mockService,
        testProps,
        (host, r) -> slowClient,
        (id) -> mockGetValueRequest);

    final SQLException loginException = new SQLException("login failed", "28P01");
    final AtomicInteger connectCallCount = new AtomicInteger(0);
    when(connectFunc.call()).thenAnswer(invocation -> {
      if (connectCallCount.incrementAndGet() == 1) {
        loginFailureOccurred.countDown();
        throw loginException;
      }
      return mockConnection;
    });
    when(mockService.isLoginException(any(SQLException.class), any())).thenReturn(true);

    // Run connect in a separate thread since forceFetchSecret will block
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Connection> connectFuture = executor.submit(() ->
          slowPlugin.connect(TEST_PROTOCOL, TEST_HOSTSPEC,
              new Properties() {{ putAll(testProps); }}, true, connectFunc));

      // Wait for the SM client to be called by the async refresh
      assertTrue(fetchStarted.await(5, TimeUnit.SECONDS), "Fetch should have started");
      // Wait for the login failure so the calling thread enters the retry/force-fetch path
      assertTrue(loginFailureOccurred.await(5, TimeUnit.SECONDS), "Login failure should have occurred");
      // Give the calling thread time to reach forceFetchSecret → .get() before releasing the latch.
      // The path from login failure to .get() is: isLoginException check → resolveSecret(true) →
      // forceFetchSecret → triggerAsyncRefreshIfNeeded → compute() → .get(). This is microseconds.
      Thread.sleep(100);

      // Now the calling thread should be blocking on .get() of the in-flight future.
      // Release the slow client to complete the fetch.
      fetchCanProceed.countDown();

      Connection result = connectFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(result);

      // Only ONE fetch should have occurred — the force-fetch joined the in-flight async refresh
      assertEquals(1, fetchCount.get(),
          "Force-fetch should join the in-flight async refresh, not start a new one");
      // connectFunc called twice: once with stale creds (login failure), once with fresh creds
      assertEquals(2, connectCallCount.get());
    } finally {
      executor.shutdown();
    }
  }

  private void waitForPendingRefreshes() {
    for (CompletableFuture<Secret> future : AwsSecretsManagerConnectionPlugin2.pendingRefreshes.values()) {
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
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
