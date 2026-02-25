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

package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.function.BiConsumer;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.plugin.iam.ElastiCacheIamTokenUtility;

public class CacheConnectionTest {
  @Mock
  GenericObjectPool<StatefulConnection<byte[], byte[]>> mockReadConnPool;
  @Mock
  GenericObjectPool<StatefulConnection<byte[], byte[]>> mockWriteConnPool;
  @Mock
  StatefulRedisConnection<byte[], byte[]> mockConnection;
  @Mock
  RedisCommands<byte[], byte[]> mockSyncCommands;
  @Mock
  RedisAsyncCommands<byte[], byte[]> mockAsyncCommands;
  @Mock
  StatefulRedisClusterConnection<byte[], byte[]> mockClusterConnection;
  @Mock
  RedisAdvancedClusterCommands<byte[], byte[]> mockClusterSyncCommands;
  @Mock
  RedisAdvancedClusterAsyncCommands<byte[], byte[]> mockClusterAsyncCommands;
  @Mock
  RedisFuture<String> mockCacheResult;
  private AutoCloseable closeable;
  private CacheConnection cacheConnection;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheEndpointAddrRo", "localhost:6380");
    cacheConnection = new CacheConnection(props);
    cacheConnection.setConnectionPools(mockReadConnPool, mockWriteConnPool);
    // Bypass cluster detection for tests to avoid real Redis connections
    cacheConnection.setClusterMode(false);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testIamAuth_PropertyExtraction() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-west-2");
    props.setProperty("cacheUsername", "myuser");
    props.setProperty("cacheName", "my-cache");

    CacheConnection connection = new CacheConnection(props);

    // Verify all IAM fields are set correctly
    assertEquals("us-west-2", getField(connection, "cacheIamRegion"));
    assertEquals("myuser", getField(connection, "cacheUsername"));
    assertEquals("my-cache", getField(connection, "cacheName"));
  }

  @Test
  void testIamAuth_PropertyExtractionTraditional() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheUsername", "myuser");
    props.setProperty("cachePassword", "password");
    props.setProperty("cacheName", "my-cache");

    CacheConnection connection = new CacheConnection(props);

    // Verify all IAM fields are set correctly
    assertEquals("myuser", getField(connection, "cacheUsername"));
    assertEquals("my-cache", getField(connection, "cacheName"));
    assertEquals("password", getField(connection, "cachePassword"));
  }

  @Test
  void testIamAuthEnabled_WhenRegionProvided() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-east-1");
    props.setProperty("cacheUsername", "testuser");
    props.setProperty("cacheName", "my-cache");

    CacheConnection connection = new CacheConnection(props);

    // Use reflection to verify iamAuthEnabled is true
    Field field = CacheConnection.class.getDeclaredField("iamAuthEnabled");
    field.setAccessible(true);
    assertTrue((boolean) field.get(connection));
    // Verify all IAM fields are set correctly
    assertEquals("us-east-1", getField(connection, "cacheIamRegion"));
    assertEquals("testuser", getField(connection, "cacheUsername"));
    assertEquals("my-cache", getField(connection, "cacheName"));
  }

  @Test
  void testConstructor_IamAuthEnabled_MissingCacheName() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-west-2");
    props.setProperty("cacheUsername", "myuser");
    // Missing cacheName property

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new CacheConnection(props)
    );

    assertTrue(exception.getMessage().contains("IAM authentication requires cache name, username, region, and "
        + "hostname"));
  }

  @Test
  void testTraditionalAuth_WhenNoIamRegion() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheUsername", "user");
    props.setProperty("cachePassword", "pass");

    CacheConnection connection = new CacheConnection(props);

    assertFalse((boolean) getField(connection, "iamAuthEnabled"));
    assertNull(getField(connection, "credentialsProvider"));
    assertEquals("user", getField(connection, "cacheUsername"));
    assertEquals("pass", getField(connection, "cachePassword"));
  }

  @Test
  void testConstructor_NoRwAddress() {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRo", "localhost:6379");

    assertThrows(IllegalArgumentException.class, () -> new CacheConnection(props));
  }

  @Test
  void testConstructor_IamAuthEnabled_MissingCacheUsername() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-east-1");

    assertThrows(IllegalArgumentException.class, () -> new CacheConnection(props));
  }

  @Test
  void testConstructor_ConflictingAuthenticationMethods() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-west-2");  // IAM auth
    props.setProperty("cacheUsername", "myuser");
    props.setProperty("cachePassword", "mypassword");  // Traditional auth
    props.setProperty("cacheName", "my-cache");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new CacheConnection(props)
    );

    assertTrue(exception.getMessage().contains("Cannot specify both IAM authentication"));
  }

  @Test
  void testAwsCredentialsProvider_WithProfile() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-east-1");
    props.setProperty("cacheUsername", "testuser");
    props.setProperty("cacheName", "my-cache");
    props.setProperty("awsProfile", "test-profile");

    CacheConnection connection = new CacheConnection(props);

    // Verify the awsProfileProperties field contains the correct profile
    Properties awsProfileProps = (Properties) getField(connection, "awsProfileProperties");
    assertEquals("test-profile", awsProfileProps.getProperty("awsProfile"));

    assertEquals("my-cache", getField(connection, "cacheName"));
    assertEquals("testuser", getField(connection, "cacheUsername"));
    assertEquals("us-east-1", getField(connection, "cacheIamRegion"));
    assertEquals("localhost:6379", getField(connection, "cacheRwServerAddr"));
  }

  @Test
  void testAwsCredentialsProvider_WithoutProfile() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-east-1");
    props.setProperty("cacheUsername", "testuser");
    props.setProperty("cacheName", "my-cache");
    // No awsProfile property

    CacheConnection connection = new CacheConnection(props);

    // Verify awsProfileProperties is not empty when no profile specified
    Properties awsProfileProps = (Properties) getField(connection, "awsProfileProperties");
    assertNull(awsProfileProps);

    assertEquals("my-cache", getField(connection, "cacheName"));
    assertEquals("testuser", getField(connection, "cacheUsername"));
    assertEquals("us-east-1", getField(connection, "cacheIamRegion"));
    assertEquals("localhost:6379", getField(connection, "cacheRwServerAddr"));
  }

  @Test
  void testBuildRedisURI_IamAuth() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheIamRegion", "us-east-1");
    props.setProperty("cacheUsername", "testuser");
    props.setProperty("cacheName", "test-cache");

    try (MockedConstruction<ElastiCacheIamTokenUtility> mockedTokenUtility =
             mockConstruction(ElastiCacheIamTokenUtility.class)) {

      CacheConnection connection = new CacheConnection(props);
      RedisURI uri = connection.buildRedisURI("localhost", 6379);

      // Verify URI properties
      assertNotNull(uri);
      assertEquals("localhost", uri.getHost());
      assertEquals(6379, uri.getPort());
      assertTrue(uri.isSsl());
      assertNotNull(uri.getCredentialsProvider());

      // Trigger the credentials provider to create the token utility
      uri.getCredentialsProvider().resolveCredentials().block();

      // Verify URI properties
      assertNotNull(uri);
      assertEquals("localhost", uri.getHost());
      assertEquals(6379, uri.getPort());
      assertTrue(uri.isSsl());
      assertNotNull(uri.getCredentialsProvider()); // IAM credentials provider set

      // Verify ElastiCacheIamTokenUtility was constructed with correct parameters
      // Verify token utility construction
      assertEquals(1, mockedTokenUtility.constructed().size());
      ElastiCacheIamTokenUtility tokenUtility = mockedTokenUtility.constructed().get(0);
      verify(tokenUtility).generateAuthenticationToken(
          any(AwsCredentialsProvider.class),
          eq(Region.US_EAST_1),
          eq("localhost"),
          eq(6379),
          eq("testuser")
      );
    }
  }

  @Test
  void testBuildRedisURI_TraditionalAuth() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheUsername", "user");
    props.setProperty("cachePassword", "pass");

    CacheConnection connection = new CacheConnection(props);
    RedisURI uri = connection.buildRedisURI("localhost", 6379);

    assertNotNull(uri);
    assertEquals("localhost", uri.getHost());
    assertEquals(6379, uri.getPort());
    assertEquals("user", uri.getUsername());
    assertEquals("pass", new String(uri.getPassword()));
  }

  @Test
  void testBuildRedisURI_NoAuth() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");

    CacheConnection connection = new CacheConnection(props);
    RedisURI uri = connection.buildRedisURI("localhost", 6379);

    assertNotNull(uri);
    assertEquals("localhost", uri.getHost());
    assertEquals(6379, uri.getPort());
    assertNull(uri.getUsername());
    assertNull(uri.getPassword());
  }

  @Test
  void test_writeToCache() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    final String key = "myQueryKey";
    final byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(), any(), any())).thenReturn(mockCacheResult);
    when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenReturn(null);
    spyConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(mockConnection).async();
    verify(mockAsyncCommands).set(any(), any(), any());
    verify(mockCacheResult).whenComplete(any(BiConsumer.class));
  }

  @Test
  void test_writeToCacheException() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    final String key = "myQueryKey";
    final byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(), any(), any())).thenThrow(new RuntimeException("test exception"));
    spyConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(mockConnection).async();
    verify(mockAsyncCommands).set(any(), any(), any());
    verify(mockWriteConnPool).invalidateObject(mockConnection);
  }

  @Test
  void testHandleCompletedCacheWrite() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());

    // Success: decrement called, no error reported, connection returned
    spyConnection.handleCompletedCacheWrite(mockConnection, 150L, null);
    verify(spyConnection).decrementInFlightSize(150L);
    verify(spyConnection, never()).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    verify(mockWriteConnPool).returnObject(mockConnection);

    // Failure: decrement called, error reported, connection invalidated
    RuntimeException writeError = new RuntimeException("Redis timeout");
    spyConnection.handleCompletedCacheWrite(mockConnection, 200L, writeError);
    verify(spyConnection).decrementInFlightSize(200L);
    verify(spyConnection).reportErrorToCacheMonitor(eq(true), eq(writeError), eq("WRITE"));
    verify(mockWriteConnPool).invalidateObject(mockConnection);

    // Multiple operations: mixed success/failure
    spyConnection.handleCompletedCacheWrite(mockConnection, 100L, null);
    spyConnection.handleCompletedCacheWrite(mockConnection, 250L, new RuntimeException("lost"));
    verify(spyConnection).decrementInFlightSize(100L);
    verify(spyConnection).decrementInFlightSize(250L);
    verify(mockWriteConnPool, times(2)).returnObject(mockConnection);
    verify(mockWriteConnPool, times(2)).invalidateObject(mockConnection);
  }

  @Test
  void test_readFromCache() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());

    byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    when(mockSyncCommands.get(any())).thenReturn(value);
    byte[] result = spyConnection.readFromCache("myQueryKey");
    assertEquals(value, result);
    verify(mockReadConnPool).borrowObject();
    verify(mockConnection).sync();
    verify(mockSyncCommands).get(any());
    verify(mockReadConnPool).returnObject(mockConnection);
  }

  @Test
  void test_readFromCacheException() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());

    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    when(mockSyncCommands.get(any())).thenThrow(new RuntimeException("test"));
    assertNull(spyConnection.readFromCache("myQueryKey"));
    verify(mockReadConnPool).borrowObject();
    verify(mockConnection).sync();
    verify(mockSyncCommands).get(any());
    verify(mockReadConnPool).invalidateObject(mockConnection);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true}) // false = CMD (standalone), true = CME (cluster)
  void test_readAndWriteCache_BothModes(boolean isClusterMode) throws Exception {
    // Setup connection with appropriate cluster mode
    CacheConnection spyConnection = spy(cacheConnection);
    spyConnection.setClusterMode(isClusterMode);

    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    String key = "testKey";
    byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

    // Test WRITE operation
    if (isClusterMode) {
      // Mock cluster connection for write
      when(mockWriteConnPool.borrowObject()).thenReturn(mockClusterConnection);
      when(mockClusterConnection.async()).thenReturn(mockClusterAsyncCommands);
      when(mockClusterAsyncCommands.set(any(), any(), any())).thenReturn(mockCacheResult);
      when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenReturn(null);
    } else {
      // Mock standalone connection for write
      when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
      when(mockConnection.async()).thenReturn(mockAsyncCommands);
      when(mockAsyncCommands.set(any(), any(), any())).thenReturn(mockCacheResult);
      when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenReturn(null);
    }

    spyConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(spyConnection).incrementInFlightSize(anyLong());

    // Test READ operation
    if (isClusterMode) {
      // Mock cluster connection for read
      when(mockReadConnPool.borrowObject()).thenReturn(mockClusterConnection);
      when(mockClusterConnection.sync()).thenReturn(mockClusterSyncCommands);
      when(mockClusterSyncCommands.get(any())).thenReturn(value);
    } else {
      // Mock standalone connection for read
      when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
      when(mockConnection.sync()).thenReturn(mockSyncCommands);
      when(mockSyncCommands.get(any())).thenReturn(value);
    }

    byte[] result = spyConnection.readFromCache(key);
    assertEquals(value, result);
    verify(mockReadConnPool).borrowObject();

    // Verify appropriate connection type was used
    if (isClusterMode) {
      verify(mockClusterConnection).async();
      verify(mockClusterConnection).sync();
      verify(mockClusterAsyncCommands).set(any(), any(), any());
      verify(mockClusterSyncCommands).get(any());
    } else {
      verify(mockConnection).async();
      verify(mockConnection).sync();
      verify(mockAsyncCommands).set(any(), any(), any());
      verify(mockSyncCommands).get(any());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void test_readAndWriteException_BothModes(boolean isClusterMode) throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    spyConnection.setClusterMode(isClusterMode);

    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    String key = "testKey";
    byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

    // Test WRITE exception handling
    if (isClusterMode) {
      when(mockWriteConnPool.borrowObject()).thenReturn(mockClusterConnection);
      when(mockClusterConnection.async()).thenReturn(mockClusterAsyncCommands);
      when(mockClusterAsyncCommands.set(any(), any(), any())).thenThrow(new RuntimeException("cluster write error"));
    } else {
      when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
      when(mockConnection.async()).thenReturn(mockAsyncCommands);
      when(mockAsyncCommands.set(any(), any(), any())).thenThrow(new RuntimeException("standalone write error"));
    }

    spyConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(mockWriteConnPool).invalidateObject(isClusterMode ? mockClusterConnection : mockConnection);

    // Test READ exception handling
    if (isClusterMode) {
      when(mockReadConnPool.borrowObject()).thenReturn(mockClusterConnection);
      when(mockClusterConnection.sync()).thenReturn(mockClusterSyncCommands);
      when(mockClusterSyncCommands.get(any())).thenThrow(new RuntimeException("cluster read error"));
    } else {
      when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
      when(mockConnection.sync()).thenReturn(mockSyncCommands);
      when(mockSyncCommands.get(any())).thenThrow(new RuntimeException("standalone read error"));
    }

    assertNull(spyConnection.readFromCache(key));
    verify(mockReadConnPool).borrowObject();
    verify(mockReadConnPool).invalidateObject(isClusterMode ? mockClusterConnection : mockConnection);
  }

  @Test
  void test_cacheConnectionPoolSize_default() throws Exception {
    clearStaticRegistry();

    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheEndpointAddrRo", "localhost:6380");

    CacheConnection connection = new CacheConnection(props);
    // Bypass cluster detection for tests
    connection.setClusterMode(false);

    // Create real pools (no network until borrow)
    connection.triggerPoolInit(true);
    connection.triggerPoolInit(false);

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool = getInstancePool(connection,
        "readConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool = getInstancePool(connection,
        "writeConnectionPool");

    assertNotNull(readPool, "read pool should be created");
    assertNotNull(writePool, "write pool should be created");

    assertEquals(20, readPool.getMaxTotal());
    assertEquals(20, readPool.getMaxIdle());
    assertEquals(20, writePool.getMaxTotal());
    assertEquals(20, writePool.getMaxIdle());
    assertNotEquals(8, readPool.getMaxTotal()); // making sure it does not set the default values of Generic pool
    assertNotEquals(8, writePool.getMaxIdle());
  }

  @Test
  void test_cacheConnectionPoolSize_Initialization() throws Exception {
    clearStaticRegistry();

    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheEndpointAddrRo", "localhost:6380");
    props.setProperty("cacheConnectionPoolSize", "15");

    CacheConnection connection = new CacheConnection(props);
    // Bypass cluster detection for tests
    connection.setClusterMode(false);

    // Create real pools (no network until borrow)
    connection.triggerPoolInit(true);
    connection.triggerPoolInit(false);

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool = getInstancePool(connection,
        "readConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool = getInstancePool(connection,
        "writeConnectionPool");

    assertNotNull(readPool, "read pool should be created");
    assertNotNull(writePool, "write pool should be created");

    assertEquals(15, readPool.getMaxTotal());
    assertEquals(15, readPool.getMaxIdle());
    assertEquals(15, writePool.getMaxTotal());
    assertEquals(15, writePool.getMaxIdle());
  }

  @Test
  void test_cacheConnectionTimeout_Initialization() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheConnectionTimeoutMs", "5000");

    CacheConnection connection = new CacheConnection(props);
    Duration timeout = (Duration) getField(connection, "cacheConnectionTimeout");
    assertEquals(Duration.ofMillis(5000), timeout);
  }

  @Test
  void test_cacheConnectionTimeout_default() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");

    CacheConnection connection = new CacheConnection(props);
    Duration timeout = (Duration) getField(connection, "cacheConnectionTimeout");
    assertEquals(Duration.ofMillis(2000), timeout, "default should be 2000 ms");
  }

  @SuppressWarnings("unchecked")
  private static GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> getInstancePool(CacheConnection connection,
      String fieldName) throws Exception {
    Field f = CacheConnection.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    return (GenericObjectPool<StatefulRedisConnection<byte[], byte[]>>) f.get(connection);
  }

  private static void clearStaticRegistry() throws Exception {
    Field registryField = CacheConnection.class.getDeclaredField("endpointToPoolRegistry");
    registryField.setAccessible(true);
    java.util.concurrent.ConcurrentHashMap<?, ?> registry =
        (java.util.concurrent.ConcurrentHashMap<?, ?>) registryField.get(null);
    registry.clear();
  }

  @Test
  void test_cacheMonitorIntegration() throws Exception {
    CacheConnection spyConnection = spy(cacheConnection);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    String key = "myQueryKey";
    byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);

    // DEGRADED state: operations bypassed
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.DEGRADED);
    spyConnection.writeToCache(key, value, 100);
    assertNull(spyConnection.readFromCache(key));
    verify(mockWriteConnPool, never()).borrowObject();
    verify(mockReadConnPool, never()).borrowObject();

    // HEALTHY state: operations proceed
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(), any(), any())).thenReturn(mockCacheResult);
    when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenReturn(null);
    spyConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(spyConnection).incrementInFlightSize(anyLong());

    // Error reporting on read failure
    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    RuntimeException testException = new RuntimeException("Connection failed");
    when(mockSyncCommands.get(any())).thenThrow(testException);
    assertNull(spyConnection.readFromCache(key));
    verify(spyConnection).reportErrorToCacheMonitor(eq(false), eq(testException), eq("READ"));

    // failWhenCacheDown: throws SQLException
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("failWhenCacheDown", "true");
    CacheConnection failConnection = spy(new CacheConnection(props));
    failConnection.setConnectionPools(mockReadConnPool, mockWriteConnPool);
    when(failConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.DEGRADED);
    SQLException exception = assertThrows(SQLException.class, () -> failConnection.readFromCache(key));
    assertTrue(exception.getMessage().contains("Cache cluster is in DEGRADED state"));
  }

  @Test
  void test_multiEndpoint_PoolReuseAndIsolation() throws Exception {
    clearStaticRegistry();

    // Test 1: Same endpoints should reuse pools (first wins)
    Properties props1 = new Properties();
    props1.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props1.setProperty("cacheEndpointAddrRo", "localhost:6380");
    props1.setProperty("cacheConnectionPoolSize", "10");

    Properties props2 = new Properties();
    props2.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props2.setProperty("cacheEndpointAddrRo", "localhost:6380");
    props2.setProperty("cacheConnectionPoolSize", "15");

    CacheConnection connection1 = new CacheConnection(props1);
    connection1.setClusterMode(false);
    CacheConnection connection2 = new CacheConnection(props2);
    connection2.setClusterMode(false);

    connection1.triggerPoolInit(true);
    connection1.triggerPoolInit(false);
    connection2.triggerPoolInit(true);
    connection2.triggerPoolInit(false);

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool1 = getInstancePool(connection1,
        "readConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool2 = getInstancePool(connection2,
        "readConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool1 = getInstancePool(connection1,
        "writeConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool2 = getInstancePool(connection2,
        "writeConnectionPool");

    assertSame(readPool1, readPool2, "Read pools should be the same instance");
    assertSame(writePool1, writePool2, "Write pools should be the same instance");
    assertEquals(10, readPool1.getMaxTotal(), "Pool size should be 10 (first initialized)");
    assertEquals(10, writePool1.getMaxTotal(), "Pool size should be 10 (first initialized)");

    // Test 2: Different endpoints should have isolated pools
    Properties props3 = new Properties();
    props3.setProperty("cacheEndpointAddrRw", "localhost:7379");
    props3.setProperty("cacheEndpointAddrRo", "localhost:7380");
    props3.setProperty("cacheConnectionPoolSize", "20");

    CacheConnection connection3 = new CacheConnection(props3);
    connection3.setClusterMode(false);
    connection3.triggerPoolInit(true);
    connection3.triggerPoolInit(false);

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool3 = getInstancePool(connection3,
        "readConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool3 = getInstancePool(connection3,
        "writeConnectionPool");

    assertNotSame(readPool1, readPool3, "Read pools should be different instances");
    assertNotSame(writePool1, writePool3, "Write pools should be different instances");
    assertEquals(20, readPool3.getMaxTotal());
    assertEquals(20, writePool3.getMaxTotal());

    // Test 3: Same RW endpoint, different RO endpoints (or no RO)
    Properties props4 = new Properties();
    props4.setProperty("cacheEndpointAddrRw", "localhost:6379");

    CacheConnection connection4 = new CacheConnection(props4);
    connection4.setClusterMode(false);
    connection4.triggerPoolInit(false);
    connection4.triggerPoolInit(true);

    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> writePool4 = getInstancePool(connection4,
        "writeConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> readPool4 = getInstancePool(connection4,
        "readConnectionPool");

    assertSame(writePool1, writePool4, "Write pools should be shared for same RW endpoint");
    assertEquals(10, writePool4.getMaxTotal(), "Connection pool size should not be changed.");
    assertNotSame(readPool1, readPool4, "Read pools should be different for different RO endpoints");
  }

  @Test
  void test_multiEndpoint_ConcurrentInitialization() throws Exception {
    clearStaticRegistry();

    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheConnectionPoolSize", "10");

    CacheConnection connection1 = new CacheConnection(props);
    connection1.setClusterMode(false);
    CacheConnection connection2 = new CacheConnection(props);
    connection2.setClusterMode(false);
    CacheConnection connection3 = new CacheConnection(props);
    connection3.setClusterMode(false);

    // Simulate concurrent initialization
    Thread t1 = new Thread(() -> connection1.triggerPoolInit(false));
    Thread t2 = new Thread(() -> connection2.triggerPoolInit(false));
    Thread t3 = new Thread(() -> connection3.triggerPoolInit(false));

    t1.start();
    t2.start();
    t3.start();

    t1.join();
    t2.join();
    t3.join();

    // All should reference the same pool
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool1 = getInstancePool(connection1,
        "writeConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool2 = getInstancePool(connection2,
        "writeConnectionPool");
    GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> pool3 = getInstancePool(connection3,
        "writeConnectionPool");

    assertSame(pool1, pool2);
    assertSame(pool2, pool3);
    assertEquals(10, pool1.getMaxTotal());
  }

  @Test
  void test_computeCacheKey() throws Exception {
    // With prefix
    Properties propsWithPrefix = createBaseProperties();
    propsWithPrefix.setProperty("cacheKeyPrefix", "app1");
    CacheConnection connWithPrefix = new CacheConnection(propsWithPrefix);
    connWithPrefix.setConnectionPools(mockReadConnPool, mockWriteConnPool);
    connWithPrefix.setClusterMode(false);
    connWithPrefix.triggerPoolInit(true);

    Method method = CacheConnection.class.getDeclaredMethod("computeCacheKey", String.class);
    method.setAccessible(true);

    byte[] resultWithPrefix = (byte[]) method.invoke(connWithPrefix, "test_key");
    assertEquals(52, resultWithPrefix.length); // 4 (prefix) + 48 (SHA-384 hash)
    assertEquals("app1", new String(resultWithPrefix, 0, 4, StandardCharsets.UTF_8));

    // Without prefix
    Properties propsNoPrefix = createBaseProperties();
    CacheConnection connNoPrefix = new CacheConnection(propsNoPrefix);
    connNoPrefix.setConnectionPools(mockReadConnPool, mockWriteConnPool);
    connNoPrefix.setClusterMode(false);
    connNoPrefix.triggerPoolInit(true);

    byte[] resultNoPrefix = (byte[]) method.invoke(connNoPrefix, "test_key");
    assertEquals(48, resultNoPrefix.length); // Just the SHA-384 hash
  }

  @Test
  void test_cacheKeyPrefix_readWriteOperations() throws Exception {
    Properties props = createBaseProperties();
    // prefix exceeds 10 characters should throw IllegalArgumentException
    props.setProperty("cacheKeyPrefix", "thisistoolong"); // 14 chars
    IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class,
        () -> new CacheConnection(props));
    assertTrue(ex1.getMessage().contains("Cache key prefix must be 10 characters or less"));

    // empty or whitespace string should throw IllegalArgumentException
    props.setProperty("cacheKeyPrefix", "");
    IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
        () -> new CacheConnection(props));
    assertTrue(ex2.getMessage().contains("Cache key prefix cannot be empty or whitespace. Use null for no prefix."));

    props.setProperty("cacheKeyPrefix", " ");
    IllegalArgumentException ex3 = assertThrows(IllegalArgumentException.class,
        () -> new CacheConnection(props));
    assertTrue(ex3.getMessage().contains("Cache key prefix cannot be empty or whitespace. Use null for no prefix."));

    String prefix = "app1";
    props.setProperty("cacheKeyPrefix", prefix);

    CacheConnection connection = new CacheConnection(props);
    connection.setConnectionPools(mockReadConnPool, mockWriteConnPool);
    connection.setClusterMode(false);

    CacheConnection spyConnection = spy(connection);
    when(spyConnection.getClusterHealthStateFromCacheMonitor()).thenReturn(CacheMonitor.HealthState.HEALTHY);
    doNothing().when(spyConnection).reportErrorToCacheMonitor(anyBoolean(), any(), any());
    doNothing().when(spyConnection).incrementInFlightSize(anyLong());
    doNothing().when(spyConnection).decrementInFlightSize(anyLong());

    final String testKey = "test_key";
    final byte[] testValue = "test_value".getBytes(StandardCharsets.UTF_8);

    // write operation with size calculation
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(byte[].class), any(byte[].class), any()))
        .thenReturn(mockCacheResult);
    when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenAnswer(invocation -> {
      BiConsumer<String, Throwable> callback = invocation.getArgument(0);
      callback.accept("OK", null);
      return null;
    });

    spyConnection.writeToCache(testKey, testValue, 300);
    Thread.sleep(100); // Wait for async completion

    // set was called with prefixed key
    verify(mockAsyncCommands).set(
        argThat(key -> {
          if (key.length < 4) {
            return false;
          }
          String keyStr = new String(key, 0, 4, StandardCharsets.UTF_8);
          return keyStr.equals(prefix);
        }),
        eq(testValue),
        any()
    );

    // write size calculation includes prefix (prefix=4 + hash=48 + value=10 = 62)
    long expectedSize = prefix.length() + 48 + testValue.length;
    verify(spyConnection).incrementInFlightSize(expectedSize);
    verify(spyConnection).decrementInFlightSize(expectedSize);
    verify(mockWriteConnPool).returnObject(mockConnection);

    // read operation
    byte[] expectedValue = "cached_data".getBytes(StandardCharsets.UTF_8);
    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    when(mockSyncCommands.get(any(byte[].class))).thenReturn(expectedValue);

    byte[] result = spyConnection.readFromCache(testKey);

    assertArrayEquals(expectedValue, result);

    // get was called with prefixed key
    verify(mockSyncCommands).get(argThat(key -> {
      if (key.length < 4) {
        return false;
      }
      String keyStr = new String(key, 0, 4, StandardCharsets.UTF_8);
      return keyStr.equals(prefix);
    }));

    verify(mockReadConnPool).returnObject(mockConnection);
  }

  private Properties createBaseProperties() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "localhost:6379");
    props.setProperty("cacheUseSSL", "false");
    props.setProperty("wrapperLogUnclosedConnections", "true");
    props.setProperty("cacheConnectionTimeout", "2000");
    props.setProperty("cacheConnectionPoolSize", "20");
    return props;
  }

  private Object getField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}
