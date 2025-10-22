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

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.plugin.iam.ElastiCacheIamTokenUtility;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CacheConnectionTest {
  @Mock GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> mockReadConnPool;
  @Mock GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> mockWriteConnPool;
  @Mock StatefulRedisConnection<byte[], byte[]> mockConnection;
  @Mock RedisCommands<byte[], byte[]> mockSyncCommands;
  @Mock RedisAsyncCommands<byte[], byte[]> mockAsyncCommands;
  @Mock RedisFuture<String> mockCacheResult;
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
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testIamAuth_PropertyExtraction() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
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
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
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
    props.setProperty("cacheEndpointAddrRw", "test.cache.amazonaws.com:6379");
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
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
    props.setProperty("cacheIamRegion", "us-west-2");
    props.setProperty("cacheUsername", "myuser");
    // Missing cacheName property

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> new CacheConnection(props)
    );

    assertTrue(exception.getMessage().contains("IAM authentication requires cache name, username, region, and hostname"));
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
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
    props.setProperty("cacheIamRegion", "us-east-1");

    assertThrows(IllegalArgumentException.class, () -> new CacheConnection(props));
  }

  @Test
  void testConstructor_ConflictingAuthenticationMethods() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
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
    props.setProperty("cacheEndpointAddrRw", "test.cache.amazonaws.com:6379");
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
    assertEquals("test.cache.amazonaws.com:6379", getField(connection, "cacheRwServerAddr"));
  }

  @Test
  void testAwsCredentialsProvider_WithoutProfile() throws Exception {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "test.cache.amazonaws.com:6379");
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
    assertEquals("test.cache.amazonaws.com:6379", getField(connection, "cacheRwServerAddr"));
  }

  @Test
  void testBuildRedisURI_IamAuth() {
    Properties props = new Properties();
    props.setProperty("cacheEndpointAddrRw", "test-cache.cache.amazonaws.com:6379");
    props.setProperty("cacheIamRegion", "us-east-1");
    props.setProperty("cacheUsername", "testuser");
    props.setProperty("cacheName", "test-cache");

    try (MockedConstruction<ElastiCacheIamTokenUtility> mockedTokenUtility = mockConstruction(ElastiCacheIamTokenUtility.class)) {

      CacheConnection connection = new CacheConnection(props);
      RedisURI uri = connection.buildRedisURI("test-cache.cache.amazonaws.com", 6379);

      // Verify URI properties
      assertNotNull(uri);
      assertEquals("test-cache.cache.amazonaws.com", uri.getHost());
      assertEquals(6379, uri.getPort());
      assertTrue(uri.isSsl());
      assertNotNull(uri.getCredentialsProvider());

      // Trigger the credentials provider to create the token utility
      uri.getCredentialsProvider().resolveCredentials().block();

      // Verify URI properties
      assertNotNull(uri);
      assertEquals("test-cache.cache.amazonaws.com", uri.getHost());
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
          eq("test-cache.cache.amazonaws.com"),
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
    String key = "myQueryKey";
    byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(), any(), any())).thenReturn(mockCacheResult);
    when(mockCacheResult.whenComplete(any(BiConsumer.class))).thenReturn(null);
    cacheConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(mockConnection).async();
    verify(mockAsyncCommands).set(any(), any(), any());
    verify(mockCacheResult).whenComplete(any(BiConsumer.class));
  }

  @Test
  void test_writeToCacheException() throws Exception {
    String key = "myQueryKey";
    byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockWriteConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.async()).thenReturn(mockAsyncCommands);
    when(mockAsyncCommands.set(any(), any(), any())).thenThrow(new RuntimeException("test exception"));
    cacheConnection.writeToCache(key, value, 100);
    verify(mockWriteConnPool).borrowObject();
    verify(mockConnection).async();
    verify(mockAsyncCommands).set(any(), any(), any());
    verify(mockWriteConnPool).invalidateObject(mockConnection);
  }

  @Test
  void test_handleCompletedCacheWrite() throws Exception {
    cacheConnection.handleCompletedCacheWrite(mockConnection, null);
    verify(mockWriteConnPool).returnObject(mockConnection);
    cacheConnection.handleCompletedCacheWrite(mockConnection, new RuntimeException("test"));
    verify(mockWriteConnPool).invalidateObject(mockConnection);
  }

  @Test
  void test_readFromCache() throws Exception {
    byte[] value = "myValue".getBytes(StandardCharsets.UTF_8);
    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    when(mockSyncCommands.get(any())).thenReturn(value);
    byte[] result = cacheConnection.readFromCache("myQueryKey");
    assertEquals(value, result);
    verify(mockReadConnPool).borrowObject();
    verify(mockConnection).sync();
    verify(mockSyncCommands).get(any());
    verify(mockReadConnPool).returnObject(mockConnection);
  }

  @Test
  void test_readFromCacheException() throws Exception {
    when(mockReadConnPool.borrowObject()).thenReturn(mockConnection);
    when(mockConnection.sync()).thenReturn(mockSyncCommands);
    when(mockSyncCommands.get(any())).thenThrow(new RuntimeException("test"));
    assertNull(cacheConnection.readFromCache("myQueryKey"));
    verify(mockReadConnPool).borrowObject();
    verify(mockConnection).sync();
    verify(mockSyncCommands).get(any());
    verify(mockReadConnPool).invalidateObject(mockConnection);
  }

  private Object getField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}
