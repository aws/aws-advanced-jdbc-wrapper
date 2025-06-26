package software.amazon.jdbc.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
import java.util.Properties;

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
}
