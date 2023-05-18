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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CacheMapTest {

  @Mock CacheMap.ItemDisposalFunc<String> mockDisposalFunc;
  @Mock CacheMap.IsItemValidFunc<String> mockIsItemValidFunc;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testGet() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.put(1, "a", timeoutNanos);

    assertEquals("a", map.get(1));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertNull(map.get(1));
    assertEquals(0, map.getCache().size());
    verify(mockIsItemValidFunc, times(1)).isValid(any());
    verify(mockDisposalFunc, times(1)).dispose(any());
  }

  @Test
  public void testGetWithDefault() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.put(1, "a", timeoutNanos);

    assertEquals("a", map.get(1, "b", timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertEquals("b", map.get(1, "b", timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertNull(map.get(1));
    assertEquals(0, map.getCache().size());
    verify(mockIsItemValidFunc, times(1)).isValid(eq("a"));
    verify(mockIsItemValidFunc, times(1)).isValid(eq("b"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("b"));
  }

  @Test
  public void testGetWithExtendExpiration() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.put(1, "a", TimeUnit.SECONDS.toNanos(timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos / 2);

    map.getWithExtendExpiration(1, timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos / 2);

    assertEquals("a", map.get(1));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos / 2);
    assertNull(map.getWithExtendExpiration(1, timeoutNanos));
    verify(mockIsItemValidFunc, times(1)).isValid(any());
    verify(mockDisposalFunc, times(1)).dispose(any());
  }

  @Test
  public void testPut() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);

    map.put(1, "a", timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.put(2, "b", timeoutNanos);
    map.put(2, "c", timeoutNanos);

    assertNull(map.get(1));
    assertEquals("c", map.get(2));
    verify(mockIsItemValidFunc, times(1)).isValid(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("b"));
  }

  @Test
  public void testPutIfAbsent() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);

    map.putIfAbsent(1, "a", timeoutNanos);
    map.putIfAbsent(1, "b", timeoutNanos);

    assertEquals("a", map.get(1));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.putIfAbsent(1, "b", timeoutNanos);
    assertEquals("b", map.get(1));
    verify(mockIsItemValidFunc, times(1)).isValid(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
  }

  @Test
  public void testComputeIfAbsent() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);

    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);
    assertEquals("a", map.get(1));
    map.computeIfAbsent(1, (key) -> "b", timeoutNanos);
    assertEquals("a", map.get(1));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.computeIfAbsent(1, (key) -> "b", timeoutNanos);
    assertEquals("b", map.get(1));
    verify(mockIsItemValidFunc, times(1)).isValid(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
  }

  @Test
  public void testRemove() {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.put(1, "a", timeoutNanos);

    map.remove(1);
    assertEquals(0, map.size());
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
  }

  @Test
  public void testClear() {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.put(1, "a", timeoutNanos);
    map.put(2, "b", timeoutNanos);

    map.clear();
    assertEquals(0, map.size());
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("b"));
  }

  @Test
  public void testGetEntries() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    Map<Integer, String> expectedEntries = new HashMap<>();
    expectedEntries.put(1, "a");
    expectedEntries.put(2, "b");
    map.put(1, "a", timeoutNanos);
    map.put(2, "b", timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);

    Map<Integer, String> entries = map.getEntries();
    assertEquals(expectedEntries, entries);
  }

  @Test
  public void testCleanup() throws InterruptedException {
    final CacheMap<Integer, String> map = new CacheMap<>(mockIsItemValidFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.setCleanupIntervalNanos(timeoutNanos * 2);
    map.put(1, "a", timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.put(2, "b", timeoutNanos);

    assertTrue(map.getCache().containsKey(1));
    assertTrue(map.getCache().get(1).isExpired());
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.put(2, "b", timeoutNanos);
    verify(mockIsItemValidFunc, times(2)).isValid(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    assertNull(map.get(1));
  }
}
