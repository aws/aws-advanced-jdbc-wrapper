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

package software.amazon.jdbc.util.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SlidingExpirationCacheTest {
  @Mock ItemDisposalFunc<String> mockDisposalFunc;
  @Mock ShouldDisposeFunc<String> mockShouldDisposeFunc;
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
  public void testComputeIfAbsent() throws InterruptedException {
    final SlidingExpirationCache<Integer, String> map =
        new SlidingExpirationCache<>(mockShouldDisposeFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.setCleanupIntervalNanos(timeoutNanos * 2);
    when(mockShouldDisposeFunc.shouldDispose(any())).thenReturn(true);

    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);
    assertEquals("a", map.computeIfAbsent(1, (key) -> "b", timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertEquals("a", map.computeIfAbsent(1, (key) -> "b", timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos / 2);
    assertEquals("a", map.computeIfAbsent(1, (key) -> "b", timeoutNanos));
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertEquals("b", map.computeIfAbsent(1, (key) -> "b", timeoutNanos));
    verify(mockShouldDisposeFunc, times(1)).shouldDispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
  }

  @Test
  public void testRemove() {
    final SlidingExpirationCache<Integer, String> map =
        new SlidingExpirationCache<>(mockShouldDisposeFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);

    map.remove(1);
    assertEquals(0, map.size());
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
  }

  @Test
  public void testClear() {
    final SlidingExpirationCache<Integer, String> map =
        new SlidingExpirationCache<>(mockShouldDisposeFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);
    map.computeIfAbsent(2, (key) -> "b", timeoutNanos);

    map.clear();
    assertEquals(0, map.size());
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("b"));
  }

  @Test
  public void testGetEntries() throws InterruptedException {
    final SlidingExpirationCache<Integer, String> map =
        new SlidingExpirationCache<>(mockShouldDisposeFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    Map<Integer, String> expectedEntries = new HashMap<>();
    expectedEntries.put(1, "a");
    expectedEntries.put(2, "b");
    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);
    map.computeIfAbsent(2, (key) -> "b", timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);

    Map<Integer, String> entries = map.getEntries();
    assertEquals(expectedEntries, entries);
  }

  @Test
  public void testCleanup() throws InterruptedException {
    final SlidingExpirationCache<Integer, String> map =
        new SlidingExpirationCache<>(mockShouldDisposeFunc, mockDisposalFunc);
    final long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
    map.setCleanupIntervalNanos(timeoutNanos * 2);
    when(mockShouldDisposeFunc.shouldDispose(any())).thenReturn(true);
    map.computeIfAbsent(1, (key) -> "a", timeoutNanos);
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    map.computeIfAbsent(2, (key) -> "b", timeoutNanos);

    assertTrue(map.getCache().containsKey(1));
    assertTrue(map.getCache().get(1).shouldCleanup());
    TimeUnit.NANOSECONDS.sleep(timeoutNanos);
    assertEquals("c", map.computeIfAbsent(2, (key) -> "c", timeoutNanos));
    verify(mockShouldDisposeFunc, times(2)).shouldDispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("a"));
    verify(mockDisposalFunc, times(1)).dispose(eq("b"));
    assertEquals("d", map.computeIfAbsent(1, (key) -> "d", timeoutNanos));
  }
}
