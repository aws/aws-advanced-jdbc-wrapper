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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CacheSupplierTest {

  @Test
  void testMemoizeWithExpiration_ValidParameters() {
    Supplier<String> delegate = () -> "test-value";

    Supplier<String> cached = CachedSupplier.memoizeWithExpiration(delegate, 1, TimeUnit.SECONDS);

    assertNotNull(cached);
    assertEquals("test-value", cached.get());
  }

  @Test
  void testMemoizeWithExpiration_NullDelegate() {
    assertThrows(NullPointerException.class, () ->
        CachedSupplier.memoizeWithExpiration(null, 1, TimeUnit.SECONDS));
  }

  @Test
  void testMemoizeWithExpiration_NullTimeUnit() {
    Supplier<String> delegate = () -> "test";

    assertThrows(NullPointerException.class, () ->
        CachedSupplier.memoizeWithExpiration(delegate, 1, null));
  }

  @Test
  void testMemoizeWithExpiration_ZeroDuration() {
    Supplier<String> delegate = () -> "test";

    assertThrows(IllegalArgumentException.class, () ->
        CachedSupplier.memoizeWithExpiration(delegate, 0, TimeUnit.SECONDS));
  }

  @Test
  void testMemoizeWithExpiration_NegativeDuration() {
    Supplier<String> delegate = () -> "test";

    assertThrows(IllegalArgumentException.class, () ->
        CachedSupplier.memoizeWithExpiration(delegate, -1, TimeUnit.SECONDS));
  }

  @Test
  void testCaching_DelegateCalledOnce() {
    Supplier<String> mockDelegate = mock(Supplier.class);
    when(mockDelegate.get()).thenReturn("cached-value");

    Supplier<String> cached = CachedSupplier.memoizeWithExpiration(mockDelegate, 1, TimeUnit.SECONDS);

    // Call multiple times quickly
    assertEquals("cached-value", cached.get());
    assertEquals("cached-value", cached.get());
    assertEquals("cached-value", cached.get());

    // Delegate should only be called once due to caching
    verify(mockDelegate, times(1)).get();
  }

  @Test
  void testExpiration_DelegateCalledAgainAfterExpiry() throws InterruptedException {
    Supplier<String> mockDelegate = mock(Supplier.class);
    when(mockDelegate.get()).thenReturn("value1", "value2");

    Supplier<String> cached = CachedSupplier.memoizeWithExpiration(mockDelegate, 50, TimeUnit.MILLISECONDS);

    // First call
    assertEquals("value1", cached.get());
    verify(mockDelegate, times(1)).get();

    // Wait for expiration
    Thread.sleep(100);

    // Second call after expiration
    assertEquals("value2", cached.get());
    verify(mockDelegate, times(2)).get();
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    Supplier<String> mockDelegate = mock(Supplier.class);
    when(mockDelegate.get()).thenReturn("concurrent-value");

    Supplier<String> cached = CachedSupplier.memoizeWithExpiration(mockDelegate, 5, TimeUnit.SECONDS);

    // Simulate concurrent access
    Thread[] threads = new Thread[10];
    String[] results = new String[10];

    for (int i = 0; i < 10; i++) {
      final int index = i;
      threads[i] = new Thread(() -> results[index] = cached.get());
      threads[i].start();
    }

    // Wait for all threads
    for (Thread thread : threads) {
      thread.join();
    }

    // All should get the same cached value
    for (String result : results) {
      assertEquals("concurrent-value", result);
    }

    // Delegate should only be called once despite concurrent access
    verify(mockDelegate, times(1)).get();
  }

  @Test
  void testExpirationNanos_EdgeCase() {
    Supplier<Long> timeSupplier = () -> System.nanoTime();

    Supplier<Long> cached = CachedSupplier.memoizeWithExpiration(timeSupplier, 1, TimeUnit.NANOSECONDS);

    Long first = cached.get();
    Long second = cached.get();

    // Due to very short expiration, second call might get different value
    assertNotNull(first);
    assertNotNull(second);
  }

  @Test
  void testPrivateConstructor() {
    // Verify utility class has private constructor
    assertThrows(Exception.class, () -> {
      java.lang.reflect.Constructor<CachedSupplier> constructor =
          CachedSupplier.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      constructor.newInstance();
    });
  }
}
