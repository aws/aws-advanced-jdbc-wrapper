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

package software.amazon.jdbc.plugin.efm.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.plugin.efm.v1.HostMonitorConnectionContextV1;
import software.amazon.jdbc.plugin.efm.v2.HostMonitorConnectionContextV2;

class ContextPoolImplTest {

  static Stream<Arguments> contextSuppliers() {
    return Stream.of(
        Arguments.of((Supplier<ConnectionContext>) HostMonitorConnectionContextV1::new),
        Arguments.of((Supplier<ConnectionContext>) HostMonitorConnectionContextV2::new)
    );
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_lazyInitializationDisabled(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(3, supplier);
    assertEquals(0, pool.size());
    pool.acquire();
    assertEquals(2, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_lazyInitializationEnabled(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(5, true, supplier);
    assertEquals(0, pool.size());

    pool.acquire();
    assertEquals(2, pool.size());

    pool.acquire();
    assertEquals(3, pool.size());

    pool.acquire();
    assertEquals(4, pool.size());

    pool.acquire();
    assertEquals(5, pool.size());

    // maxIdleCount has been reached.
    pool.acquire();
    assertEquals(4, pool.size());
    // acquire should no longer refill idle context pool.
    pool.acquire();
    assertEquals(3, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseNull_returnsFalse(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, supplier);
    pool.acquire();
    int sizeBeforeRelease = pool.size();

    boolean released = pool.release(null);
    assertFalse(released);
    assertEquals(sizeBeforeRelease, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_contextReuse_diffInstanceReturnedForLazyInitialization(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(1, true, supplier);

    ConnectionContext context1 = pool.acquire();

    // Another idle context should have been created due to the lazy initialization method.
    assertEquals(1, pool.size());

    // maxIdleCount has already been reached. Release should be no-op.
    assertFalse(pool.release(context1));

    ConnectionContext context2 = pool.acquire();
    assertNotSame(context1, context2);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_contextReuse_sameInstanceReturnedForLazyInitialization(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(3, true, supplier);

    ConnectionContext context1 = pool.acquire();

    // 2 more idle contexts should have been created due to the lazy initialization method.
    assertEquals(2, pool.size());

    // Should create 2 more idle contexts to reach maxIdleCount
    pool.acquire();
    assertEquals(3, pool.size());

    // Exhaust the pool.
    pool.acquire();
    pool.acquire();
    pool.acquire();

    // Release original context back to pool.
    assertTrue(pool.release(context1));
    assertEquals(1, pool.size());

    ConnectionContext context2 = pool.acquire();
    assertSame(context1, context2);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_contextReuse_multipleContextsReused(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(3, supplier);

    // Acquire all contexts.
    ConnectionContext context1 = pool.acquire();
    ConnectionContext context2 = pool.acquire();
    ConnectionContext context3 = pool.acquire();
    assertEquals(0, pool.size());

    pool.release(context1);
    pool.release(context2);
    pool.release(context3);
    assertEquals(3, pool.size());

    Set<ConnectionContext> originalContexts = new HashSet<>();
    originalContexts.add(context1);
    originalContexts.add(context2);
    originalContexts.add(context3);
    ConnectionContext reacquired1 = pool.acquire();
    ConnectionContext reacquired2 = pool.acquire();
    ConnectionContext reacquired3 = pool.acquire();

    assertTrue(originalContexts.contains(reacquired1));
    assertTrue(originalContexts.contains(reacquired2));
    assertTrue(originalContexts.contains(reacquired3));
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseCallsSetInactive(Supplier<ConnectionContext> supplier) {
    ConnectionContext mockContext = mock(ConnectionContext.class);
    ContextPool pool = new ContextPoolImpl(1, supplier);

    final boolean released = pool.release(mockContext);
    assertTrue(released);
    verify(mockContext, times(1)).setInactive();
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_clearPool(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(5, supplier);
    pool.acquire();
    assertEquals(4, pool.size());

    pool.clearPool();
    assertEquals(0, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseWhenAtCapacity_doesNotAddToPool(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, supplier);
    ConnectionContext ctx1 = pool.acquire();
    ConnectionContext ctx2 = pool.acquire();
    ConnectionContext ctx3 = pool.acquire();
    assertEquals(0, pool.size());

    assertTrue(pool.release(ctx1));
    assertTrue(pool.release(ctx2));
    assertEquals(2, pool.size());

    // Pool is at capacity, this release does not update pool.
    assertFalse(pool.release(ctx3));
    assertEquals(2, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_threadSafety_concurrentAcquireRelease(Supplier<ConnectionContext> supplier) throws Exception {
    final int maxIdleCount = 5;
    final int threadCount = 10;
    final int operationsPerThread = 100;

    ContextPool pool = new ContextPoolImpl(maxIdleCount, supplier);

    CompletableFuture<?>[] futures = new CompletableFuture[threadCount];
    for (int i = 0; i < threadCount; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
        for (int j = 0; j < operationsPerThread; j++) {
          ConnectionContext ctx = pool.acquire();
          assertNotNull(ctx);
          Thread.yield();
          pool.release(ctx);
        }
      });
    }

    CompletableFuture.allOf(futures).get(30, TimeUnit.SECONDS);

    assertTrue(pool.size() <= maxIdleCount);
  }
}
