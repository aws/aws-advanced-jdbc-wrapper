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
  void test_initialCapacity(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(5, 3, supplier);
    assertEquals(5, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_acquireFromPool(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(3, 2, supplier);
    assertEquals(3, pool.size());

    ConnectionContext context = pool.acquire();
    assertNotNull(context);
    assertEquals(2, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_acquireWhenEmpty(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(1, 1, supplier);
    pool.acquire();
    assertEquals(0, pool.size());

    ConnectionContext context = pool.acquire();
    assertNotNull(context);
    assertEquals(0, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseToPool(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, 2, supplier);
    ConnectionContext context = pool.acquire();
    assertEquals(1, pool.size());

    boolean released = pool.release(context);
    assertTrue(released);
    assertEquals(2, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseNull(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, 2, supplier);
    boolean released = pool.release(null);
    assertFalse(released);
    assertEquals(2, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_contextReuse(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(1, 1, supplier);
    ConnectionContext context1 = pool.acquire();
    pool.release(context1);

    ConnectionContext context2 = pool.acquire();
    assertSame(context1, context2);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_shrinkingWhenExceedsMaxIdle(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(5, 2, supplier);
    assertEquals(5, pool.size());

    pool.acquire();
    assertEquals(3, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_shrinkingMultipleAcquires(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(10, 3, supplier);
    assertEquals(10, pool.size());

    pool.acquire();
    // Current pool size is over the maxIdleCount, acquire should release one extra context every call.
    assertEquals(8, pool.size());

    pool.acquire();
    assertEquals(6, pool.size());

    pool.acquire();
    assertEquals(4, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_noShrinkingWhenBelowMaxIdle(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, 5, supplier);
    assertEquals(2, pool.size());

    pool.acquire();
    assertEquals(1, pool.size());

    pool.acquire();
    assertEquals(0, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseCallsSetInactive(Supplier<ConnectionContext> supplier) {
    ConnectionContext mockContext = mock(ConnectionContext.class);
    ContextPool pool = new ContextPoolImpl(1, 1, supplier);

    pool.release(mockContext);
    verify(mockContext, times(1)).setInactive();
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_clearPool(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(5, 3, supplier);
    assertEquals(5, pool.size());

    pool.clearPool();
    assertEquals(0, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_acquireAfterClear(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(3, 2, supplier);
    ConnectionContext context1 = pool.acquire();
    pool.clearPool();

    ConnectionContext context2 = pool.acquire();
    assertNotNull(context2);
    assertNotSame(context1, context2);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_multipleReleaseAndAcquire(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, 2, supplier);
    ConnectionContext ctx1 = pool.acquire();
    ConnectionContext ctx2 = pool.acquire();
    assertEquals(0, pool.size());

    pool.release(ctx1);
    assertEquals(1, pool.size());

    pool.release(ctx2);
    assertEquals(2, pool.size());

    ConnectionContext ctx3 = pool.acquire();
    assertSame(ctx1, ctx3);
    assertEquals(1, pool.size());
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_poolGrowthBeyondInitialCapacity(Supplier<ConnectionContext> supplier) {
    ContextPool pool = new ContextPoolImpl(2, 5, supplier);
    ConnectionContext ctx1 = pool.acquire();
    ConnectionContext ctx2 = pool.acquire();
    ConnectionContext ctx3 = pool.acquire();

    pool.release(ctx1);
    pool.release(ctx2);
    pool.release(ctx3);

    assertEquals(3, pool.size());
  }
}
