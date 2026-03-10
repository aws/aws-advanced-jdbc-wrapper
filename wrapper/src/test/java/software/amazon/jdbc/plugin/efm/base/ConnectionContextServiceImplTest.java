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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.ParameterizedTest;
import software.amazon.jdbc.plugin.efm.v1.HostMonitorConnectionContextV1;
import software.amazon.jdbc.plugin.efm.v2.HostMonitorConnectionContextV2;

class ConnectionContextServiceImplTest {

  static Stream<Arguments> contextSuppliers() {
    return Stream.of(
        Arguments.of(HostMonitorConnectionContextV1.class),
        Arguments.of(HostMonitorConnectionContextV2.class)
    );
  }

  @AfterEach
  void cleanUp() {
    System.clearProperty("aws.jdbc.efm.contextPool.initialCapacity");
    System.clearProperty("aws.jdbc.efm.contextPool.maxIdleCount");
    ConnectionContextServiceImpl.clear();
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_defaultConfiguration(Class<? extends ConnectionContext> contextClass) {
    ConnectionContextService service = new ConnectionContextServiceImpl();
    ConnectionContext context = service.acquire(contextClass);
    assertNotNull(context);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_acquireAndRelease(Class<? extends ConnectionContext> contextClass) {
    ConnectionContextService service = new ConnectionContextServiceImpl();
    ConnectionContext context = service.acquire(contextClass);

    boolean released = service.release(context);
    assertTrue(released);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_contextReuse(Class<? extends ConnectionContext> contextClass) {
    ConnectionContextService service = new ConnectionContextServiceImpl();
    ConnectionContext context1 = service.acquire(contextClass);
    service.release(context1);

    ConnectionContext context2 = service.acquire(contextClass);
    assertSame(context1, context2);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_releaseCallsSetInactive(Class<? extends ConnectionContext> contextClass) {
    ConnectionContext mockContext = mock(ConnectionContext.class);
    ConnectionContextService service = new ConnectionContextServiceImpl();

    service.release(mockContext);
    verify(mockContext, times(1)).setInactive();
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_customInitialCapacity(Class<? extends ConnectionContext> contextClass) {
    System.setProperty("aws.jdbc.efm.contextPool.initialCapacity", "5");
    ConnectionContextService service = new ConnectionContextServiceImpl();
    assertNotNull(service);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_customMaxIdleCount(Class<? extends ConnectionContext> contextClass) {
    System.setProperty("aws.jdbc.efm.contextPool.maxIdleCount", "3");
    ConnectionContextService service = new ConnectionContextServiceImpl();
    assertNotNull(service);
  }

  @ParameterizedTest
  @MethodSource("contextSuppliers")
  void test_multipleAcquireAndRelease(Class<? extends ConnectionContext> contextClass) {
    System.setProperty("aws.jdbc.efm.contextPool.initialCapacity", "1");
    ConnectionContextService service = new ConnectionContextServiceImpl();
    ConnectionContext ctx1 = service.acquire(contextClass);
    ConnectionContext ctx2 = service.acquire(contextClass);
    ConnectionContext ctx3 = service.acquire(contextClass);

    service.release(ctx1);
    service.release(ctx2);
    service.release(ctx3);

    ConnectionContext ctx4 = service.acquire(contextClass);
    assertSame(ctx1, ctx4);
  }
}
