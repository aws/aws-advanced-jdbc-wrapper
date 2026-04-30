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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link WrapperUtils#isSameConnection(Connection, Connection)}.
 * Covers identity checks, JDK dynamic proxy inner-class detection,
 * isWrapperFor fallback, and negative cases.
 */
class WrapperUtilsIsSameConnectionTest {

  @Test
  void testSameObjectIdentity() {
    Connection conn = mock(Connection.class);
    assertTrue(WrapperUtils.isSameConnection(conn, conn),
        "Same object reference should return true");
  }

  @Test
  void testDifferentNonProxyConnections() {
    Connection conn1 = mock(Connection.class);
    Connection conn2 = mock(Connection.class);
    assertFalse(WrapperUtils.isSameConnection(conn1, conn2),
        "Different non-proxy connections should return false");
  }

  @Test
  void testTwoProxiesWithSameHandler() {
    InvocationHandler sharedHandler = (proxy, method, args) -> null;
    Connection proxyA = createProxyConnection(sharedHandler);
    Connection proxyB = createProxyConnection(sharedHandler);

    assertTrue(WrapperUtils.isSameConnection(proxyA, proxyB),
        "Two proxies sharing the same InvocationHandler should be the same connection");
  }

  @Test
  void testTwoProxiesWithDifferentHandlers() {
    InvocationHandler handlerA = (proxy, method, args) -> null;
    InvocationHandler handlerB = (proxy, method, args) -> null;
    Connection proxyA = createProxyConnection(handlerA);
    Connection proxyB = createProxyConnection(handlerB);

    assertFalse(WrapperUtils.isSameConnection(proxyA, proxyB),
        "Two proxies with unrelated handlers should not be the same connection");
  }

  /**
   * Simulates MySQL's loadbalance proxy pattern: the outer handler is the
   * LoadBalancedConnectionProxy, and the inner handler is a JdbcInterfaceProxy
   * with a this$0 field pointing to the outer handler.
   */
  @Test
  void testInnerClassProxyDetection() {
    OuterHandler outer = new OuterHandler();
    OuterHandler.InnerHandler inner = outer.new InnerHandler();

    Connection proxyOuter = createProxyConnection(outer);
    Connection proxyInner = createProxyConnection(inner);

    assertTrue(WrapperUtils.isSameConnection(proxyOuter, proxyInner),
        "Proxy with inner-class handler whose this$0 points to outer handler should match");
    // Test reverse order too
    assertTrue(WrapperUtils.isSameConnection(proxyInner, proxyOuter),
        "Order should not matter for inner-class proxy detection");
  }

  @Test
  void testIsWrapperForFallback() throws SQLException {
    Connection conn1 = mock(Connection.class);
    Connection conn2 = mock(Connection.class);
    when(conn1.isWrapperFor(conn2.getClass())).thenReturn(true);

    assertTrue(WrapperUtils.isSameConnection(conn1, conn2),
        "Should fall back to isWrapperFor when other checks fail");
  }

  @Test
  void testIsWrapperForReverse() throws SQLException {
    Connection conn1 = mock(Connection.class);
    Connection conn2 = mock(Connection.class);
    when(conn1.isWrapperFor(conn2.getClass())).thenReturn(false);
    when(conn2.isWrapperFor(conn1.getClass())).thenReturn(true);

    assertTrue(WrapperUtils.isSameConnection(conn1, conn2),
        "Should check isWrapperFor in both directions");
  }

  @Test
  void testIsWrapperForThrowsException() throws SQLException {
    Connection conn1 = mock(Connection.class);
    Connection conn2 = mock(Connection.class);
    when(conn1.isWrapperFor(conn2.getClass())).thenThrow(new SQLException("unsupported"));
    when(conn2.isWrapperFor(conn1.getClass())).thenThrow(new SQLException("unsupported"));

    assertFalse(WrapperUtils.isSameConnection(conn1, conn2),
        "Should return false when isWrapperFor throws and no other match");
  }

  @Test
  void testProxyAndNonProxyDifferent() {
    InvocationHandler handler = (proxy, method, args) -> null;
    Connection proxyConn = createProxyConnection(handler);
    Connection regularConn = mock(Connection.class);

    assertFalse(WrapperUtils.isSameConnection(proxyConn, regularConn),
        "Proxy and unrelated non-proxy should not match");
  }

  @Test
  void testNullSafety() {
    // isSameConnection doesn't accept null per its signature, but verify no NPE
    // if one connection is a proxy and the other is not, with no wrapping relationship
    Connection proxy = createProxyConnection((p, m, a) -> null);
    Connection mock = mock(Connection.class);
    assertFalse(WrapperUtils.isSameConnection(proxy, mock));
    assertFalse(WrapperUtils.isSameConnection(mock, proxy));
  }

  // --- Helper classes to simulate MySQL's inner-class proxy pattern ---

  /** Simulates MySQL's LoadBalancedConnectionProxy (outer handler). */
  static class OuterHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return null;
    }

    /** Simulates MySQL's JdbcInterfaceProxy (inner class with this$0 reference). */
    class InnerHandler implements InvocationHandler {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) {
        return null;
      }
    }
  }

  private static Connection createProxyConnection(InvocationHandler handler) {
    return (Connection) Proxy.newProxyInstance(
        WrapperUtilsIsSameConnectionTest.class.getClassLoader(),
        new Class<?>[] { Connection.class },
        handler);
  }
}
