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

package software.amazon.jdbc.targetdriverdialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TargetDriverDialect#isSameConnection(Connection, Connection)}.
 * Tests both the default implementation (identity check) and the MySQL-specific
 * implementation that uses {@link JdbcConnection#getMultiHostSafeProxy()} to detect
 * proxy connections from loadbalance:// and replication:// protocols.
 */
class IsSameConnectionTest {

  // --- Default TargetDriverDialect tests (identity check only) ---

  @Test
  void testDefaultDialect_sameObjectIdentity() {
    TargetDriverDialect dialect = new GenericTargetDriverDialect();
    Connection conn = mock(Connection.class);
    assertTrue(dialect.isSameConnection(conn, conn));
  }

  @Test
  void testDefaultDialect_differentConnections() {
    TargetDriverDialect dialect = new GenericTargetDriverDialect();
    assertFalse(dialect.isSameConnection(mock(Connection.class), mock(Connection.class)));
  }

  // --- MySQL TargetDriverDialect tests ---

  @Test
  void testMysqlDialect_sameObjectIdentity() {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    Connection conn = mock(Connection.class);
    assertTrue(dialect.isSameConnection(conn, conn));
  }

  @Test
  void testMysqlDialect_differentNonJdbcConnections() {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    // Plain Connection mocks don't implement JdbcConnection, so they fall through
    assertFalse(dialect.isSameConnection(mock(Connection.class), mock(Connection.class)));
  }

  @Test
  void testMysqlDialect_sameMultiHostSafeProxy() {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    JdbcConnection sharedProxy = mock(JdbcConnection.class);

    JdbcConnection connA = mock(JdbcConnection.class);
    JdbcConnection connB = mock(JdbcConnection.class);
    when(connA.getMultiHostSafeProxy()).thenReturn(sharedProxy);
    when(connB.getMultiHostSafeProxy()).thenReturn(sharedProxy);

    assertTrue(dialect.isSameConnection(connA, connB),
        "Two JdbcConnections with the same getMultiHostSafeProxy() should be the same connection");
  }

  @Test
  void testMysqlDialect_differentMultiHostSafeProxy() {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();

    JdbcConnection connA = mock(JdbcConnection.class);
    JdbcConnection connB = mock(JdbcConnection.class);
    when(connA.getMultiHostSafeProxy()).thenReturn(mock(JdbcConnection.class));
    when(connB.getMultiHostSafeProxy()).thenReturn(mock(JdbcConnection.class));

    assertFalse(dialect.isSameConnection(connA, connB),
        "Two JdbcConnections with different getMultiHostSafeProxy() should not be the same");
  }

  @Test
  void testMysqlDialect_jdbcConnectionAndPlainConnection() {
    MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
    JdbcConnection mysqlConn = mock(JdbcConnection.class);
    Connection plainConn = mock(Connection.class);

    assertFalse(dialect.isSameConnection(mysqlConn, plainConn));
    assertFalse(dialect.isSameConnection(plainConn, mysqlConn));
  }
}
