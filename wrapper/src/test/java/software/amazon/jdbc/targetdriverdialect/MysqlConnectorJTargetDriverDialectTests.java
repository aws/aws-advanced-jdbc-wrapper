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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;

public class MysqlConnectorJTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  @Mock private Connection mockConnection;
  @Mock private Statement mockJdbcStatement;
  @Mock private Statement mockFallbackJdbcStatement;
  @Mock private ResultSet mockResultSet;
  private final MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testGetQueryFromPreparedStatement() {
    when(mockStatement.toString()).thenReturn("com.mysql.cj.jdbc.ClientPreparedStatement: select * from T where A=1")
      .thenReturn("com.mysql.cj.jdbc.ClientPreparedStatement: /* CACHE_PARAM(ttl=50s) */ select book0_.id as id1, "
          + "book0_.title as title2 from Book book0_ where book0_.id=1 ")
      .thenReturn("not a proper response")
      .thenReturn(null);
    assertEquals(" select * from T where A=1", dialect.getSQLQueryString(mockStatement));
    assertEquals(" /* CACHE_PARAM(ttl=50s) */ select book0_.id as id1, book0_.title as title2 from "
        + "Book book0_ where book0_.id=1 ", dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }

  @Test
  void readsAuthorizationSessionState() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("application_user@client.example");
    when(mockResultSet.getString(2)).thenReturn("application_user@%");
    when(mockResultSet.getString(3)).thenReturn("`tenant_a`@`%`");
    when(mockResultSet.getString(4)).thenReturn("orders");

    assertEquals(Optional.of(new AuthorizationSessionState(
        "application_user@client.example",
        "application_user@%",
        "",
        "",
        "`tenant_a`@`%`",
        "orders")), dialect.readAuthorizationSessionState(mockConnection));
  }

  @Test
  void readsAuthorizationSessionStateWithoutRolesOnOlderMysql() throws SQLException {
    when(mockConnection.createStatement())
        .thenReturn(mockJdbcStatement, mockFallbackJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString()))
        .thenThrow(new SQLException("CURRENT_ROLE is unavailable", "42000", 1305));
    when(mockFallbackJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("application_user@client.example");
    when(mockResultSet.getString(2)).thenReturn("application_user@%");
    when(mockResultSet.getString(3)).thenReturn("orders");

    assertEquals(Optional.of(new AuthorizationSessionState(
        "application_user@client.example",
        "application_user@%",
        "",
        "",
        "",
        "orders")), dialect.readAuthorizationSessionState(mockConnection));
  }

  @Test
  void doesNotOmitRolesForOtherAuthorizationStateQueryFailures() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString()))
        .thenThrow(new SQLException("connection unavailable", "08006", 2013));

    assertThrows(SQLException.class, () -> dialect.readAuthorizationSessionState(mockConnection));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET ROLE tenant_a",
      "SET ROLE DEFAULT",
      "USE tenant_a",
      "RESET CONNECTION",
      "SELECT 1; /* tenant switch */ USE tenant_b"
  })
  void detectsTrackedAuthorizationStateChanges(final String sql) {
    assertEquals(AuthorizationStateImpact.TRACKED, dialect.getAuthorizationStateImpact(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "CALL switch_tenant()",
      "DO @tenant_id := 'tenant_a'",
      "SET @tenant_id = 'tenant_a'",
      "EXECUTE tenant_stmt",
      "/*! USE tenant_b */",
      "/*M! SET ROLE tenant_b */",
      "CREATE TEMPORARY TABLE tenant_orders (id bigint)",
      "CREATE TEMPORARY TABLE tenant_orders SELECT * FROM orders",
      "CREATE OR REPLACE TEMPORARY TABLE tenant_orders (id bigint)"
  })
  void detectsUntrackedAuthorizationStateChanges(final String sql) {
    assertEquals(AuthorizationStateImpact.UNTRACKED, dialect.getAuthorizationStateImpact(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 1",
      "SET DEFAULT ROLE tenant_a TO application_user",
      "PREPARE tenant_stmt FROM 'USE tenant_a'",
      "DEALLOCATE PREPARE tenant_stmt",
      "SELECT 'SET ROLE tenant_a'",
      "SELECT 1 /* USE tenant_a */"
  })
  void ignoresStatementsThatDoNotChangeAuthorizationSessionState(final String sql) {
    assertEquals(AuthorizationStateImpact.NONE, dialect.getAuthorizationStateImpact(sql));
  }
}
