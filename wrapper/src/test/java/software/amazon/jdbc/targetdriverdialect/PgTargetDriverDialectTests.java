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

public class PgTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  @Mock private Connection mockConnection;
  @Mock private Statement mockJdbcStatement;
  @Mock private ResultSet mockResultSet;
  private final PgTargetDriverDialect dialect = new PgTargetDriverDialect();
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
    when(mockStatement.toString()).thenReturn("select * from T")
      .thenReturn(" /* delete from User */ delete from users ")
      .thenReturn(null);
    assertEquals("select * from T", dialect.getSQLQueryString(mockStatement));
    assertEquals(" /* delete from User */ delete from users ", dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }

  @Test
  void recognizesSupportedDataSourceClasses() {
    // The PG target driver dialect must recognize all PG data source classes it supports, including
    // the XA data source. If it does not, AwsWrapperXADataSource falls back to the generic dialect,
    // which does not propagate socket/connect timeouts to the target -- breaking failover fast-fail
    // during an XA branch. This must hold for every multi-release variant (base and java24), so this
    // test guards whichever variant matches the running JVM.
    assertTrue(dialect.isDialect("org.postgresql.ds.PGSimpleDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.ds.PGPoolingDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.ds.PGConnectionPoolDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.xa.PGXADataSource"),
        "PG target driver dialect must recognize the PG XA data source");
    assertFalse(dialect.isDialect("com.example.NotPgDataSource"));
  }

  @Test
  void readsAuthorizationSessionState() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("application_user");
    when(mockResultSet.getString(2)).thenReturn("tenant_a");
    when(mockResultSet.getString(3)).thenReturn("\"tenant_a\", public");
    when(mockResultSet.getString(4)).thenReturn("[\"pg_catalog\",\"tenant_a\",\"public\"]");

    final Optional<AuthorizationSessionState> result =
        dialect.readAuthorizationSessionState(mockConnection);

    assertEquals(Optional.of(new AuthorizationSessionState(
        "application_user",
        "tenant_a",
        "\"tenant_a\", public",
        "[\"pg_catalog\",\"tenant_a\",\"public\"]")), result);
  }

  @Test
  void returnsEmptyAuthorizationSessionStateWhenDatabaseReturnsNoRow() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    assertEquals(Optional.empty(), dialect.readAuthorizationSessionState(mockConnection));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET ROLE tenant_a",
      "SET SESSION ROLE tenant_a",
      "SET LOCAL ROLE tenant_a",
      "SET SESSION SESSION AUTHORIZATION tenant_a",
      "SET LOCAL SESSION AUTHORIZATION tenant_a",
      "SET LOCAL search_path TO tenant_a, public",
      "SET SCHEMA 'tenant_a'",
      "SET SESSION SCHEMA 'tenant_a'",
      "SET LOCAL SCHEMA 'tenant_a'",
      "SET \"role\" = 'tenant_a'",
      "SET session_authorization = 'tenant_a'",
      "RESET ROLE",
      "RESET \"role\"",
      "DISCARD ALL",
      "CALL switch_tenant()",
      "SET app.tenant_id = 'tenant-a'",
      "RESET app.tenant_id",
      "SELECT set_config('app.tenant_id', 'tenant-a', false)",
      "SELECT pg_catalog.\"set_config\"('search_path', 'tenant_a', false)",
      "SELECT 1; /* change tenant */ SET ROLE tenant_a",
      "/* outer /* inner */ still outer */ SET ROLE tenant_a"
  })
  void detectsStatementsThatMayChangeAuthorizationSessionState(final String sql) {
    assertTrue(dialect.mayChangeAuthorizationSessionState(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET app.tenant_id = 'tenant-a'",
      "SET LOCAL \"app.tenant_id\" TO 'tenant-a'",
      "RESET app.tenant_id",
      "CALL switch_tenant()",
      "DO $$ BEGIN PERFORM set_config('app.tenant_id', 'tenant-a', false); END $$",
      "SELECT set_config('app.tenant_id', 'tenant-a', false)",
      "SELECT pg_catalog.\"set_config\"('search_path', 'tenant_a', false)",
      "CREATE TEMP TABLE tenant_orders (id bigint)",
      "CREATE TEMPORARY TABLE tenant_orders (id bigint)",
      "SELECT * INTO TEMP tenant_orders FROM orders",
      "SELECT * INTO TEMPORARY TABLE tenant_orders FROM orders"
  })
  void detectsStatementsThatMayChangeUntrackedAuthorizationSessionState(final String sql) {
    assertTrue(dialect.mayChangeUntrackedAuthorizationSessionState(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET ROLE tenant_a",
      "SET SESSION ROLE tenant_a",
      "SET LOCAL ROLE tenant_a",
      "SET SESSION SESSION AUTHORIZATION tenant_a",
      "SET LOCAL SESSION AUTHORIZATION tenant_a",
      "SET LOCAL search_path TO tenant_a, public",
      "RESET ROLE",
      "SELECT * FROM orders",
      "SELECT 'SET app.tenant_id = tenant-a'"
  })
  void ignoresStatementsThatDoNotChangeUntrackedAuthorizationSessionState(final String sql) {
    assertFalse(dialect.mayChangeUntrackedAuthorizationSessionState(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM orders",
      "SHOW search_path",
      "SELECT current_user",
      "SELECT 'SET ROLE tenant_a'"
  })
  void ignoresStatementsThatDoNotChangeAuthorizationSessionState(final String sql) {
    assertFalse(dialect.mayChangeAuthorizationSessionState(sql));
  }
}
