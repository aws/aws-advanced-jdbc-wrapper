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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SqlMethodAnalyzerTest {

  @Mock Connection conn;


  private final SqlMethodAnalyzer sqlMethodAnalyzer = new SqlMethodAnalyzer();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @ParameterizedTest
  @MethodSource("openTransactionQueries")
  void testOpenTransaction(final String methodName, final String sql, final boolean autocommit,
      final boolean expected)
      throws SQLException {
    final Object[] args;
    if (sql != null) {
      args = new Object[] {sql};
    } else {
      args = new Object[] {};
    }

    when(conn.getAutoCommit()).thenReturn(autocommit);
    final boolean actual = sqlMethodAnalyzer.doesOpenTransaction(conn, methodName, args);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("closeTransactionQueries")
  void testCloseTransaction(final String methodName, final String sql, final boolean expected) {
    final Object[] args;
    if (sql != null) {
      args = new Object[] {sql};
    } else {
      args = new Object[] {};
    }

    final boolean actual = sqlMethodAnalyzer.doesCloseTransaction(conn, methodName, args);
    assertEquals(expected, actual);
  }

  @Test
  void testDoesSwitchAutoCommitFalseTrue() throws SQLException {
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Connection.setAutoCommit",
        new Object[] {false}));
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Statement.execute",
        new Object[] {"SET autocommit = 0"}));

    assertTrue(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Connection.setAutoCommit",
        new Object[] {true}));
    assertTrue(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Statement.execute",
        new Object[] {"SET autocommit = 1"}));

    when(conn.getAutoCommit()).thenReturn(true);

    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Connection.setAutoCommit",
        new Object[] {false}));
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Statement.execute",
        new Object[] {"SET autocommit = 0"}));
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Connection.setAutoCommit",
        new Object[] {true}));
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Statement.execute",
        new Object[] {"SET autocommit = 1"}));
    assertFalse(sqlMethodAnalyzer.doesSwitchAutoCommitFalseTrue(conn, "Statement.execute",
        new Object[] {"SET TIME ZONE 'UTC'"}));
  }

  @ParameterizedTest
  @MethodSource("isSettingAutoCommitQueries")
  void testIsStatementSettingAutoCommit(final String methodName, final String sql,
      final boolean expected) {
    final Object[] args;
    if (sql != null) {
      args = new Object[] {sql};
    } else {
      args = new Object[] {};
    }

    final boolean actual = sqlMethodAnalyzer.isStatementSettingAutoCommit(methodName, args);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getAutoCommitQueries")
  void testGetAutoCommit(final String sql, final Boolean expected) {
    final Object[] args;
    if (sql != null) {
      args = new Object[] {sql};
    } else {
      args = new Object[] {};
    }

    final Boolean actual = sqlMethodAnalyzer.getAutoCommitValueFromSqlStatement(args);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getIsMethodClosingSqlObjectMethods")
  void testIsMethodClosingSqlObject(final String methodName, final boolean expected) {
    final boolean actual = sqlMethodAnalyzer.isMethodClosingSqlObject(methodName);
    assertEquals(expected, actual);
  }

  private static Stream<Arguments> openTransactionQueries() {
    return Stream.of(
        Arguments.of("Statement.execute", "  bEgIn ; ", true, true),
        Arguments.of("Statement.execute", "START TRANSACTION", true, true),
        Arguments.of("Statement.execute", "START /* COMMENT */ TRANSACTION; SELECT 1;", true, true),
        Arguments.of("Statement.execute", "START/* COMMENT */TRANSACTION;", true, true),
        Arguments.of("Statement.execute", "START      /* COMMENT */    TRANSACTION;", true, true),
        Arguments.of("Statement.executeUpdate", "START   /*COMMENT*/TRANSACTION;", true, true),
        Arguments.of("Statement.executeUpdate", "/*COMMENT*/START   /*COMMENT*/TRANSACTION;", true,
            true),
        Arguments.of("Statement.executeUpdate", " /*COMMENT*/ START   /*COMMENT*/TRANSACTION;",
            true, true),
        Arguments.of("Statement.executeUpdate", " /*COMMENT*/ begin", true, true),
        Arguments.of("Statement.executeUpdate", "commit", false, false),
        Arguments.of("Statement.executeQuery", " select 1", true, false),
        Arguments.of("Statement.executeQuery", " SELECT 1", false, true),
        Arguments.of("Statement.executeUpdate", " INSERT INTO test_table VALUES (1) ; ", false,
            true),
        Arguments.of("Statement.executeUpdate", " set autocommit = 1 ", false, false),
        Arguments.of("Connection.commit", null, false, false)
    );
  }

  private static Stream<Arguments> closeTransactionQueries() {
    return Stream.of(
        Arguments.of("Statement.execute", "rollback;", true),
        Arguments.of("Statement.execute", "commit;", true),
        Arguments.of("Statement.executeUpdate", "end", true),
        Arguments.of("Statement.executeUpdate", "abort;", true),
        Arguments.of("Statement.execute", "select 1", false),
        Arguments.of("Statement.close", null, false),
        Arguments.of("Statement.isClosed", null, false),
        Arguments.of("Connection.commit", null, true),
        Arguments.of("Connection.rollback", null, true),
        Arguments.of("Connection.close", null, true),
        Arguments.of("Connection.abort", null, true)
    );
  }

  private static Stream<Arguments> isSettingAutoCommitQueries() {
    return Stream.of(
        Arguments.of("Connection.commit", null, false),
        Arguments.of("Statement.execute", " START  TRANSACTION   READ  ONLY", false),
        Arguments.of("Statement.execute", "  set  autocommit = 1 ; ", true),
        Arguments.of("Statement.executeUpdate", "SET AUTOCOMMIT TO OFF ;  ", true)
    );
  }

  private static Stream<Arguments> getAutoCommitQueries() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of("SELECT 1; SET AUTOCOMMIT = 1", null),
        Arguments.of("  set autocommit to off", false),
        Arguments.of("SET AUTOCOMMIT = false", false),
        Arguments.of("SET AUTOCOMMIT to 0", false),
        Arguments.of("set autoCOMMIT = on", true),
        Arguments.of("set autoCOMMIT TO trUE", true),
        Arguments.of("  SeT  aUtOcommIT  = 1", true)
    );
  }

  private static Stream<Arguments> getIsMethodClosingSqlObjectMethods() {
    return Stream.of(
        Arguments.of("Statement.close", true),
        Arguments.of("Statement.closeOnCompletion", false),
        Arguments.of("Statement.isClosed", false),
        Arguments.of("Connection.commit", false),
        Arguments.of("Connection.rollback", false),
        Arguments.of("Connection.close", true),
        Arguments.of("Connection.abort", true)
    );
  }
}
