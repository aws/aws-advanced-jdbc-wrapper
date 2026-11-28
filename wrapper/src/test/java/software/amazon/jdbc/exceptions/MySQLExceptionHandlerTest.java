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

package software.amazon.jdbc.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.exceptions.CJException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.jdbc.targetdriverdialect.MysqlConnectorJTargetDriverDialect;

public class MySQLExceptionHandlerTest {

  @Test
  public void testIsLoginException_NestedAuthenticationException() {

    MySQLExceptionHandler handler = new MySQLExceptionHandler();

    // Create the nested exception structure as described in requirements
    // Inner cause: Authentication exception with SQL state "28000"
    CJException authException = new CJException(
        "Access denied for user 'db_user'@'172.18.0.1' (using password: YES)"
    );
    authException.setSQLState("28000");

    // Outer exception: Connection exception with SQL state "08001"
    SQLNonTransientConnectionException connectionException = new SQLNonTransientConnectionException(
        "Could not create connection to database server. Attempted reconnect 3 times. Giving up.",
        "08001",
        authException
    );

    assertTrue(handler.isLoginException(connectionException, new MysqlConnectorJTargetDriverDialect()),
        "Should detect nested authentication exception with dialect");
  }

  @ParameterizedTest
  @CsvSource({
      "HY000, 1290, true",  // --read-only option
      "HY000, 1836, true",  // running in read-only mode
      "25006, 1792, true",  // cannot execute statement in a READ ONLY transaction
      "25006, 0, true",     // read-only transaction SQL state alone
      "HY000, 1792, true",  // read-only transaction error code alone
      "HY000, 1205, false", // lock wait timeout
      "42000, 1064, false", // syntax error
      "08S01, 0, false"     // communications link failure
  })
  public void testIsReadOnlyConnectionException(
      final String sqlState, final int errorCode, final boolean expected) {
    final MySQLExceptionHandler handler = new MySQLExceptionHandler();

    assertEquals(expected, handler.isReadOnlyConnectionException(sqlState, errorCode));
    assertEquals(expected, handler.isReadOnlyConnectionException(
        new SQLException("test", sqlState, errorCode), new MysqlConnectorJTargetDriverDialect()));
  }

  @Test
  public void testIsReadOnlyConnectionException_NestedReadOnlyTransaction() {
    final MySQLExceptionHandler handler = new MySQLExceptionHandler();
    final SQLException readOnlyTransaction =
        new SQLException("Cannot execute statement in a READ ONLY transaction.", "25006", 1792);
    final SQLException wrapper = new SQLException("wrapped", null, 0, readOnlyTransaction);

    assertTrue(handler.isReadOnlyConnectionException(wrapper, new MysqlConnectorJTargetDriverDialect()));
  }

  @Test
  public void testIsReadOnlyConnectionException_NullSqlState() {
    assertFalse(new MySQLExceptionHandler().isReadOnlyConnectionException((String) null, (Integer) null));
  }
}
