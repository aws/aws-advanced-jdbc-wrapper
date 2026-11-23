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

package software.amazon.jdbc.plugin.encryption;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SQL fragments shared by the plugin's queries against its metadata and key tables and by the
 * re-encryption queries. Their comparisons use these operators, which on PostgreSQL are the
 * built-in operators regardless of search_path.
 */
public final class EncryptionSql {

  /** The PostgreSQL equality operator, pinned to pg_catalog. */
  public static final String PG_EQUALS = "OPERATOR(pg_catalog.=)";

  /** The PostgreSQL greater-than operator, pinned to pg_catalog. */
  public static final String PG_GREATER_THAN = "OPERATOR(pg_catalog.>)";

  /** The MySQL equality operator. MySQL has no user-defined operators. */
  public static final String MYSQL_EQUALS = "=";

  /** Limits a session to the built-in objects, and puts temporary objects last. */
  public static final String PIN_PG_SEARCH_PATH_SQL = "SET search_path = pg_catalog, pg_temp";

  private EncryptionSql() {
  }

  /**
   * Returns the equality operator to use in the plugin's own SQL.
   *
   * @param isPostgreSQL whether the SQL runs on PostgreSQL
   * @return the operator, without surrounding whitespace
   */
  public static String equalsOperator(boolean isPostgreSQL) {
    return isPostgreSQL ? PG_EQUALS : MYSQL_EQUALS;
  }

  /**
   * Tells whether a connection is to PostgreSQL, from the database product name it reports.
   *
   * @param connection the connection to check
   * @return true for PostgreSQL, false otherwise or when the driver does not report a product name
   * @throws SQLException if the driver fails to report its metadata
   */
  public static boolean isPostgreSQL(Connection connection) throws SQLException {
    final @Nullable DatabaseMetaData metaData = connection.getMetaData();
    if (metaData == null) {
      return false;
    }
    final @Nullable String productName = metaData.getDatabaseProductName();
    return productName != null && productName.toLowerCase(Locale.ROOT).contains("postgresql");
  }

  /**
   * Pins the search_path of a connection to the built-in objects. Only for connections the plugin
   * opens for itself: on any other connection this would change the application's session.
   *
   * @param connection a connection the plugin opened and will close itself
   * @throws SQLException if the statement fails
   */
  public static void pinPgSearchPath(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(PIN_PG_SEARCH_PATH_SQL);
    }
  }

  /**
   * Pins the search_path of a connection the plugin opened for itself, as
   * {@link #pinPgSearchPath(Connection)} does, and closes the connection if that fails.
   *
   * @param connection a connection the plugin opened and will close itself
   * @throws SQLException if the statement fails; a failure to close is added as suppressed
   */
  public static void pinPgSearchPathOrClose(Connection connection) throws SQLException {
    try {
      pinPgSearchPath(connection);
    } catch (SQLException e) {
      try {
        connection.close();
      } catch (SQLException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }
}
