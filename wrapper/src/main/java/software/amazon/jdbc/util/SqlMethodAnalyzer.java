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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcMethod;

public class SqlMethodAnalyzer {

  private static final Set<String> CLOSING_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_CLOSE.methodName,
          JdbcMethod.CONNECTION_ABORT.methodName,
          JdbcMethod.STATEMENT_CLOSE.methodName,
          JdbcMethod.CALLABLESTATEMENT_CLOSE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_CLOSE.methodName,
          JdbcMethod.RESULTSET_CLOSE.methodName
      )));

  private static final Set<String> EXECUTE_SQL_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.STATEMENT_EXECUTE.methodName,
          JdbcMethod.STATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName,
          JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName
      )));

  private static final Set<String> CLOSE_TRANSACTION_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_COMMIT.methodName,
          JdbcMethod.CONNECTION_ROLLBACK.methodName,
          JdbcMethod.CONNECTION_CLOSE.methodName,
          JdbcMethod.CONNECTION_ABORT.methodName
      )));

  public boolean doesOpenTransaction(final Connection conn, final String methodName,
      final @Nullable Object[] args) {
    if (!(EXECUTE_SQL_METHOD_NAMES.contains(methodName) && args != null && args.length >= 1)) {
      return false;
    }

    final String statement = getFirstSqlStatement(String.valueOf(args[0]));
    if (isStatementStartingTransaction(statement)) {
      return true;
    }

    final boolean autocommit;
    try {
      autocommit = conn.getAutoCommit();
    } catch (final SQLException e) {
      return false;
    }

    return !autocommit && isStatementDml(statement);
  }

  private String getFirstSqlStatement(final String sql) {
    // Comments are removed before the query is normalized and split. Both steps depend on it:
    // parseMultiStatementQueries collapses every whitespace run (including the newline that
    // terminates a line comment) into a single space, and a ";" inside a comment must not be
    // treated as a statement separator.
    List<String> statementList = parseMultiStatementQueries(stripComments(sql));
    if (statementList.isEmpty()) {
      return sql;
    }
    String statement = statementList.get(0);
    statement = statement.toUpperCase();
    return statement.trim();
  }

  /**
   * Removes SQL comments from a query so that the leading keyword of a statement can be matched
   * reliably.
   *
   * <p>Line comments have to be removed before whitespace is collapsed: once the terminating
   * newline is gone, the comment and the statement on the following line become indistinguishable,
   * so {@code "-- note\nBEGIN"} would look like {@code "-- NOTE BEGIN"} and no longer start with
   * {@code BEGIN}, even though the database opens a transaction for it.
   *
   * <p>Dialect notes: PostgreSQL treats {@code --} as a comment through the end of the line
   * unconditionally, while MySQL requires whitespace (or a control character) after {@code --} and
   * additionally supports {@code #} line comments. The most permissive reading is applied here
   * ({@code --} and {@code #} always begin a comment), because failing to recognize a comment is
   * the harmful direction: it hides a transaction-control statement from the caller. The opposite
   * risk is benign, since every caller only compares the leading keyword of a statement, so
   * truncating a MySQL {@code 1--2} expression or a PostgreSQL {@code #} operator cannot change the
   * outcome.
   *
   * <p>Quoted sections are preserved so a comment marker inside a string literal or a quoted
   * identifier is not mistaken for a comment. Backslash escapes and PostgreSQL dollar quoting are
   * not interpreted; for the same reason as above, that cannot change which keyword a statement
   * starts with. An unterminated block comment is left in place, as before.
   *
   * <p>Each comment is replaced by a single space so that adjacent tokens do not merge, matching
   * the behavior of the block-comment handling this replaces.
   */
  private static String stripComments(final String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }

    final int length = sql.length();
    final StringBuilder result = new StringBuilder(length);
    int i = 0;

    while (i < length) {
      final char c = sql.charAt(i);

      if (c == '\'' || c == '"' || c == '`') {
        final int end = skipQuoted(sql, i);
        result.append(sql, i, end);
        i = end;
      } else if (c == '#' || (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-')) {
        // Line comment: drop everything up to, but not including, the line terminator. Keeping the
        // terminator leaves a whitespace boundary between the comment and the next statement.
        i = skipToEndOfLine(sql, i);
        result.append(' ');
      } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
        final int end = sql.indexOf("*/", i + 2);
        if (end < 0) {
          result.append(sql, i, length);
          i = length;
        } else {
          i = end + 2;
          result.append(' ');
        }
      } else {
        result.append(c);
        i++;
      }
    }

    return result.toString();
  }

  /**
   * Returns the index just past the quoted section that starts at {@code start}. A doubled quote
   * character escapes itself. An unterminated section extends to the end of the query.
   */
  private static int skipQuoted(final String sql, final int start) {
    final char quote = sql.charAt(start);
    final int length = sql.length();
    int i = start + 1;

    while (i < length) {
      if (sql.charAt(i) == quote) {
        if (i + 1 < length && sql.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }

    return length;
  }

  /** Returns the index of the line terminator ending the current line, or the end of the query. */
  private static int skipToEndOfLine(final String sql, final int start) {
    for (int i = start; i < sql.length(); i++) {
      final char c = sql.charAt(i);
      if (c == '\n' || c == '\r') {
        return i;
      }
    }
    return sql.length();
  }

  private List<String> parseMultiStatementQueries(String query) {
    if (StringUtils.isNullOrEmpty(query)) {
      return new ArrayList<>();
    }

    query = query.replaceAll("\\s+", " ");

    // Check to see if string only has blank spaces.
    if (query.trim().isEmpty()) {
      return new ArrayList<>();
    }

    return Arrays.stream(query.split(";")).collect(Collectors.toList());
  }

  public boolean doesCloseTransaction(final Connection conn, final String methodName,
      final @Nullable Object[] args) {
    if (CLOSE_TRANSACTION_METHOD_NAMES.contains(methodName)) {
      return true;
    }

    if (doesSwitchAutoCommitFalseTrue(conn, methodName, args)) {
      return true;
    }

    if (!(methodName.contains("execute") && args != null && args.length >= 1)) {
      return false;
    }

    final String statement = getFirstSqlStatement(String.valueOf(args[0]));
    return isStatementClosingTransaction(statement);
  }

  public boolean isStatementDml(final String statement) {
    return !isStatementStartingTransaction(statement)
        && !isStatementClosingTransaction(statement)
        && !statement.startsWith("SET ")
        && !statement.startsWith("USE ")
        && !statement.startsWith("SHOW ");
  }

  public boolean isStatementStartingTransaction(final String statement) {
    return statement.startsWith("BEGIN") || statement.startsWith("START TRANSACTION");
  }

  public boolean isStatementClosingTransaction(final String statement) {
    return statement.startsWith("COMMIT")
        || statement.startsWith("ROLLBACK")
        || statement.startsWith("END")
        || statement.startsWith("ABORT")
        // Two-phase commit control statements. "PREPARE TRANSACTION" detaches the current
        // transaction from the session (the session leaves the active transaction); "COMMIT
        // PREPARED"/"ROLLBACK PREPARED" resolve a previously-prepared branch and already match the
        // COMMIT/ROLLBACK prefixes above. Note: this matches only "PREPARE TRANSACTION", not the
        // unrelated "PREPARE <name> AS ..." prepared-statement command.
        || statement.startsWith("PREPARE TRANSACTION");
  }

  public boolean isStatementSettingAutoCommit(final String methodName, final @Nullable Object[] args) {
    if (args == null || args.length < 1) {
      return false;
    }

    if (!EXECUTE_SQL_METHOD_NAMES.contains(methodName)) {
      return false;
    }

    final String statement = getFirstSqlStatement(String.valueOf(args[0]));
    return statement.startsWith("SET AUTOCOMMIT");
  }

  public boolean doesSwitchAutoCommitFalseTrue(final Connection conn, final String methodName,
      final @Nullable Object[] jdbcMethodArgs) {
    final boolean isStatementSettingAutoCommit = isStatementSettingAutoCommit(
        methodName, jdbcMethodArgs);
    if (!isStatementSettingAutoCommit && !JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName.equals(methodName)) {
      return false;
    }

    final boolean oldAutoCommitVal;
    Boolean newAutoCommitVal = null;
    try {
      oldAutoCommitVal = conn.getAutoCommit();
    } catch (final SQLException e) {
      return false;
    }

    if (JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName.equals(methodName) && jdbcMethodArgs.length > 0) {
      newAutoCommitVal = (Boolean) jdbcMethodArgs[0];
    } else if (isStatementSettingAutoCommit) {
      newAutoCommitVal = getAutoCommitValueFromSqlStatement(jdbcMethodArgs);
    }

    return !oldAutoCommitVal && Boolean.TRUE.equals(newAutoCommitVal);
  }

  public @Nullable Boolean getAutoCommitValueFromSqlStatement(final @Nullable Object[] args) {
    if (args == null || args.length < 1) {
      return null;
    }

    String sql = getFirstSqlStatement(String.valueOf(args[0]));

    final int valueIndex;
    int separatorIndex = sql.indexOf("=");

    if (separatorIndex != -1) {
      valueIndex = separatorIndex + 1;
    } else {
      separatorIndex = sql.indexOf(" TO ");
      if (separatorIndex == -1) {
        return null;
      } else {
        valueIndex = separatorIndex + 3;
      }
    }

    sql = sql.substring(valueIndex);
    if (sql.contains(";")) {
      sql = sql.substring(0, sql.indexOf(";"));
    }

    sql = sql.trim();
    if ("FALSE".equals(sql) || "0".equals(sql) || "OFF".equals(sql)) {
      return false;
    } else if ("TRUE".equals(sql) || "1".equals(sql) || "ON".equals(sql)) {
      return true;
    } else {
      return null;
    }
  }

  public boolean isMethodClosingSqlObject(final String methodName) {
    return CLOSING_METHOD_NAMES.contains(methodName);
  }
}
