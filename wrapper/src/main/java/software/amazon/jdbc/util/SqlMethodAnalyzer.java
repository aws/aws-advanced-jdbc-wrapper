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

  private static final String SET_AUTOCOMMIT_PREFIX = "SET AUTOCOMMIT";

  private static final Set<String> CLOSE_TRANSACTION_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JdbcMethod.CONNECTION_COMMIT.methodName,
          JdbcMethod.CONNECTION_ROLLBACK.methodName,
          JdbcMethod.CONNECTION_CLOSE.methodName,
          JdbcMethod.CONNECTION_ABORT.methodName
      )));

  /**
   * Everything this analyzer can derive from the SQL of a single JDBC call, so the same text is
   * parsed once per call rather than once per question asked about it. A call with no SQL argument
   * is represented by {@link #NO_STATEMENT}.
   *
   * <p>The facts here depend only on the SQL. Whether a call opens or closes a transaction also
   * depends on the method name and on the connection's autocommit setting, so that stays in
   * {@link SqlMethodAnalyzer#doesOpenTransaction} and
   * {@link SqlMethodAnalyzer#doesCloseTransaction}, which take an analysis to work from.
   */
  public static final class SqlStatementAnalysis {

    private final boolean present;
    private final boolean startsTransaction;
    private final boolean closesTransaction;
    private final boolean dml;
    private final boolean settingAutoCommit;
    private final @Nullable Boolean autoCommitValue;

    private SqlStatementAnalysis(
        final boolean present,
        final boolean startsTransaction,
        final boolean closesTransaction,
        final boolean dml,
        final boolean settingAutoCommit,
        final @Nullable Boolean autoCommitValue) {
      this.present = present;
      this.startsTransaction = startsTransaction;
      this.closesTransaction = closesTransaction;
      this.dml = dml;
      this.settingAutoCommit = settingAutoCommit;
      this.autoCommitValue = autoCommitValue;
    }

    /** Whether the call carried a SQL argument at all. */
    public boolean isPresent() {
      return this.present;
    }

    public boolean startsTransaction() {
      return this.startsTransaction;
    }

    public boolean closesTransaction() {
      return this.closesTransaction;
    }

    public boolean isDml() {
      return this.dml;
    }

    /** Whether the statement is a {@code SET AUTOCOMMIT} of either value. */
    public boolean isSettingAutoCommit() {
      return this.settingAutoCommit;
    }

    /**
     * The value a {@code SET AUTOCOMMIT} statement assigns, or {@code null} if the statement is not
     * one or its value could not be read.
     */
    public @Nullable Boolean getAutoCommitValue() {
      return this.autoCommitValue;
    }
  }

  /** The analysis of a call that carried no SQL, for example {@code Connection.commit}. */
  public static final SqlStatementAnalysis NO_STATEMENT =
      new SqlStatementAnalysis(false, false, false, false, false, null);

  /**
   * Analyzes the SQL of a JDBC call once, so the result can be handed to every question about it.
   *
   * @param args the JDBC method arguments, whose first element is taken as the SQL.
   * @return the analysis, or {@link #NO_STATEMENT} if the call carried no SQL.
   */
  public SqlStatementAnalysis analyze(final @Nullable Object[] args) {
    if (args == null || args.length < 1) {
      return NO_STATEMENT;
    }
    return this.analyze(String.valueOf(args[0]));
  }

  /**
   * Analyzes one SQL string once, so the result can be handed to every question about it.
   *
   * @param sql the SQL to analyze.
   * @return the analysis of {@code sql}.
   */
  public SqlStatementAnalysis analyze(final String sql) {
    final String statement = getFirstSqlStatement(sql);
    final boolean startsTransaction = isStatementStartingTransaction(statement);
    final boolean closesTransaction = isStatementClosingTransaction(statement);
    final boolean settingAutoCommit = statement.startsWith(SET_AUTOCOMMIT_PREFIX);
    return new SqlStatementAnalysis(
        true,
        startsTransaction,
        closesTransaction,
        !startsTransaction
            && !closesTransaction
            && !statement.startsWith("SET ")
            && !statement.startsWith("USE ")
            && !statement.startsWith("SHOW "),
        settingAutoCommit,
        settingAutoCommit ? readAutoCommitValue(statement) : null);
  }

  public boolean doesOpenTransaction(final Connection conn, final String methodName,
      final @Nullable Object[] args) {
    return this.doesOpenTransaction(conn, methodName, args, this.analyze(args));
  }

  /**
   * As {@link #doesOpenTransaction(Connection, String, Object[])}, working from an analysis the
   * caller has already made of the same SQL.
   */
  public boolean doesOpenTransaction(final Connection conn, final String methodName,
      final @Nullable Object[] args, final SqlStatementAnalysis analysis) {
    if (!(EXECUTE_SQL_METHOD_NAMES.contains(methodName) && args != null && args.length >= 1)) {
      return false;
    }

    if (analysis.startsTransaction()) {
      return true;
    }

    final boolean autocommit;
    try {
      autocommit = conn.getAutoCommit();
    } catch (final SQLException e) {
      return false;
    }

    return !autocommit && analysis.isDml();
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
  public static String stripComments(final String sql) {
    return stripCommentsInternal(sql, false);
  }

  /**
   * Removes SQL comments while supporting nested block comments, as required by PostgreSQL.
   */
  public static String stripCommentsWithNestedBlockComments(final String sql) {
    return stripCommentsInternal(sql, true);
  }

  private static String stripCommentsInternal(
      final String sql,
      final boolean supportsNestedBlockComments) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }

    if (!mayContainComment(sql)) {
      // Nothing to strip, so the scan below would copy the query character by character into an
      // identical string. Most queries take this path.
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
        final int end = skipBlockComment(sql, i, supportsNestedBlockComments);
        if (end < 0) {
          result.append(sql, i, length);
          i = length;
        } else {
          i = end;
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
   * Whether the query holds one of the three comment openers the stripping scan reacts to. That
   * scan allocates a builder and copies every character, so ruling it out keeps the common case of
   * a query with no comment in it allocation free.
   *
   * <p>All three openers have to be looked for, not just {@code /*}: the scan strips {@code --} and
   * {@code #} line comments as well, and missing one of those would leave a comment in front of the
   * leading keyword, hiding a transaction-control statement from every caller.
   *
   * <p>Quoted sections need no check of their own. The scan copies them verbatim, so a query
   * carrying quotes but no opener comes back unchanged anyway, and one carrying an opener inside a
   * quoted section fails this check and reaches the full scan, which preserves it.
   */
  private static boolean mayContainComment(final String sql) {
    final int lastIndex = sql.length() - 1;
    for (int i = 0; i <= lastIndex; i++) {
      final char c = sql.charAt(i);
      if (c == '#') {
        return true;
      }
      if (i < lastIndex
          && ((c == '-' && sql.charAt(i + 1) == '-')
              || (c == '/' && sql.charAt(i + 1) == '*'))) {
        return true;
      }
    }
    return false;
  }

  private static int skipBlockComment(
      final String sql,
      final int start,
      final boolean supportsNestedBlockComments) {
    int depth = 1;

    for (int i = start + 2; i + 1 < sql.length(); i++) {
      if (supportsNestedBlockComments
          && sql.charAt(i) == '/'
          && sql.charAt(i + 1) == '*') {
        depth++;
        i++;
      } else if (sql.charAt(i) == '*'
          && sql.charAt(i + 1) == '/') {
        depth--;
        i++;
        if (depth == 0) {
          return i + 1;
        }
      }
    }

    return -1;
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
    return this.doesCloseTransaction(conn, methodName, args, this.analyze(args));
  }

  /**
   * As {@link #doesCloseTransaction(Connection, String, Object[])}, working from an analysis the
   * caller has already made of the same SQL.
   */
  public boolean doesCloseTransaction(final Connection conn, final String methodName,
      final @Nullable Object[] args, final SqlStatementAnalysis analysis) {
    if (CLOSE_TRANSACTION_METHOD_NAMES.contains(methodName)) {
      return true;
    }

    if (doesSwitchAutoCommitFalseTrue(conn, methodName, args, analysis)) {
      return true;
    }

    if (!(methodName.contains("execute") && args != null && args.length >= 1)) {
      return false;
    }

    return analysis.closesTransaction();
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
    return this.isStatementSettingAutoCommit(methodName, this.analyze(args));
  }

  /**
   * As {@link #isStatementSettingAutoCommit(String, Object[])}, working from an analysis the caller
   * has already made of the same SQL.
   */
  public boolean isStatementSettingAutoCommit(
      final String methodName, final SqlStatementAnalysis analysis) {
    return analysis.isPresent()
        && EXECUTE_SQL_METHOD_NAMES.contains(methodName)
        && analysis.isSettingAutoCommit();
  }

  public boolean doesSwitchAutoCommitFalseTrue(final Connection conn, final String methodName,
      final @Nullable Object[] jdbcMethodArgs) {
    return this.doesSwitchAutoCommitFalseTrue(
        conn, methodName, jdbcMethodArgs, this.analyze(jdbcMethodArgs));
  }

  /**
   * As {@link #doesSwitchAutoCommitFalseTrue(Connection, String, Object[])}, working from an
   * analysis the caller has already made of the same SQL.
   */
  public boolean doesSwitchAutoCommitFalseTrue(final Connection conn, final String methodName,
      final @Nullable Object[] jdbcMethodArgs, final SqlStatementAnalysis analysis) {
    final boolean isStatementSettingAutoCommit = isStatementSettingAutoCommit(methodName, analysis);
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
      newAutoCommitVal = analysis.getAutoCommitValue();
    }

    return !oldAutoCommitVal && Boolean.TRUE.equals(newAutoCommitVal);
  }

  public @Nullable Boolean getAutoCommitValueFromSqlStatement(final @Nullable Object[] args) {
    if (args == null || args.length < 1) {
      return null;
    }

    return readAutoCommitValue(getFirstSqlStatement(String.valueOf(args[0])));
  }

  /** Reads the assigned value out of a statement already normalized by getFirstSqlStatement. */
  private static @Nullable Boolean readAutoCommitValue(final String statement) {
    String sql = statement;

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
