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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;
import software.amazon.jdbc.util.SqlMethodAnalyzer;
import software.amazon.jdbc.util.StringUtils;

abstract class MysqlCompatibleTargetDriverDialect extends GenericTargetDriverDialect {

  private static final int ER_PARSE_ERROR = 1064;
  private static final int ER_SP_DOES_NOT_EXIST = 1305;
  private static final String AUTHORIZATION_SESSION_STATE_QUERY =
      "SELECT USER(), CURRENT_USER(), CURRENT_ROLE(), DATABASE()";
  private static final String AUTHORIZATION_SESSION_STATE_WITHOUT_ROLE_QUERY =
      "SELECT USER(), CURRENT_USER(), DATABASE()";

  private static final Pattern AUTHORIZATION_STATE_STATEMENT_PATTERN = Pattern.compile(
      "(?:^|;)\\s*(?:SET\\s+ROLE\\b|USE\\b|RESET\\s+CONNECTION\\b)",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN = Pattern.compile(
      "(?:^|;)\\s*(?:"
          + "CALL\\b"
          + "|DO\\b"
          + "|SET\\s+@"
          + "|EXECUTE\\b"
          // HANDLER reads through a table handler opened earlier in the session, so the SQL text
          // does not identify the table or read position.
          + "|HANDLER\\b"
          + "|CREATE\\s+(?:OR\\s+REPLACE\\s+)?TEMPORARY\\s+TABLE\\b"
          + ")",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern EXECUTABLE_COMMENT_PATTERN =
      Pattern.compile("/\\*(?:!|M!)", Pattern.CASE_INSENSITIVE);

  private static final Pattern SESSION_DEPENDENT_FUNCTION_PATTERN = Pattern.compile(
      "\\b(?:LAST_INSERT_ID|FOUND_ROWS|ROW_COUNT)\\s*\\(",
      Pattern.CASE_INSENSITIVE);

  @Override
  public boolean supportsAuthorizationSessionState() {
    return true;
  }

  @Override
  public Optional<AuthorizationSessionState> readAuthorizationSessionState(
      final @NonNull Connection connection) throws SQLException {
    try {
      return readAuthorizationSessionState(connection, true);
    } catch (final SQLException exception) {
      if (!isCurrentRoleUnsupported(exception)) {
        throw exception;
      }
      return readAuthorizationSessionState(connection, false);
    }
  }

  private static Optional<AuthorizationSessionState> readAuthorizationSessionState(
      final Connection connection,
      final boolean includeActiveRoles) throws SQLException {
    final String query = includeActiveRoles
        ? AUTHORIZATION_SESSION_STATE_QUERY
        : AUTHORIZATION_SESSION_STATE_WITHOUT_ROLE_QUERY;
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      if (!resultSet.next()) {
        return Optional.empty();
      }

      final String sessionUser = resultSet.getString(1);
      final String currentUser = resultSet.getString(2);
      if (sessionUser == null || currentUser == null) {
        return Optional.empty();
      }

      final @Nullable String activeRoles = includeActiveRoles ? resultSet.getString(3) : null;
      final @Nullable String currentDatabase =
          resultSet.getString(includeActiveRoles ? 4 : 3);
      return Optional.of(new AuthorizationSessionState(
          sessionUser,
          currentUser,
          "",
          "",
          activeRoles == null ? "" : activeRoles,
          currentDatabase == null ? "" : currentDatabase));
    }
  }

  private static boolean isCurrentRoleUnsupported(final SQLException exception) {
    return exception.getErrorCode() == ER_SP_DOES_NOT_EXIST
        || exception.getErrorCode() == ER_PARSE_ERROR;
  }

  @Override
  public AuthorizationStateImpact getAuthorizationStateImpact(final @Nullable String sql) {
    if (StringUtils.isNullOrEmpty(sql)) {
      return AuthorizationStateImpact.NONE;
    }

    if (EXECUTABLE_COMMENT_PATTERN.matcher(sql).find()) {
      return AuthorizationStateImpact.UNTRACKED;
    }

    final String sqlWithoutComments = SqlMethodAnalyzer.stripComments(sql);
    if (UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find()
        || referencesUserVariable(sqlWithoutComments)) {
      return AuthorizationStateImpact.UNTRACKED;
    }

    if (AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find()) {
      return AuthorizationStateImpact.TRACKED;
    }

    if (SESSION_DEPENDENT_FUNCTION_PATTERN.matcher(sqlWithoutComments).find()) {
      return AuthorizationStateImpact.UNCACHEABLE;
    }

    return AuthorizationStateImpact.NONE;
  }

  /**
   * Returns whether the SQL references a user-defined variable such as {@code @tenant},
   * {@code @'tenant'}, or {@code @`tenant`} outside quoted text. Account names such as
   * {@code 'app'@'%'} or {@code app@localhost} and system variables such as {@code @@sql_mode} are
   * not user-defined variables.
   */
  static boolean referencesUserVariable(final String sql) {
    // Whether a backslash escapes a quote depends on the NO_BACKSLASH_ESCAPES SQL mode, which the
    // driver cannot observe. Report a reference if either interpretation contains one.
    return referencesUserVariable(sql, true) || referencesUserVariable(sql, false);
  }

  private static boolean referencesUserVariable(final String sql, final boolean backslashEscapes) {
    final int length = sql.length();
    // True when the previous character ends a name or quoted text, so a following '@' separates the
    // user and host parts of an account name rather than starting a variable.
    boolean previousEndsName = false;
    int i = 0;
    while (i < length) {
      final char c = sql.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(sql, i, backslashEscapes && c != '`');
        previousEndsName = true;
      } else if (c == '@') {
        if (i + 1 < length && sql.charAt(i + 1) == '@') {
          // System variable, for example @@session.sql_mode.
          i += 2;
        } else if (!previousEndsName && i + 1 < length && isUserVariableNameStart(sql.charAt(i + 1))) {
          return true;
        } else {
          i++;
        }
        previousEndsName = false;
      } else {
        previousEndsName = Character.isLetterOrDigit(c) || c == '_' || c == '$';
        i++;
      }
    }
    return false;
  }

  private static boolean isUserVariableNameStart(final char c) {
    return Character.isLetterOrDigit(c)
        || c == '_'
        || c == '$'
        || c == '.'
        || c == '\''
        || c == '"'
        || c == '`';
  }

  private static int skipQuoted(final String sql, final int start, final boolean backslashEscapes) {
    final char quote = sql.charAt(start);
    final int length = sql.length();
    int i = start + 1;
    while (i < length) {
      final char c = sql.charAt(i);
      if (backslashEscapes && c == '\\') {
        i += 2;
      } else if (c == quote) {
        if (i + 1 < length && sql.charAt(i + 1) == quote) {
          i += 2;
        } else {
          return i + 1;
        }
      } else {
        i++;
      }
    }
    return length;
  }
}
