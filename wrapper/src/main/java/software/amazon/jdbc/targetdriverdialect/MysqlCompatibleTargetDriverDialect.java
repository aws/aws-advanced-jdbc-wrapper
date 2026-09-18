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
          + ")",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern EXECUTABLE_COMMENT_PATTERN =
      Pattern.compile("/\\*(?:!|M!)", Pattern.CASE_INSENSITIVE);

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
  public boolean mayChangeAuthorizationSessionState(final @Nullable String sql) {
    if (StringUtils.isNullOrEmpty(sql)) {
      return false;
    }

    if (EXECUTABLE_COMMENT_PATTERN.matcher(sql).find()) {
      return true;
    }

    final String sqlWithoutComments = SqlMethodAnalyzer.stripComments(sql);
    return AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find()
        || UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find();
  }

  @Override
  public boolean mayChangeUntrackedAuthorizationSessionState(final @Nullable String sql) {
    if (StringUtils.isNullOrEmpty(sql)) {
      return false;
    }

    if (EXECUTABLE_COMMENT_PATTERN.matcher(sql).find()) {
      return true;
    }

    final String sqlWithoutComments = SqlMethodAnalyzer.stripComments(sql);
    return UNTRACKED_AUTHORIZATION_STATE_STATEMENT_PATTERN.matcher(sqlWithoutComments).find();
  }
}
