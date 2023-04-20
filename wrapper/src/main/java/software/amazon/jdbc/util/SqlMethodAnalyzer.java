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
import java.util.List;
import java.util.stream.Collectors;

public class SqlMethodAnalyzer {

  public boolean doesOpenTransaction(final Connection conn, final String methodName,
      final Object[] args) {
    if (!(methodName.contains("execute") && args != null && args.length >= 1)) {
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
    String statement = parseMultiStatementQueries(sql).get(0);
    statement = statement.toUpperCase();
    statement = statement.replaceAll("\\s*/\\*(.*?)\\*/\\s*", " ").trim();
    return statement;
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
      final Object[] args) {
    if (methodName.equals("Connection.commit") || methodName.equals("Connection.rollback")
        || methodName.equals("Connection.close") || methodName.equals("Connection.abort")) {
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
        || statement.startsWith("ABORT");
  }

  public boolean isStatementSettingAutoCommit(final String methodName, final Object[] args) {
    if (!(methodName.contains("execute") && args != null && args.length >= 1)) {
      return false;
    }

    final String statement = getFirstSqlStatement(String.valueOf(args[0]));
    return statement.startsWith("SET AUTOCOMMIT");
  }

  public boolean doesSwitchAutoCommitFalseTrue(final Connection conn, final String methodName,
      final Object[] jdbcMethodArgs) {
    final boolean isStatementSettingAutoCommit = isStatementSettingAutoCommit(
        methodName, jdbcMethodArgs);
    if (!methodName.contains("setAutoCommit") && !isStatementSettingAutoCommit) {
      return false;
    }

    final boolean oldAutoCommitVal;
    Boolean newAutoCommitVal = null;
    try {
      oldAutoCommitVal = conn.getAutoCommit();
    } catch (final SQLException e) {
      return false;
    }

    if (methodName.contains("setAutoCommit") && jdbcMethodArgs.length > 0) {
      newAutoCommitVal = (Boolean) jdbcMethodArgs[0];
    } else if (isStatementSettingAutoCommit) {
      newAutoCommitVal = getAutoCommitValueFromSqlStatement(jdbcMethodArgs);
    }

    return !oldAutoCommitVal && Boolean.TRUE.equals(newAutoCommitVal);
  }

  public Boolean getAutoCommitValueFromSqlStatement(final Object[] args) {
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
    return methodName.endsWith(".close") || methodName.endsWith(".abort");
  }
}
