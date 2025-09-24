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
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import software.amazon.jdbc.JdbcMethod;

public class SqlMethodAnalyzer {

  private static final DSLContext dsl = DSL.using(SQLDialect.DEFAULT);

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
      final Object[] args) {
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
    List<String> statementList = parseMultiStatementQueries(sql);
    if (statementList.isEmpty()) {
      return sql;
    }
    String statement = statementList.get(0);
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

  private String getQueryTypeFromParseTree(final String sql) {
    try {
      final Query query = dsl.parser().parseQuery(sql);
      final String className = query.getClass().getSimpleName();
      
      if (className.contains("Select")) return "SELECT";
      if (className.contains("Insert")) return "INSERT";
      if (className.contains("Update")) return "UPDATE";
      if (className.contains("Delete")) return "DELETE";
      if (className.contains("Create")) return "CREATE";
      if (className.contains("Drop")) return "DROP";
      if (className.contains("Alter")) return "ALTER";
      
      return "UNKNOWN";
    } catch (Exception e) {
      // Fallback to string parsing if jOOQ fails
      return getQueryTypeFromString(sql);
    }
  }

  private String getQueryTypeFromString(final String sql) {
    final String trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("SELECT")) return "SELECT";
    if (trimmed.startsWith("INSERT")) return "INSERT";
    if (trimmed.startsWith("UPDATE")) return "UPDATE";
    if (trimmed.startsWith("DELETE")) return "DELETE";
    if (trimmed.startsWith("CREATE")) return "CREATE";
    if (trimmed.startsWith("DROP")) return "DROP";
    if (trimmed.startsWith("ALTER")) return "ALTER";
    if (trimmed.startsWith("BEGIN") || trimmed.startsWith("START TRANSACTION")) return "BEGIN";
    if (trimmed.startsWith("COMMIT")) return "COMMIT";
    if (trimmed.startsWith("ROLLBACK")) return "ROLLBACK";
    if (trimmed.startsWith("SET")) return "SET";
    if (trimmed.startsWith("USE")) return "USE";
    if (trimmed.startsWith("SHOW")) return "SHOW";
    return "UNKNOWN";
  }

  public boolean isStatementDml(final String statement) {
    final String queryType = getQueryTypeFromParseTree(statement);
    return "SELECT".equals(queryType) || "INSERT".equals(queryType) || 
           "UPDATE".equals(queryType) || "DELETE".equals(queryType);
  }

  public boolean isStatementStartingTransaction(final String statement) {
    final String queryType = getQueryTypeFromParseTree(statement);
    return "BEGIN".equals(queryType);
  }

  public boolean isStatementClosingTransaction(final String statement) {
    final String queryType = getQueryTypeFromParseTree(statement);
    return "COMMIT".equals(queryType) || "ROLLBACK".equals(queryType);
  }

  public boolean isStatementSettingAutoCommit(final String methodName, final Object[] args) {
    if (args == null || args.length < 1) {
      return false;
    }

    if (!EXECUTE_SQL_METHOD_NAMES.contains(methodName)) {
      return false;
    }

    final String statement = getFirstSqlStatement(String.valueOf(args[0]));
    final String queryType = getQueryTypeFromParseTree(statement);
    return "SET".equals(queryType) && statement.toUpperCase().contains("AUTOCOMMIT");
  }

  public boolean doesSwitchAutoCommitFalseTrue(final Connection conn, final String methodName,
      final Object[] jdbcMethodArgs) {
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
    return CLOSING_METHOD_NAMES.contains(methodName);
  }
}
