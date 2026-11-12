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

package software.amazon.jdbc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import net.sf.jsqlparser.statement.insert.Insert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.parser.ParameterColumnMapper.ParameterMapping;
import software.amazon.jdbc.parser.ParameterColumnMapper.UnboundColumn;
import software.amazon.jdbc.parser.ParameterMappingProblem.Type;
import software.amazon.jdbc.parser.PlaceholderScanner.Mode;

class ParameterColumnMapperTest {

  private static ParameterMapping map(final String sql) {
    return map(sql, Mode.GENERIC);
  }

  private static ParameterMapping map(final String sql, final Mode mode) {
    return ParameterColumnMapper.map(JSQLParserAnalyzer.parse(sql), PlaceholderScanner.scan(sql, mode));
  }

  private static Map<Integer, String> mapping(final Object... indexAndColumn) {
    final Map<Integer, String> result = new HashMap<>();
    for (int i = 0; i < indexAndColumn.length; i += 2) {
      result.put((Integer) indexAndColumn[i], (String) indexAndColumn[i + 1]);
    }
    return result;
  }

  /** Variant A rows without mapping problems: id, SQL, expected mapping. */
  static Stream<Arguments> cleanRows() {
    return Stream.of(
        Arguments.of("A1", "INSERT INTO users (name, ssn) VALUES (?, ?)",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A2", "INSERT INTO users (name, ssn) VALUES ('literal-name', ?)",
            mapping(1, "users.ssn")),
        Arguments.of("A3", "UPDATE users SET name = 'x', ssn = ? WHERE id = ?",
            mapping(1, "users.ssn")),
        Arguments.of("A4", "UPDATE users SET ssn = ? WHERE id = ?",
            mapping(1, "users.ssn")),
        Arguments.of("A5", "UPDATE users SET ssn = ?, name = 'x' WHERE id = ?",
            mapping(1, "users.ssn")),
        Arguments.of("A6", "INSERT INTO users (name, ssn) VALUES (?, ?), (?, ?)",
            mapping(1, "users.name", 2, "users.ssn", 3, "users.name", 4, "users.ssn")),
        Arguments.of("A7a", "INSERT INTO users (name, ssn) SELECT ?, ?",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A7b", "INSERT INTO users (name, ssn) SELECT name, ssn FROM staging WHERE id = ?",
            mapping()),
        Arguments.of("A7c", "INSERT INTO users (name, ssn) SELECT 'n', ? FROM staging WHERE id = ?",
            mapping(1, "users.ssn")),
        Arguments.of("A11",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = ?",
            mapping(1, "users.id", 2, "users.ssn", 3, "users.ssn")),
        Arguments.of("A12",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON DUPLICATE KEY UPDATE ssn = ?",
            mapping(1, "users.id", 2, "users.ssn", 3, "users.ssn")),
        Arguments.of("A13", "UPDATE users SET name = ?, ssn = ? WHERE id = ?",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A14", "INSERT INTO users (name, ssn) VALUES (NULL, ?)",
            mapping(1, "users.ssn")),
        Arguments.of("A15", "INSERT INTO users (id, ssn) VALUES (DEFAULT, ?)",
            mapping(1, "users.ssn")),
        Arguments.of("A16", "INSERT INTO users (\"name\", \"ssn\") VALUES (?, ?)",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A17", "INSERT INTO users (name, ssn) VALUES (?, ?) RETURNING id",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A18", "INSERT INTO users (ssn, name) VALUES (?, 'x')",
            mapping(1, "users.ssn")),
        Arguments.of("A19", "UPDATE users SET (name, ssn) = (?, ?) WHERE id = ?",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("A20", "INSERT INTO users (created, ssn) VALUES (now(), ?)",
            mapping(1, "users.ssn")),
        Arguments.of("A21", "UPDATE users SET ver = ver + 1, ssn = ? WHERE id = ?",
            mapping(1, "users.ssn")),
        Arguments.of("A22", "INSERT INTO public.users (name, ssn) VALUES ('n', ?)",
            mapping(1, "users.ssn")),
        Arguments.of("A23", "SELECT * FROM users WHERE name = 'x' AND ssn = ?",
            mapping(1, "ssn")),
        Arguments.of("cast", "INSERT INTO users (name, ssn) VALUES (?, CAST(? AS bytea))",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("pgCast", "INSERT INTO users (name, ssn) VALUES (?, ?::bytea)",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("paren", "INSERT INTO users (name, ssn) VALUES (?, (?))",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("union", "INSERT INTO users (name, ssn) SELECT ?, ? UNION ALL SELECT ?, ?",
            mapping(1, "users.name", 2, "users.ssn", 3, "users.name", 4, "users.ssn")),
        Arguments.of("parenthesedSelect", "INSERT INTO users (name, ssn) (SELECT ?, ?)",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("insertSet", "INSERT INTO users SET name = ?, ssn = ?",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("singleColumn", "INSERT INTO users (ssn) VALUES (?)",
            mapping(1, "users.ssn")),
        Arguments.of("singleColumnMultiRow", "INSERT INTO users (ssn) VALUES (?), (?)",
            mapping(1, "users.ssn", 2, "users.ssn")),
        Arguments.of("singleColumnDoubleParen", "INSERT INTO users (ssn) VALUES ((?))",
            mapping(1, "users.ssn")),
        Arguments.of("backticks", "INSERT INTO `users` (`name`, `ssn`) VALUES (?, ?)",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("conflictRowSet",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET (name, ssn) = (?, ?)",
            mapping(1, "users.id", 2, "users.ssn", 3, "users.name", 4, "users.ssn")),
        Arguments.of("conflictExcluded",
            "INSERT INTO users (name, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = EXCLUDED.ssn",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("duplicateValues",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON DUPLICATE KEY UPDATE ssn = VALUES(ssn)",
            mapping(1, "users.id", 2, "users.ssn")),
        Arguments.of("conflictDoNothing",
            "INSERT INTO users (name, ssn) VALUES (?, ?) ON CONFLICT (id) DO NOTHING",
            mapping(1, "users.name", 2, "users.ssn")),
        Arguments.of("delete", "DELETE FROM users WHERE ssn = ?", mapping()),
        Arguments.of("selectReversed", "SELECT * FROM users WHERE ? = ssn AND id IN (?, ?)",
            mapping(1, "ssn")),
        Arguments.of("comments",
            "/* why? */ UPDATE users SET note = 'really?', ssn = ? WHERE id = ? -- what?",
            mapping(1, "users.ssn")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cleanRows")
  void testMappingWithoutProblems(final String id, final String sql, final Map<Integer, String> expected) {
    final ParameterMapping result = map(sql);

    assertEquals(expected, result.getColumns(), id);
    assertTrue(result.getProblems().isEmpty(), id + ": " + result.getProblems());
  }

  @Test
  void testWriteFlagAndTargetTable() {
    final ParameterMapping insert = map("INSERT INTO public.users (name, ssn) VALUES ('n', ?)");
    assertTrue(insert.isWriteStatement());
    assertEquals("users", insert.getTargetTable());
    assertEquals(1, insert.getAstParameterCount());

    final ParameterMapping backticks = map("INSERT INTO `users` (`name`, `ssn`) VALUES (?, ?)");
    assertEquals("users", backticks.getTargetTable());

    final ParameterMapping select = map("SELECT * FROM users WHERE ssn = ?");
    assertFalse(select.isWriteStatement());
    assertNull(select.getTargetTable());

    final ParameterMapping delete = map("DELETE FROM users WHERE ssn = ?");
    assertFalse(delete.isWriteStatement());
  }

  @Test
  void testBareColumns() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES ('n', ?)");
    assertEquals(mapping(1, "ssn"), result.getBareColumns());
  }

  @Test
  void testA8_paramInsideFunction() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES (lower(?), ?)");

    assertEquals(mapping(2, "users.ssn"), result.getColumns());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.PARAMS_IN_EXPRESSION, "users",
        Collections.singletonList("name"), Collections.singletonList(1));
  }

  @Test
  void testA9_paramsInsideConcat() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES (concat(?, ?), ?)");

    assertEquals(mapping(3, "users.ssn"), result.getColumns());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.PARAMS_IN_EXPRESSION, "users",
        Collections.singletonList("name"), Arrays.asList(1, 2));
  }

  @Test
  void testA10_insertWithoutColumnList() {
    final ParameterMapping result = map("INSERT INTO users VALUES (?, ?, ?)");

    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.NO_COLUMN_LIST, "users",
        Collections.emptyList(), Arrays.asList(1, 2, 3));
  }

  @Test
  void testRowSubSelectHasUnknownTargets() {
    final ParameterMapping result =
        map("UPDATE users SET (name, ssn) = (SELECT a, b FROM t WHERE id = ?) WHERE id = ?");

    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.UNKNOWN_TARGET_PARAMS, "users",
        Arrays.asList("name", "ssn"), Collections.singletonList(1));
  }

  @Test
  void testScalarSubSelectIsParamsInExpression() {
    final ParameterMapping result =
        map("UPDATE users SET ssn = (SELECT s FROM t WHERE id = ?) WHERE id = ?");

    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.PARAMS_IN_EXPRESSION, "users",
        Collections.singletonList("ssn"), Collections.singletonList(1));
  }

  @Test
  void testRowArityMismatchHasUnknownTargets() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES (?, ?, ?)");

    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.UNKNOWN_TARGET_PARAMS, "users",
        Arrays.asList("name", "ssn"), Arrays.asList(1, 2, 3));
  }

  @Test
  void testFixedIndexParameters() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES (?1, ?2)");

    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.FIXED_INDEX_PARAMETER, "users",
        Collections.emptyList(), Arrays.asList(1, 2));
  }

  @Test
  void testReplaceIsUnsupported() {
    final ParameterMapping result = map("REPLACE INTO users (name, ssn) VALUES (?, ?)");

    assertTrue(result.isWriteStatement());
    assertEquals(1, result.getProblems().size());
    final ParameterMappingProblem problem = result.getProblems().get(0);
    assertProblem(problem, Type.UNSUPPORTED_STATEMENT, "users",
        Collections.emptyList(), Arrays.asList(1, 2));
    assertEquals("REPLACE", problem.getStatementKind());
  }

  @Test
  void testMergeIsUnsupported() {
    final ParameterMapping result = map(
        "MERGE INTO users u USING src s ON (u.id = s.id) WHEN MATCHED THEN UPDATE SET u.ssn = ?");

    assertTrue(result.isWriteStatement());
    assertEquals(1, result.getProblems().size());
    final ParameterMappingProblem problem = result.getProblems().get(0);
    assertProblem(problem, Type.UNSUPPORTED_STATEMENT, "users",
        Collections.emptyList(), Collections.singletonList(1));
    assertEquals("MERGE", problem.getStatementKind());
  }

  @Test
  void testUnparsedWriteStatement() {
    final String sql = "UPDATE users SET flag = (attrs ?? 'k'), ssn = ? WHERE id = ?";
    assertNull(JSQLParserAnalyzer.parse(sql));

    final ParameterMapping result = ParameterColumnMapper.map(null, PlaceholderScanner.scan(sql, Mode.GENERIC));

    assertTrue(result.isWriteStatement());
    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    final ParameterMappingProblem problem = result.getProblems().get(0);
    assertEquals(Type.UNPARSED_STATEMENT, problem.getType());
    assertNull(problem.getTable());
    assertTrue(problem.getCandidateTables().contains("users"));
    assertEquals(Arrays.asList(1, 2), problem.getParameterIndexes());
  }

  @Test
  void testUnparsedWriteStatementAfterLeadingComment() {
    final String sql = "-- note\nWITH x AS (SELECT 1) UPDATE users SET flag = (attrs ?? 'k'), ssn = ?";

    final ParameterMapping result = ParameterColumnMapper.map(null, PlaceholderScanner.scan(sql, Mode.GENERIC));

    assertEquals(1, result.getProblems().size());
    assertEquals(Type.UNPARSED_STATEMENT, result.getProblems().get(0).getType());
  }

  @Test
  void testUnparsedSelectHasNoProblems() {
    final String sql = "SELECT a ?? 'k' FROM users WHERE id = ?";

    final ParameterMapping result = ParameterColumnMapper.map(null, PlaceholderScanner.scan(sql, Mode.GENERIC));

    assertFalse(result.isWriteStatement());
    assertTrue(result.getColumns().isEmpty());
    assertTrue(result.getProblems().isEmpty());
  }

  @Test
  void testPlaceholderCountMismatch() {
    final String sql = "UPDATE users SET `what?` = 'x', ssn = ? WHERE id = ?";

    final ParameterMapping result = map(sql, Mode.POSTGRESQL);

    assertEquals(mapping(1, "users.ssn"), result.getColumns());
    assertEquals(1, result.getProblems().size());
    final ParameterMappingProblem problem = result.getProblems().get(0);
    assertProblem(problem, Type.PLACEHOLDER_COUNT_MISMATCH, "users",
        Collections.emptyList(), Arrays.asList(1, 2, 3));
    assertEquals(3, problem.getExpectedCount());
    assertEquals(2, problem.getActualCount());
  }

  @Test
  void testSameStatementWithMatchingModeHasNoMismatch() {
    final ParameterMapping result =
        map("UPDATE users SET `what?` = 'x', ssn = ? WHERE id = ?", Mode.MYSQL);

    assertEquals(mapping(1, "users.ssn"), result.getColumns());
    assertTrue(result.getProblems().isEmpty());
  }

  @Test
  void testUnexpectedShapeFailsClosedForWrites() {
    final Insert insert = mock(Insert.class);
    when(insert.getTable()).thenThrow(new IllegalStateException("boom"));

    final ParameterMapping result =
        ParameterColumnMapper.map(insert, PlaceholderScanner.scan("INSERT ? ?", Mode.GENERIC));

    assertTrue(result.isWriteStatement());
    assertTrue(result.getColumns().isEmpty());
    assertEquals(1, result.getProblems().size());
    assertProblem(result.getProblems().get(0), Type.UNKNOWN_TARGET_PARAMS, null,
        Collections.emptyList(), Arrays.asList(1, 2));
  }

  @Test
  void testProblemToStringHasNoSqlText() {
    final String sql = "UPDATE users SET flag = (attrs ?? 'secret-literal'), ssn = ? WHERE id = ?";
    final ParameterMapping result = ParameterColumnMapper.map(null, PlaceholderScanner.scan(sql, Mode.GENERIC));

    final String text = result.getProblems().get(0).toString();
    assertFalse(text.contains("secret-literal"));
    assertFalse(text.contains("UPDATE"));
  }

  // ---------------------------------------------------------------------------------------------
  // Values that are not bind parameters
  // ---------------------------------------------------------------------------------------------

  private static List<String> unbound(final ParameterMapping mapping) {
    final List<String> result = new ArrayList<>();
    for (UnboundColumn column : mapping.getUnboundColumns()) {
      result.add(column.getTable() + "." + column.getColumn() + ":" + column.getKind());
    }
    return result;
  }

  /** Client constants: certain to be stored as plaintext. */
  static Stream<Arguments> unboundRows() {
    return Stream.of(
        Arguments.of("literal", "INSERT INTO users (name, ssn) VALUES (?, '123')",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("negativeNumber", "INSERT INTO users (name, ssn) VALUES (?, -5)",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("boolean", "INSERT INTO users (name, ssn) VALUES (?, TRUE)",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("castLiteral", "INSERT INTO users (name, ssn) VALUES (?, 'x'::bytea)",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("dateLiteral", "INSERT INTO users (name, ssn) VALUES (?, DATE '2020-01-01')",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("function", "INSERT INTO users (name, ssn) VALUES (?, now())",
            Arrays.asList("users.ssn:EXPRESSION")),
        Arguments.of("concat", "INSERT INTO users (name, ssn) VALUES (?, 'a' || 'b')",
            Arrays.asList("users.ssn:EXPRESSION")),
        Arguments.of("multiRow", "INSERT INTO users (ssn) VALUES (?), ('2')",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("bothColumns", "INSERT INTO users (name, ssn) VALUES ('n', '1')",
            Arrays.asList("users.name:LITERAL", "users.ssn:LITERAL")),
        Arguments.of("updateLiteral", "UPDATE users SET ssn = 'x' WHERE id = ?",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("insertSet", "INSERT INTO users SET name = ?, ssn = 'x'",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("upsertLiteral",
            "INSERT INTO users (name, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = 'x'",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("insertSelectLiteral", "INSERT INTO users (name, ssn) SELECT ?, '123'",
            Arrays.asList("users.ssn:LITERAL")),
        Arguments.of("insertSelectLiteralFrom", "INSERT INTO users (name, ssn) SELECT 'n', ssn FROM staging",
            Arrays.asList("users.name:LITERAL")),
        Arguments.of("valuesRow", "INSERT INTO users (name, ssn) VALUES ROW('a', '1'), ROW('b', '2')",
            Arrays.asList("users.name:LITERAL", "users.ssn:LITERAL", "users.name:LITERAL",
                "users.ssn:LITERAL")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unboundRows")
  void testUnboundColumns(final String id, final String sql, final List<String> expected) {
    final ParameterMapping result = map(sql);

    assertEquals(expected, unbound(result), id);
    assertTrue(result.getProblems().isEmpty(), id + ": " + result.getProblems());
  }

  /** NULL, DEFAULT, and values read from a column or a subquery: left to the database. */
  static Stream<Arguments> databaseCheckedRows() {
    return Stream.of(
        Arguments.of("null", "INSERT INTO users (name, ssn) VALUES (?, NULL)"),
        Arguments.of("updateNull", "UPDATE users SET ssn = NULL WHERE id = ?"),
        Arguments.of("default", "INSERT INTO users (name, ssn) VALUES (?, DEFAULT)"),
        Arguments.of("updateDefault", "UPDATE users SET ssn = DEFAULT"),
        Arguments.of("columnReference", "UPDATE users SET ssn = other_col WHERE id = ?"),
        Arguments.of("expressionOfColumn", "UPDATE users SET ssn = upper(name)"),
        Arguments.of("scalarSubquery", "INSERT INTO users (name, ssn) VALUES (?, (SELECT ssn FROM staging))"),
        Arguments.of("rowSubquery", "UPDATE users SET (name, ssn) = (SELECT a, b FROM t)"),
        Arguments.of("conflictExcluded",
            "INSERT INTO users (name, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = EXCLUDED.ssn"),
        Arguments.of("conflictCoalesce",
            "INSERT INTO users (name, ssn) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET ssn = "
                + "COALESCE(EXCLUDED.ssn, users.ssn)"),
        Arguments.of("duplicateValues",
            "INSERT INTO users (id, ssn) VALUES (?, ?) ON DUPLICATE KEY UPDATE ssn = VALUES(ssn)"),
        Arguments.of("insertSelectColumns", "INSERT INTO users (name, ssn) SELECT name, ssn FROM staging"),
        Arguments.of("insertSelectFromCte",
            "WITH s AS (SELECT '123' AS ssn) INSERT INTO users (ssn) SELECT ssn FROM s"),
        Arguments.of("insertSelectUnion",
            "INSERT INTO users (ssn) SELECT ssn FROM a UNION ALL SELECT ssn FROM b"),
        Arguments.of("insertSelectStar", "INSERT INTO users SELECT * FROM staging"),
        Arguments.of("unpairedRow", "INSERT INTO users (name, ssn) VALUES ('a', '1', '2')"),
        Arguments.of("selectWithLiteral", "SELECT * FROM users WHERE ssn = 'x'"),
        Arguments.of("delete", "DELETE FROM users WHERE ssn = 'x'"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("databaseCheckedRows")
  void testNoUnboundColumnsOrProblems(final String id, final String sql) {
    final ParameterMapping result = map(sql);

    assertTrue(result.getUnboundColumns().isEmpty(), id + ": " + result.getUnboundColumns());
    assertTrue(result.getProblems().isEmpty(), id + ": " + result.getProblems());
  }

  /** Writes the plugin cannot read and that have no parameters to encrypt: left to the database. */
  static Stream<Arguments> unreadableWritesWithoutParameters() {
    return Stream.of(
        Arguments.of("noColumnList", "INSERT INTO users VALUES ('n', '123')"),
        Arguments.of("merge",
            "MERGE INTO users u USING src s ON (u.id = s.id) WHEN MATCHED THEN UPDATE SET u.ssn = s.ssn"),
        Arguments.of("replace", "REPLACE INTO users (name, ssn) VALUES ('n', '1')"),
        Arguments.of("dataModifyingCte",
            "WITH w AS (INSERT INTO users (ssn) VALUES ('1') RETURNING id) SELECT * FROM w"),
        Arguments.of("defaultValues", "INSERT INTO users DEFAULT VALUES"),
        Arguments.of("writeAfterSelect", "SELECT 1; INSERT INTO users (ssn) VALUES ('1')"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unreadableWritesWithoutParameters")
  void testUnreadableWriteWithoutParametersHasNoProblems(final String id, final String sql) {
    final ParameterMapping result = map(sql);

    assertTrue(result.isWriteStatement(), id);
    assertTrue(result.getProblems().isEmpty(), id + ": " + result.getProblems());
  }

  static Stream<Arguments> unreadableWritesWithParameters() {
    return Stream.of(
        Arguments.of("dataModifyingCte",
            "WITH w AS (INSERT INTO users (ssn) VALUES (?) RETURNING id) SELECT * FROM w"),
        Arguments.of("writeAfterSelect", "SELECT 1; INSERT INTO users (ssn) VALUES (?)"),
        Arguments.of("writeBeforeSelect", "INSERT INTO users (ssn) VALUES (?); SELECT 1"),
        Arguments.of("twoParsableWrites", "UPDATE other SET a = 1; UPDATE users SET ssn = ?"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unreadableWritesWithParameters")
  void testUnreadableWriteWithParametersIsReported(final String id, final String sql) {
    final ParameterMapping result = map(sql);

    assertTrue(result.isWriteStatement(), id);
    assertEquals(1, result.getProblems().size(), id);
    final ParameterMappingProblem problem = result.getProblems().get(0);
    assertEquals(Type.UNPARSED_STATEMENT, problem.getType(), id);
    assertTrue(problem.getCandidateTables().contains("users"), id + ": " + problem.getCandidateTables());
    assertEquals(Collections.singletonList(1), problem.getParameterIndexes(), id);
  }

  static Stream<Arguments> unparsedReads() {
    return Stream.of(
        Arguments.of("copyFrom", "COPY users (ssn) FROM STDIN"),
        Arguments.of("copyTo", "COPY users TO STDOUT"),
        Arguments.of("loadData", "LOAD DATA LOCAL INFILE '/tmp/u.csv' INTO TABLE users (name, ssn)"),
        Arguments.of("twoSelects", "SELECT * FROM users WHERE id = ?; SELECT 2"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unparsedReads")
  void testOtherUnparsedStatementsHaveNoProblems(final String id, final String sql) {
    final ParameterMapping result = map(sql);

    assertTrue(result.getProblems().isEmpty(), id + ": " + result.getProblems());
  }

  @Test
  void testTrailingSemicolonIsStillMapped() {
    final ParameterMapping result = map("INSERT INTO users (name, ssn) VALUES (?, ?);");

    assertEquals(mapping(1, "users.name", 2, "users.ssn"), result.getColumns());
    assertTrue(result.getProblems().isEmpty());
  }

  @Test
  void testValuesRowConstructorMapsParameters() {
    final ParameterMapping bound = map("INSERT INTO users (name, ssn) VALUES ROW(?, ?)", Mode.MYSQL);

    assertEquals(mapping(1, "users.name", 2, "users.ssn"), bound.getColumns());
    assertTrue(bound.getUnboundColumns().isEmpty());
    assertTrue(bound.getProblems().isEmpty(), bound.getProblems().toString());
  }

  static Stream<Arguments> multiTableUpdates() {
    return Stream.of(
        Arguments.of("join", "UPDATE other o JOIN users u ON o.id = u.id SET u.ssn = 'x'"),
        Arguments.of("comma", "UPDATE other o, users u SET u.ssn = 'x' WHERE o.id = u.id"),
        Arguments.of("tableName", "UPDATE other JOIN users ON other.id = users.id SET users.ssn = 'x'"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("multiTableUpdates")
  void testMultiTableUpdateAttributesQualifiedColumn(final String id, final String sql) {
    assertEquals(Arrays.asList("users.ssn:LITERAL"), unbound(map(sql, Mode.MYSQL)), id);
  }

  @Test
  void testMultiTableUpdateMapsQualifiedParameterToItsTable() {
    final ParameterMapping result =
        map("UPDATE other o JOIN users u ON o.id = u.id SET o.note = ?, u.ssn = ? WHERE o.id = ?", Mode.MYSQL);

    assertEquals(mapping(1, "other.note", 2, "users.ssn"), result.getColumns());
    assertTrue(result.getProblems().isEmpty(), result.getProblems().toString());
  }

  @Test
  void testMultiTableUpdateUnqualifiedColumnIsCheckedAgainstEveryTable() {
    final ParameterMapping literal =
        map("UPDATE other o JOIN users u ON o.id = u.id SET ssn = 'x'", Mode.MYSQL);
    assertEquals(Arrays.asList("other.ssn:LITERAL", "users.ssn:LITERAL"), unbound(literal));

    final ParameterMapping bound =
        map("UPDATE other o JOIN users u ON o.id = u.id SET ssn = ?", Mode.MYSQL);
    assertTrue(bound.getColumns().isEmpty());
    assertEquals(2, bound.getProblems().size());
    assertProblem(bound.getProblems().get(0), Type.UNKNOWN_TARGET_PARAMS, "other",
        Collections.singletonList("ssn"), Collections.singletonList(1));
    assertProblem(bound.getProblems().get(1), Type.UNKNOWN_TARGET_PARAMS, "users",
        Collections.singletonList("ssn"), Collections.singletonList(1));
  }

  @Test
  void testPostgresUpdateFromListIsNotATarget() {
    final ParameterMapping result = map("UPDATE users SET ssn = 'x' FROM staging s WHERE s.id = users.id");

    assertEquals(Arrays.asList("users.ssn:LITERAL"), unbound(result));
  }

  @Test
  void testUnboundColumnToStringHasNoSqlText() {
    final ParameterMapping result = map("INSERT INTO users (ssn) VALUES ('secret-literal')");

    final String text = result.getUnboundColumns().get(0).toString();
    assertFalse(text.contains("secret-literal"), text);
  }

  private static void assertProblem(
      final ParameterMappingProblem problem,
      final Type type,
      final String table,
      final List<String> columns,
      final List<Integer> indexes) {
    assertEquals(type, problem.getType(), problem.toString());
    assertEquals(table, problem.getTable(), problem.toString());
    assertEquals(columns, problem.getColumns(), problem.toString());
    assertEquals(indexes, problem.getParameterIndexes(), problem.toString());
  }
}
