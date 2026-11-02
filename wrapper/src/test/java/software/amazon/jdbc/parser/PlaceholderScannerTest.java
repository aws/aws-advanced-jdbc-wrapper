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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.core.NativeQuery;
import org.postgresql.core.Parser;
import software.amazon.jdbc.parser.PlaceholderScanner.Mode;

class PlaceholderScannerTest {

  /** SQL and the expected placeholder count per mode: POSTGRESQL, MYSQL, GENERIC. */
  private static final Object[][] CASES = {
      {"SELECT '?', ?", 1, 1, 1},
      {"SELECT 'it''s?', ?", 1, 1, 1},
      {"SELECT E'a\\'?', ?", 1, 1, 1},
      {"SELECT x, E'a\\'?', ?", 1, 1, 1},
      {"SELECT 'a\\'?', ?", 1, 1, 1},
      {"SELECT \"what?\", ?", 1, 1, 1},
      {"SELECT \"wh\"\"at?\", ?", 1, 1, 1},
      {"SELECT `what?`, ?", 2, 1, 1},
      {"SELECT 1 -- ?\n, ?", 1, 1, 1},
      {"SELECT 1 --?\n, ?", 1, 2, 1},
      {"SELECT 1 # ?\n, ?", 2, 1, 2},
      {"SELECT /* ? */ ?", 1, 1, 1},
      {"SELECT /* /* ? */ ? */ ?", 1, 2, 2},
      {"SELECT $$?$$, ?", 1, 2, 1},
      {"SELECT $tag$?$tag$, ?", 1, 2, 1},
      {"SELECT a$b$c ?", 1, 1, 1},
      {"SELECT a ?? 'k', ?", 1, 3, 1},
      {"SELECT a ??| b, ?", 1, 3, 1},
      {"SELECT a ??& b, ?", 1, 3, 1},
      {"SELECT ?, 'abc ?", 1, 1, 1},
      {"SELECT ? /* ? ", 1, 1, 1},
      {"SELECT ? -- ?", 1, 1, 1},
      {"SELECT ?, ?, ?", 3, 3, 3},
      {"SELECT 1", 0, 0, 0},
  };

  private static final Mode[] CASE_MODES = {Mode.POSTGRESQL, Mode.MYSQL, Mode.GENERIC};

  static Stream<Arguments> countCases() {
    final List<Arguments> arguments = new ArrayList<>();
    for (Object[] row : CASES) {
      for (int m = 0; m < CASE_MODES.length; m++) {
        arguments.add(Arguments.of(row[0], CASE_MODES[m], row[m + 1]));
      }
    }
    return arguments.stream();
  }

  static Stream<String> postgresCases() {
    final List<String> sqls = new ArrayList<>();
    for (Object[] row : CASES) {
      sqls.add((String) row[0]);
    }
    return sqls.stream();
  }

  @ParameterizedTest
  @MethodSource("countCases")
  void testPlaceholderCount(final String sql, final Mode mode, final int expected) {
    assertEquals(expected, PlaceholderScanner.scan(sql, mode).getPlaceholderCount());
  }

  @ParameterizedTest
  @MethodSource("postgresCases")
  void testPostgresModeAgreesWithPgJdbc(final String sql) throws Exception {
    final List<NativeQuery> queries = Parser.parseJdbcSql(sql, true, true, true, false, true);
    int pgJdbcCount = 0;
    for (NativeQuery query : queries) {
      pgJdbcCount += query.bindPositions.length;
    }
    assertEquals(pgJdbcCount, PlaceholderScanner.scan(sql, Mode.POSTGRESQL).getPlaceholderCount());
  }

  @Test
  void testBackslashOffsetsDifferByMode() {
    final String sql = "SELECT 'a\\'?', ?";
    // MySQL: 'a\'?' is one literal; the placeholder is the last character.
    assertEquals(Collections.singletonList(sql.length() - 1),
        PlaceholderScanner.scan(sql, Mode.MYSQL).getPlaceholderOffsets());
    // PostgreSQL: 'a\' ends at the second quote; the next '?' is a placeholder and ', ?' is an
    // unterminated literal.
    assertEquals(Collections.singletonList(sql.indexOf('?')),
        PlaceholderScanner.scan(sql, Mode.POSTGRESQL).getPlaceholderOffsets());
  }

  @Test
  void testNullAndEmpty() {
    for (Mode mode : Mode.values()) {
      assertEquals(0, PlaceholderScanner.scan(null, mode).getPlaceholderCount());
      assertEquals(0, PlaceholderScanner.scan("", mode).getPlaceholderCount());
      assertTrue(PlaceholderScanner.scan(null, mode).getWords().isEmpty());
      assertTrue(PlaceholderScanner.scan("", mode).getBlockComments().isEmpty());
    }
  }

  @Test
  void testBlockCommentSpans() {
    final String sql = "/* a */ SELECT /* b /* c */ d */ ?";
    final PlaceholderScanner.Result pg = PlaceholderScanner.scan(sql, Mode.POSTGRESQL);
    assertEquals(2, pg.getBlockComments().size());
    assertArrayEquals(new int[] {0, 7}, pg.getBlockComments().get(0));
    assertArrayEquals(new int[] {15, sql.lastIndexOf("*/") + 2}, pg.getBlockComments().get(1));
    assertTrue(pg.isBlockCommentStart(0));
    assertTrue(pg.isBlockCommentStart(15));
    assertFalse(pg.isBlockCommentStart(sql.indexOf("/* c")));
    assertEquals(1, pg.getPlaceholderCount());

    // Not nested: the comment ends at the first "*/", the nested start is not top-level either.
    final PlaceholderScanner.Result generic = PlaceholderScanner.scan(sql, Mode.GENERIC);
    assertEquals(2, generic.getBlockComments().size());
    assertArrayEquals(new int[] {15, sql.indexOf("*/", 15) + 2}, generic.getBlockComments().get(1));
    assertFalse(generic.isBlockCommentStart(sql.indexOf("/* c")));
  }

  @Test
  void testUnterminatedBlockCommentRunsToEnd() {
    final String sql = "SELECT ? /* ? ";
    final PlaceholderScanner.Result result = PlaceholderScanner.scan(sql, Mode.GENERIC);
    assertArrayEquals(new int[] {9, sql.length()}, result.getBlockComments().get(0));
  }

  @Test
  void testWords() {
    assertEquals(Arrays.asList("UPDATE", "Users", "SET"),
        PlaceholderScanner.scan("-- x\nUPDATE \"Users\" SET", Mode.POSTGRESQL).getWords());
    assertEquals(Arrays.asList("UPDATE", "Users", "SET"),
        PlaceholderScanner.scan("-- x\nUPDATE \"Users\" SET", Mode.GENERIC).getWords());
    assertEquals(Arrays.asList("UPDATE", "users", "SET", "a"),
        PlaceholderScanner.scan("UPDATE `users` SET a = 'x' /* t */", Mode.MYSQL).getWords());
    assertEquals(Arrays.asList("INSERT", "INTO", "t", "VALUES"),
        PlaceholderScanner.scan("INSERT INTO t VALUES ('lit', 12, ?)", Mode.GENERIC).getWords());
  }

  @Test
  void testMySqlDoubleQuotedStringIsNotAWord() {
    assertEquals(Arrays.asList("SELECT"),
        PlaceholderScanner.scan("SELECT \"users\"", Mode.MYSQL).getWords());
  }
}
