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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.parser.PlaceholderScanner.Mode;

/** Tests for EncryptionAnnotationParser. */
public class EncryptionAnnotationParserTest {

  private static final String ANN = "/*@encrypt:users.ssn*/";

  @BeforeEach
  public void setUp() {
    // No setup needed for static methods
  }

  @Test
  public void testParseAnnotations_singleAnnotation() {
    String sql = "INSERT INTO users (name, ssn) VALUES (?, /*@encrypt:users.ssn*/ ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
  }

  @Test
  public void testParseAnnotations_multipleAnnotations() {
    String sql =
        "INSERT INTO users (ssn, credit_card, email) VALUES "
            + "(/*@encrypt:users.ssn*/ ?, /*@encrypt:users.credit_card*/ ?, ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(2, annotations.size());
    assertEquals("users.ssn", annotations.get(1));
    assertEquals("users.credit_card", annotations.get(2));
  }

  @Test
  public void testParseAnnotations_updateStatement() {
    String sql = "UPDATE users SET name = ?, ssn = /*@encrypt:users.ssn*/ ? WHERE id = ?";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
  }

  @Test
  public void testParseAnnotations_noAnnotations() {
    String sql = "INSERT INTO users (name, email) VALUES (?, ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertTrue(annotations.isEmpty());
  }

  @Test
  public void testParseAnnotations_nullSql() {
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(null);
    assertTrue(annotations.isEmpty());
  }

  @Test
  public void testParseAnnotations_emptySql() {
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations("");
    assertTrue(annotations.isEmpty());
  }

  @Test
  public void testStripAnnotations_singleAnnotation() {
    String sql = "INSERT INTO users (ssn) VALUES (/*@encrypt:users.ssn*/ ?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql);

    assertEquals("INSERT INTO users (ssn) VALUES (?)", clean);
  }

  @Test
  public void testStripAnnotations_multipleAnnotations() {
    String sql =
        "INSERT INTO users (ssn, cc) VALUES (/*@encrypt:users.ssn*/ ?, /*@encrypt:users.cc*/ ?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql);

    assertEquals("INSERT INTO users (ssn, cc) VALUES (?, ?)", clean);
  }

  @Test
  public void testStripAnnotations_withWhitespace() {
    String sql = "INSERT INTO users (ssn) VALUES (/*@encrypt:users.ssn*/   ?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql);

    assertEquals("INSERT INTO users (ssn) VALUES (?)", clean);
  }

  @Test
  public void testStripAnnotations_noAnnotations() {
    String sql = "INSERT INTO users (name) VALUES (?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql);

    assertEquals(sql, clean);
  }

  @Test
  public void testStripAnnotations_nullSql() {
    String clean = EncryptionAnnotationParser.stripAnnotations(null);
    assertEquals(null, clean);
  }

  @Test
  public void testHasAnnotations_withAnnotation() {
    String sql = "INSERT INTO users (ssn) VALUES (/*@encrypt:users.ssn*/ ?)";
    assertTrue(EncryptionAnnotationParser.hasAnnotations(sql));
  }

  @Test
  public void testHasAnnotations_withoutAnnotation() {
    String sql = "INSERT INTO users (name) VALUES (?)";
    assertFalse(EncryptionAnnotationParser.hasAnnotations(sql));
  }

  @Test
  public void testHasAnnotations_nullSql() {
    assertFalse(EncryptionAnnotationParser.hasAnnotations(null));
  }

  @Test
  public void testParseAnnotations_complexQuery() {
    String sql =
        "INSERT INTO orders (user_id, payment_info, amount) "
            + "SELECT id, /*@encrypt:orders.payment_info*/ ?, total FROM temp WHERE id = ?";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("orders.payment_info", annotations.get(1));
  }

  @Test
  public void testParseAnnotations_multilineQuery() {
    String sql =
        "INSERT INTO users (name, ssn, email)\n"
            + "VALUES (\n"
            + "  ?,\n"
            + "  /*@encrypt:users.ssn*/ ?,\n"
            + "  ?\n"
            + ")";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
  }

  @Test
  public void testParseAnnotations_underscoreInColumnName() {
    String sql = "INSERT INTO users (credit_card_number) VALUES (/*@encrypt:users.credit_card_number*/ ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.credit_card_number", annotations.get(1));
  }

  @Test
  public void testParseAnnotations_schemaQualified() {
    String sql = "INSERT INTO public.users (ssn) VALUES (/*@encrypt:public.users.ssn*/ ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("public.users.ssn", annotations.get(1));
  }

  /** Variant B rows: SQL, mode, expected parameter index of the annotation (null = no annotation). */
  static Stream<Arguments> variantBCases() {
    return Stream.of(
        Arguments.of("B1", "UPDATE users SET note = 'ok', ssn = " + ANN + " ? WHERE id = 1",
            Mode.GENERIC, 1),
        Arguments.of("B2", "UPDATE users SET note = 'really?', ssn = " + ANN + " ? WHERE id = 1",
            Mode.GENERIC, 1),
        Arguments.of("B3", "INSERT INTO users (note, ssn) VALUES ('it''s ok?', " + ANN + " ?)",
            Mode.GENERIC, 1),
        Arguments.of("B4", "INSERT INTO users (note, ssn) VALUES (E'a\\'?', " + ANN + " ?)",
            Mode.POSTGRESQL, 1),
        Arguments.of("B4", "INSERT INTO users (note, ssn) VALUES (E'a\\'?', " + ANN + " ?)",
            Mode.GENERIC, 1),
        Arguments.of("B5", "UPDATE users SET \"what?\" = 'x', ssn = " + ANN + " ? WHERE id = ?",
            Mode.POSTGRESQL, 1),
        Arguments.of("B6", "-- why?\nUPDATE users SET ssn = " + ANN + " ? WHERE id = ?",
            Mode.GENERIC, 1),
        Arguments.of("B7", "/* why? */ UPDATE users SET ssn = " + ANN + " ? WHERE id = ?",
            Mode.GENERIC, 1),
        Arguments.of("B8", "UPDATE users SET note = $$really?$$, ssn = " + ANN + " ? WHERE id = 1",
            Mode.POSTGRESQL, 1),
        Arguments.of("B9", "UPDATE users SET flag = (attrs ?? 'k'), ssn = " + ANN + " ? WHERE id = ?",
            Mode.GENERIC, 1),
        Arguments.of("B10", "UPDATE users SET name = ?, ssn = ? WHERE id = ? -- " + ANN + " ?",
            Mode.GENERIC, null),
        Arguments.of("B11", "INSERT INTO users (note, name, ssn) VALUES ('" + ANN + " ?', ?, ?)",
            Mode.GENERIC, null),
        Arguments.of("B12", "INSERT INTO users (note, ssn) VALUES ('a\\'?', " + ANN + " ?)",
            Mode.MYSQL, 1),
        Arguments.of("B13", "UPDATE users SET `what?` = 'x', ssn = " + ANN + " ? WHERE id = ?",
            Mode.MYSQL, 1),
        Arguments.of("B13", "UPDATE users SET `what?` = 'x', ssn = " + ANN + " ? WHERE id = ?",
            Mode.GENERIC, 1),
        // Backticks are not quotes in PostgreSQL, so the '?' inside them is a placeholder there.
        Arguments.of("B13", "UPDATE users SET `what?` = 'x', ssn = " + ANN + " ? WHERE id = ?",
            Mode.POSTGRESQL, 2),
        Arguments.of("B14", "INSERT INTO users (note, ssn) VALUES ('what?', " + ANN + " ?)",
            Mode.GENERIC, 1),
        Arguments.of("B15", "INSERT INTO users (name, ssn) VALUES ('n', " + ANN + " ?)",
            Mode.GENERIC, 1),
        // Annotation inside an outer (nested) PostgreSQL comment does not count.
        Arguments.of("nested", "UPDATE users SET a = 1 /* " + ANN + " ? */, ssn = ? WHERE id = 1",
            Mode.POSTGRESQL, null));
  }

  @ParameterizedTest(name = "{0} {2}")
  @MethodSource("variantBCases")
  public void testParseAnnotations_variantB(
      final String id, final String sql, final Mode mode, final Integer expectedIndex) {
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql, mode);

    if (expectedIndex == null) {
      assertTrue(annotations.isEmpty(), id + ": " + annotations);
    } else {
      assertEquals(Collections.singletonMap(expectedIndex, "users.ssn"), annotations, id);
    }
    assertEquals(annotations,
        EncryptionAnnotationParser.parseAnnotations(sql, PlaceholderScanner.scan(sql, mode)), id);
  }

  @Test
  public void testParseAnnotations_escapedQuestionMarkIsDroppedWithWarning() {
    String sql = "SELECT * FROM users WHERE attrs " + ANN + " ?? 'k' AND id = ?";
    Logger logger = Logger.getLogger(EncryptionAnnotationParser.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel() == Level.WARNING) {
          records.add(record);
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    logger.addHandler(handler);
    try {
      Map<Integer, String> annotations =
          EncryptionAnnotationParser.parseAnnotations(sql, Mode.GENERIC);
      assertTrue(annotations.isEmpty());
    } finally {
      logger.removeHandler(handler);
    }
    assertEquals(1, records.size());
    assertTrue(records.get(0).getMessage().contains("users.ssn"));
    assertFalse(records.get(0).getMessage().contains("SELECT"));
  }

  @Test
  public void testStripAnnotations_leavesLiteralsIntact() {
    String sql = "INSERT INTO users (note, ssn) VALUES ('" + ANN + " ?', " + ANN + " ?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql, Mode.GENERIC);

    assertEquals("INSERT INTO users (note, ssn) VALUES ('" + ANN + " ?', ?)", clean);
    assertEquals(clean, EncryptionAnnotationParser.stripAnnotations(sql));
  }

  @Test
  public void testStripAnnotations_leavesLineCommentsIntact() {
    String sql = "UPDATE users SET ssn = ? -- " + ANN + " ?";
    assertEquals(sql, EncryptionAnnotationParser.stripAnnotations(sql, Mode.GENERIC));
  }
}
