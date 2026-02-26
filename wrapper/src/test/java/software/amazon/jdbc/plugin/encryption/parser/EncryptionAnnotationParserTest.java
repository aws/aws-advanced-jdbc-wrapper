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

package software.amazon.jdbc.plugin.encryption.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for EncryptionAnnotationParser. */
public class EncryptionAnnotationParserTest {

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
}
