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
import org.junit.jupiter.api.Test;

/** Tests encryption annotations with MySQL syntax. */
class MySqlAnnotationTest {

  @Test
  void testMySqlWithBackticksAndAnnotations() {
    String sql = "INSERT INTO `users` (`name`, `ssn`) VALUES (?, /*@encrypt:users.ssn*/ ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
  }

  @Test
  void testMySqlMultipleAnnotationsWithBackticks() {
    String sql =
        "INSERT INTO `users` (`ssn`, `credit_card`, `email`) VALUES "
            + "(/*@encrypt:users.ssn*/ ?, /*@encrypt:users.credit_card*/ ?, ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(2, annotations.size());
    assertEquals("users.ssn", annotations.get(1));
    assertEquals("users.credit_card", annotations.get(2));
  }

  @Test
  void testMySqlUpdateWithBackticksAndAnnotations() {
    String sql =
        "UPDATE `users` SET `name` = ?, `ssn` = /*@encrypt:users.ssn*/ ? WHERE `id` = ?";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
  }

  @Test
  void testMySqlStripAnnotationsWithBackticks() {
    String sql = "INSERT INTO `users` (`ssn`) VALUES (/*@encrypt:users.ssn*/ ?)";
    String clean = EncryptionAnnotationParser.stripAnnotations(sql);

    assertEquals("INSERT INTO `users` (`ssn`) VALUES (?)".trim(), clean.trim());
    assertFalse(clean.contains("@encrypt"));
  }

  @Test
  void testMySqlHasAnnotationsWithBackticks() {
    String sql = "INSERT INTO `users` (`ssn`) VALUES (/*@encrypt:users.ssn*/ ?)";
    assertTrue(EncryptionAnnotationParser.hasAnnotations(sql));

    String sqlNoAnnotation = "INSERT INTO `users` (`ssn`) VALUES (?)";
    assertFalse(EncryptionAnnotationParser.hasAnnotations(sqlNoAnnotation));
  }

  @Test
  void testMySqlComplexQueryWithAnnotations() {
    String sql =
        "INSERT INTO `orders` (`user_id`, `payment_info`, `total`) "
            + "VALUES (?, /*@encrypt:orders.payment_info*/ ?, ?)";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(1, annotations.size());
    assertEquals("orders.payment_info", annotations.get(2));

    String clean = EncryptionAnnotationParser.stripAnnotations(sql);
    assertFalse(clean.contains("@encrypt"));
    assertTrue(clean.contains("`orders`"));
    assertTrue(clean.contains("`payment_info`"));
  }

  @Test
  void testMySqlOnDuplicateKeyWithAnnotations() {
    String sql =
        "INSERT INTO `users` (`id`, `ssn`) VALUES (?, /*@encrypt:users.ssn*/ ?) "
            + "ON DUPLICATE KEY UPDATE `ssn` = /*@encrypt:users.ssn*/ ?";
    Map<Integer, String> annotations = EncryptionAnnotationParser.parseAnnotations(sql);

    assertEquals(2, annotations.size());
    assertEquals("users.ssn", annotations.get(2));
    assertEquals("users.ssn", annotations.get(3));
  }
}
