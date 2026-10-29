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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.jdbc.util.Messages;

/**
 * Parses SQL encryption annotations from SQL statements.
 *
 * <p>Annotations format: comment with @encrypt:table.column followed by parameter placeholder.
 *
 * <p>Only an annotation that starts a block comment outside string literals, quoted identifiers and
 * other comments counts. It applies to the real parameter placeholder that follows it, and the
 * parameter index is the position of that placeholder among all real placeholders of the statement
 * (see {@link PlaceholderScanner}).
 */
public final class EncryptionAnnotationParser {

  private static final Logger LOGGER = Logger.getLogger(EncryptionAnnotationParser.class.getName());

  private static final Pattern ENCRYPT_ANNOTATION_PATTERN =
      Pattern.compile("/\\*@encrypt:([\\w.]+)\\*/\\s*\\?");

  private static final Pattern STRIP_ANNOTATION_PATTERN =
      Pattern.compile("/\\*@encrypt:[\\w.]+\\*/\\s*");

  private EncryptionAnnotationParser() {
    // Utility class
  }

  /**
   * Parse SQL for encryption annotations using {@link PlaceholderScanner.Mode#GENERIC} lexical rules.
   *
   * @param sql SQL statement with potential annotations
   * @return Map of parameter index (1-based) to column identifier (table.column)
   */
  public static Map<Integer, String> parseAnnotations(String sql) {
    return parseAnnotations(sql, PlaceholderScanner.Mode.GENERIC);
  }

  /**
   * Parse SQL for encryption annotations.
   *
   * @param sql SQL statement with potential annotations
   * @param mode lexical rules of the target driver
   * @return Map of parameter index (1-based) to column identifier (table.column)
   */
  public static Map<Integer, String> parseAnnotations(String sql, PlaceholderScanner.Mode mode) {
    if (sql == null || sql.isEmpty()) {
      return new HashMap<>();
    }
    return parseAnnotations(sql, PlaceholderScanner.scan(sql, mode));
  }

  /**
   * Parse SQL for encryption annotations.
   *
   * @param sql SQL statement with potential annotations
   * @param scan result of {@link PlaceholderScanner#scan} for the same {@code sql}
   * @return Map of parameter index (1-based) to column identifier (table.column)
   */
  public static Map<Integer, String> parseAnnotations(String sql, PlaceholderScanner.Result scan) {
    final Map<Integer, String> encryptionMap = new HashMap<>();
    if (sql == null || sql.isEmpty()) {
      return encryptionMap;
    }

    final List<Integer> placeholderOffsets = scan.getPlaceholderOffsets();
    final Matcher annotationMatcher = ENCRYPT_ANNOTATION_PATTERN.matcher(sql);
    while (annotationMatcher.find()) {
      // Annotation-like text inside literals, quoted identifiers or other comments is ignored.
      if (!scan.isBlockCommentStart(annotationMatcher.start())) {
        continue;
      }
      // Matcher.group() is declared nullable; group 1 is a mandatory part of the pattern, so a
      // successful match always captures it. Skipping a null group keeps the map values non-null.
      final String columnIdentifier = annotationMatcher.group(1);
      if (columnIdentifier == null) {
        continue;
      }
      final int questionMarkPos = annotationMatcher.end() - 1;
      final int placeholderIndex = Collections.binarySearch(placeholderOffsets, questionMarkPos);
      if (placeholderIndex < 0) {
        LOGGER.warning(() -> Messages.get(
            "EncryptionAnnotationParser.annotationWithoutPlaceholder",
            new Object[] {columnIdentifier}));
        continue;
      }
      encryptionMap.put(placeholderIndex + 1, columnIdentifier);
    }

    return encryptionMap;
  }

  /**
   * Strips encryption annotations from a SQL statement using {@link PlaceholderScanner.Mode#GENERIC}
   * lexical rules.
   *
   * <p>The returned text is used for the wrapper's own parsing only. It is <b>not</b> the text sent
   * to the database: the plugin pipeline hands each JDBC method the arguments the application
   * supplied, and no stage substitutes this value for the original statement, so the annotated SQL is
   * what the target driver receives. Callers must not rely on this method to keep annotations out of
   * server-side logs.
   *
   * @param sql SQL statement with annotations
   * @return SQL with annotations removed
   */
  public static String stripAnnotations(String sql) {
    return stripAnnotations(sql, PlaceholderScanner.Mode.GENERIC);
  }

  /**
   * Strips encryption annotations from a SQL statement. See {@link #stripAnnotations(String)}.
   *
   * @param sql SQL statement with annotations
   * @param mode lexical rules of the target driver
   * @return SQL with annotations removed
   */
  public static String stripAnnotations(String sql, PlaceholderScanner.Mode mode) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    return stripAnnotations(sql, PlaceholderScanner.scan(sql, mode));
  }

  /**
   * Strips encryption annotations from a SQL statement. Only annotations that start a top-level
   * block comment are removed; string literals and other comments are left untouched. See
   * {@link #stripAnnotations(String)}.
   *
   * @param sql SQL statement with annotations
   * @param scan result of {@link PlaceholderScanner#scan} for the same {@code sql}
   * @return SQL with annotations removed
   */
  public static String stripAnnotations(String sql, PlaceholderScanner.Result scan) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    final Matcher matcher = STRIP_ANNOTATION_PATTERN.matcher(sql);
    final StringBuilder result = new StringBuilder(sql.length());
    int copiedUpTo = 0;
    boolean stripped = false;
    while (matcher.find()) {
      if (!scan.isBlockCommentStart(matcher.start())) {
        continue;
      }
      result.append(sql, copiedUpTo, matcher.start());
      copiedUpTo = matcher.end();
      stripped = true;
    }
    if (!stripped) {
      return sql;
    }
    result.append(sql, copiedUpTo, sql.length());
    return result.toString();
  }

  /**
   * Check if SQL contains any encryption annotations.
   *
   * @param sql SQL statement
   * @return true if annotations are present
   */
  public static boolean hasAnnotations(String sql) {
    if (sql == null || sql.isEmpty()) {
      return false;
    }
    return ENCRYPT_ANNOTATION_PATTERN.matcher(sql).find();
  }
}
