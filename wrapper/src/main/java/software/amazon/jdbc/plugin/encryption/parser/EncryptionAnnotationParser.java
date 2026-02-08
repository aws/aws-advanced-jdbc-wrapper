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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses SQL encryption annotations from SQL statements.
 *
 * <p>Annotations format: comment with @encrypt:table.column followed by parameter placeholder.
 */
public final class EncryptionAnnotationParser {

  private static final Pattern ENCRYPT_ANNOTATION_PATTERN =
      Pattern.compile("/\\*@encrypt:([\\w.]+)\\*/\\s*\\?");

  private EncryptionAnnotationParser() {
    // Utility class
  }

  /**
   * Parse SQL for encryption annotations.
   *
   * @param sql SQL statement with potential annotations
   * @return Map of parameter index (1-based) to column identifier (table.column)
   */
  public static Map<Integer, String> parseAnnotations(String sql) {
    if (sql == null || sql.isEmpty()) {
      return new HashMap<>();
    }

    Map<Integer, String> encryptionMap = new HashMap<>();
    Matcher annotationMatcher = ENCRYPT_ANNOTATION_PATTERN.matcher(sql);
    
    // Find all annotations with their positions
    Map<Integer, String> annotationsByPosition = new HashMap<>();
    while (annotationMatcher.find()) {
      int questionMarkPos = annotationMatcher.end() - 1; // Position of the '?'
      annotationsByPosition.put(questionMarkPos, annotationMatcher.group(1));
    }
    
    // Count all '?' placeholders and map annotations to parameter indices
    int paramIndex = 1;
    for (int i = 0; i < sql.length(); i++) {
      if (sql.charAt(i) == '?') {
        if (annotationsByPosition.containsKey(i)) {
          encryptionMap.put(paramIndex, annotationsByPosition.get(i));
        }
        paramIndex++;
      }
    }

    return encryptionMap;
  }

  /**
   * Strip encryption annotations from SQL before sending to database.
   *
   * @param sql SQL statement with annotations
   * @return SQL with annotations removed
   */
  public static String stripAnnotations(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    return sql.replaceAll("/\\*@encrypt:[\\w.]+\\*/\\s*", "");
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
