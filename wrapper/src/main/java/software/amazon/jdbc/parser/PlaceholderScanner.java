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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Lexical scanner that finds the JDBC parameter placeholders of a SQL statement the way the target
 * driver does.
 *
 * <p>A {@code ?} inside a string literal, a quoted identifier, a comment or a dollar-quoted string is
 * not a placeholder. The quoting and comment rules depend on the target driver, see {@link Mode}.
 * The rules mirror {@code org.postgresql.core.Parser} (PgJDBC) and MySQL Connector/J.
 */
public final class PlaceholderScanner {

  /** Lexical rules to apply. */
  public enum Mode {
    /**
     * PostgreSQL / PgJDBC: {@code "..."} is an identifier, backslash escapes only in {@code E'...'},
     * nested block comments, dollar quotes, {@code ??} is an escaped {@code ?} operator.
     */
    POSTGRESQL,
    /**
     * MySQL / MariaDB: {@code '...'} and {@code "..."} are strings with backslash escapes, backtick
     * identifiers, {@code #} and {@code -- } line comments, every unquoted {@code ?} is a placeholder.
     */
    MYSQL,
    /** Driver unknown: PostgreSQL rules, plus backtick-quoted identifiers. */
    GENERIC
  }

  private static final Result EMPTY_RESULT = new Result(
      Collections.<Integer>emptyList(), Collections.<int[]>emptyList(), Collections.<String>emptyList());

  private PlaceholderScanner() {
    // Utility class
  }

  /**
   * Scans a SQL statement.
   *
   * @param sql the SQL statement; null or empty yields an empty result
   * @param mode the lexical rules to apply
   * @return the placeholders, top-level block comments and top-level words of the statement
   */
  public static Result scan(final @Nullable String sql, final Mode mode) {
    if (sql == null || sql.isEmpty()) {
      return EMPTY_RESULT;
    }

    final List<Integer> placeholders = new ArrayList<>();
    final List<int[]> blockComments = new ArrayList<>();
    final List<String> words = new ArrayList<>();
    final int len = sql.length();
    int i = 0;

    while (i < len) {
      final char c = sql.charAt(i);
      final char next = i + 1 < len ? sql.charAt(i + 1) : '\0';

      if (c == '\'') {
        final boolean backslashEscapes = mode == Mode.MYSQL || isEscapeStringPrefix(sql, i);
        i = skipQuoted(sql, i, '\'', backslashEscapes, null);
      } else if (c == '"') {
        if (mode == Mode.MYSQL) {
          i = skipQuoted(sql, i, '"', true, null);
        } else {
          i = skipQuoted(sql, i, '"', false, words);
        }
      } else if (c == '`' && mode != Mode.POSTGRESQL) {
        i = skipQuoted(sql, i, '`', false, words);
      } else if (c == '-' && next == '-' && isLineCommentStart(sql, i, mode)) {
        i = skipLineComment(sql, i);
      } else if (c == '#' && mode == Mode.MYSQL) {
        i = skipLineComment(sql, i);
      } else if (c == '/' && next == '*') {
        final int end = skipBlockComment(sql, i, mode == Mode.POSTGRESQL);
        blockComments.add(new int[] {i, end});
        i = end;
      } else if (c == '$' && mode != Mode.MYSQL) {
        i = skipDollarQuote(sql, i);
      } else if (c == '?') {
        if (mode != Mode.MYSQL && next == '?') {
          // JDBC escape for the PostgreSQL '?' operator; not a placeholder.
          i += 2;
        } else {
          placeholders.add(i);
          i++;
        }
      } else if (Character.isLetter(c) || c == '_') {
        int end = i + 1;
        while (end < len && isIdentifierChar(sql.charAt(end))) {
          end++;
        }
        words.add(sql.substring(i, end));
        i = end;
      } else if (Character.isDigit(c)) {
        // Skip numeric tokens (and identifier characters glued to them) as a whole.
        int end = i + 1;
        while (end < len && isIdentifierChar(sql.charAt(end))) {
          end++;
        }
        i = end;
      } else {
        i++;
      }
    }

    return new Result(placeholders, blockComments, words);
  }

  private static boolean isIdentifierChar(final char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  /** True when the quote at {@code quotePos} starts a PostgreSQL {@code E'...'} escape string. */
  private static boolean isEscapeStringPrefix(final String sql, final int quotePos) {
    if (quotePos < 1) {
      return false;
    }
    final char prefix = sql.charAt(quotePos - 1);
    if (prefix != 'E' && prefix != 'e') {
      return false;
    }
    return quotePos == 1 || !isIdentifierChar(sql.charAt(quotePos - 2));
  }

  private static boolean isLineCommentStart(final String sql, final int pos, final Mode mode) {
    if (mode != Mode.MYSQL) {
      return true;
    }
    // MySQL requires whitespace or a control character after "--".
    if (pos + 2 >= sql.length()) {
      return true;
    }
    final char after = sql.charAt(pos + 2);
    return after <= ' ' || Character.isWhitespace(after) || Character.isISOControl(after);
  }

  /**
   * Skips a quoted token starting at {@code start}. A doubled quote character is an escaped quote.
   *
   * @return the offset after the closing quote, or the end of the SQL when unterminated
   */
  private static int skipQuoted(
      final String sql,
      final int start,
      final char quote,
      final boolean backslashEscapes,
      final @Nullable List<String> identifierSink) {
    final int len = sql.length();
    final @Nullable StringBuilder content = identifierSink == null ? null : new StringBuilder();
    int j = start + 1;
    while (j < len) {
      final char ch = sql.charAt(j);
      if (backslashEscapes && ch == '\\') {
        j += 2;
        continue;
      }
      if (ch == quote) {
        if (j + 1 < len && sql.charAt(j + 1) == quote) {
          if (content != null) {
            content.append(quote);
          }
          j += 2;
          continue;
        }
        j++;
        break;
      }
      if (content != null) {
        content.append(ch);
      }
      j++;
    }
    if (identifierSink != null && content != null) {
      identifierSink.add(content.toString());
    }
    return Math.min(j, len);
  }

  private static int skipLineComment(final String sql, final int start) {
    final int len = sql.length();
    int j = start;
    while (j < len && sql.charAt(j) != '\n' && sql.charAt(j) != '\r') {
      j++;
    }
    return j;
  }

  /** Returns the offset after the closing comment delimiter, or the end of the SQL. */
  private static int skipBlockComment(final String sql, final int start, final boolean nested) {
    final int len = sql.length();
    int depth = 1;
    int j = start + 2;
    while (j < len) {
      final char ch = sql.charAt(j);
      final char after = j + 1 < len ? sql.charAt(j + 1) : '\0';
      if (nested && ch == '/' && after == '*') {
        depth++;
        j += 2;
      } else if (ch == '*' && after == '/') {
        depth--;
        j += 2;
        if (depth == 0) {
          return j;
        }
      } else {
        j++;
      }
    }
    return len;
  }

  /**
   * Skips a dollar-quoted string ({@code $$...$$} or {@code $tag$...$tag$}) starting at {@code start}.
   * When the {@code $} does not start a dollar quote, returns {@code start + 1}.
   */
  private static int skipDollarQuote(final String sql, final int start) {
    final int len = sql.length();
    if (start > 0 && isIdentifierChar(sql.charAt(start - 1))) {
      return start + 1;
    }
    int tagEnd = -1;
    if (start + 1 < len && sql.charAt(start + 1) == '$') {
      tagEnd = start + 1;
    } else if (start + 1 < len
        && (Character.isLetter(sql.charAt(start + 1)) || sql.charAt(start + 1) == '_')) {
      for (int d = start + 2; d < len; d++) {
        final char ch = sql.charAt(d);
        if (ch == '$') {
          tagEnd = d;
          break;
        }
        if (!Character.isLetterOrDigit(ch) && ch != '_') {
          break;
        }
      }
    }
    if (tagEnd < 0) {
      return start + 1;
    }
    final String tag = sql.substring(start, tagEnd + 1);
    final int close = sql.indexOf(tag, tagEnd + 1);
    return close < 0 ? len : close + tag.length();
  }

  /** Result of a scan. All lists are unmodifiable. */
  public static final class Result {
    private final List<Integer> placeholderOffsets;
    private final List<int[]> blockComments;
    private final List<String> words;

    private Result(
        final List<Integer> placeholderOffsets,
        final List<int[]> blockComments,
        final List<String> words) {
      this.placeholderOffsets = Collections.unmodifiableList(placeholderOffsets);
      this.blockComments = Collections.unmodifiableList(blockComments);
      this.words = Collections.unmodifiableList(words);
    }

    /** Character offsets of the real placeholders, in ascending order. */
    public List<Integer> getPlaceholderOffsets() {
      return placeholderOffsets;
    }

    /**
     * Top-level block comments as {@code {start, endExclusive}} pairs, in ascending order. Comments
     * nested in another comment are not listed separately.
     */
    public List<int[]> getBlockComments() {
      return blockComments;
    }

    /**
     * Top-level unquoted words and the inner text of quoted identifiers, in order. Text inside string
     * literals and comments is not included.
     */
    public List<String> getWords() {
      return words;
    }

    /** Number of real placeholders. */
    public int getPlaceholderCount() {
      return placeholderOffsets.size();
    }

    /** True when a top-level block comment starts at {@code offset}. */
    public boolean isBlockCommentStart(final int offset) {
      int low = 0;
      int high = blockComments.size() - 1;
      while (low <= high) {
        final int mid = (low + high) >>> 1;
        final int start = blockComments.get(mid)[0];
        if (start < offset) {
          low = mid + 1;
        } else if (start > offset) {
          high = mid - 1;
        } else {
          return true;
        }
      }
      return false;
    }
  }
}
