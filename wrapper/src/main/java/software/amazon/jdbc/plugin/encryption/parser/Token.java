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

/** Represents a SQL token with type and value. */
public class Token {
  public enum Type {
    // Literals
    IDENT,
    SCONST,
    ICONST,
    FCONST,
    PLACEHOLDER,

    // Keywords
    SELECT,
    FROM,
    WHERE,
    INSERT,
    INTO,
    UPDATE,
    DELETE,
    CREATE,
    DROP,
    ALTER,
    TABLE,
    INDEX,
    DATABASE,
    SCHEMA,
    VIEW,
    FUNCTION,
    PROCEDURE,
    AND,
    OR,
    NOT,
    NULL,
    TRUE,
    FALSE,
    AS,
    ON,
    IN,
    EXISTS,
    BETWEEN,
    LIKE,
    IS,
    ISNULL,
    NOTNULL,
    ORDER,
    BY,
    GROUP,
    HAVING,
    LIMIT,
    OFFSET,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    JOIN,
    CROSS,
    UNION,
    INTERSECT,
    EXCEPT,
    ALL,
    DISTINCT,
    VALUES,
    SET,
    PRIMARY,
    KEY,
    FOREIGN,
    REFERENCES,
    CASE,
    WHEN,
    THEN,
    ELSE,
    END,
    CAST,
    RETURNING,
    WITH,
    RECURSIVE,
    WINDOW,
    OVER,
    PARTITION,
    ROWS,
    RANGE,
    NULLS,
    FIRST,
    LAST,
    ASC,
    DESC,

    // Operators
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    GREATER_THAN,
    LESS_EQUALS,
    GREATER_EQUALS,
    PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,
    MODULO,
    CONCAT, // ||

    // Punctuation
    SEMICOLON,
    COMMA,
    DOT,
    LPAREN,
    RPAREN,

    // Special
    EOF,
    WHITESPACE,
    COMMENT
  }

  private final Type type;
  private final String value;
  private final int line;
  private final int column;

  public Token(Type type, String value, int line, int column) {
    this.type = type;
    this.value = value;
    this.line = line;
    this.column = column;
  }

  public Type getType() {
    return type;
  }

  public String getValue() {
    return value;
  }

  public int getLine() {
    return line;
  }

  public int getColumn() {
    return column;
  }

  @Override
  public String toString() {
    return String.format("Token{%s, '%s', %d:%d}", type, value, line, column);
  }
}
