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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SQL Lexer based on PostgreSQL's scan.l */
public class SqlLexer {
  private final String input;
  private int position;
  private int line;
  private int column;

  // Keywords map
  private static final Map<String, Token.Type> KEYWORDS = new HashMap<>();

  static {
    KEYWORDS.put("SELECT", Token.Type.SELECT);
    KEYWORDS.put("FROM", Token.Type.FROM);
    KEYWORDS.put("WHERE", Token.Type.WHERE);
    KEYWORDS.put("INSERT", Token.Type.INSERT);
    KEYWORDS.put("INTO", Token.Type.INTO);
    KEYWORDS.put("UPDATE", Token.Type.UPDATE);
    KEYWORDS.put("DELETE", Token.Type.DELETE);
    KEYWORDS.put("CREATE", Token.Type.CREATE);
    KEYWORDS.put("DROP", Token.Type.DROP);
    KEYWORDS.put("ALTER", Token.Type.ALTER);
    KEYWORDS.put("TABLE", Token.Type.TABLE);
    KEYWORDS.put("INDEX", Token.Type.INDEX);
    KEYWORDS.put("DATABASE", Token.Type.DATABASE);
    KEYWORDS.put("SCHEMA", Token.Type.SCHEMA);
    KEYWORDS.put("VIEW", Token.Type.VIEW);
    KEYWORDS.put("FUNCTION", Token.Type.FUNCTION);
    KEYWORDS.put("PROCEDURE", Token.Type.PROCEDURE);
    KEYWORDS.put("AND", Token.Type.AND);
    KEYWORDS.put("OR", Token.Type.OR);
    KEYWORDS.put("NOT", Token.Type.NOT);
    KEYWORDS.put("NULL", Token.Type.NULL);
    KEYWORDS.put("TRUE", Token.Type.TRUE);
    KEYWORDS.put("FALSE", Token.Type.FALSE);
    KEYWORDS.put("AS", Token.Type.AS);
    KEYWORDS.put("ON", Token.Type.ON);
    KEYWORDS.put("IN", Token.Type.IN);
    KEYWORDS.put("EXISTS", Token.Type.EXISTS);
    KEYWORDS.put("BETWEEN", Token.Type.BETWEEN);
    KEYWORDS.put("LIKE", Token.Type.LIKE);
    KEYWORDS.put("IS", Token.Type.IS);
    KEYWORDS.put("ISNULL", Token.Type.ISNULL);
    KEYWORDS.put("NOTNULL", Token.Type.NOTNULL);
    KEYWORDS.put("ORDER", Token.Type.ORDER);
    KEYWORDS.put("BY", Token.Type.BY);
    KEYWORDS.put("GROUP", Token.Type.GROUP);
    KEYWORDS.put("HAVING", Token.Type.HAVING);
    KEYWORDS.put("LIMIT", Token.Type.LIMIT);
    KEYWORDS.put("OFFSET", Token.Type.OFFSET);
    KEYWORDS.put("INNER", Token.Type.INNER);
    KEYWORDS.put("LEFT", Token.Type.LEFT);
    KEYWORDS.put("RIGHT", Token.Type.RIGHT);
    KEYWORDS.put("FULL", Token.Type.FULL);
    KEYWORDS.put("OUTER", Token.Type.OUTER);
    KEYWORDS.put("JOIN", Token.Type.JOIN);
    KEYWORDS.put("CROSS", Token.Type.CROSS);
    KEYWORDS.put("UNION", Token.Type.UNION);
    KEYWORDS.put("INTERSECT", Token.Type.INTERSECT);
    KEYWORDS.put("EXCEPT", Token.Type.EXCEPT);
    KEYWORDS.put("ALL", Token.Type.ALL);
    KEYWORDS.put("DISTINCT", Token.Type.DISTINCT);
    KEYWORDS.put("VALUES", Token.Type.VALUES);
    KEYWORDS.put("SET", Token.Type.SET);
    KEYWORDS.put("PRIMARY", Token.Type.PRIMARY);
    KEYWORDS.put("KEY", Token.Type.KEY);
    KEYWORDS.put("FOREIGN", Token.Type.FOREIGN);
    KEYWORDS.put("REFERENCES", Token.Type.REFERENCES);
    KEYWORDS.put("CASE", Token.Type.CASE);
    KEYWORDS.put("WHEN", Token.Type.WHEN);
    KEYWORDS.put("THEN", Token.Type.THEN);
    KEYWORDS.put("ELSE", Token.Type.ELSE);
    KEYWORDS.put("END", Token.Type.END);
    KEYWORDS.put("CAST", Token.Type.CAST);
    KEYWORDS.put("RETURNING", Token.Type.RETURNING);
    KEYWORDS.put("WITH", Token.Type.WITH);
    KEYWORDS.put("RECURSIVE", Token.Type.RECURSIVE);
    KEYWORDS.put("WINDOW", Token.Type.WINDOW);
    KEYWORDS.put("OVER", Token.Type.OVER);
    KEYWORDS.put("PARTITION", Token.Type.PARTITION);
    KEYWORDS.put("ROWS", Token.Type.ROWS);
    KEYWORDS.put("RANGE", Token.Type.RANGE);
    KEYWORDS.put("NULLS", Token.Type.NULLS);
    KEYWORDS.put("FIRST", Token.Type.FIRST);
    KEYWORDS.put("LAST", Token.Type.LAST);
    KEYWORDS.put("ASC", Token.Type.ASC);
    KEYWORDS.put("DESC", Token.Type.DESC);
  }

  public SqlLexer(String input) {
    this.input = input;
    this.position = 0;
    this.line = 1;
    this.column = 1;
  }

  public List<Token> tokenize() {
    List<Token> tokens = new ArrayList<>();
    Token token;

    while ((token = nextToken()).getType() != Token.Type.EOF) {
      if (token.getType() != Token.Type.WHITESPACE && token.getType() != Token.Type.COMMENT) {
        tokens.add(token);
      }
    }
    tokens.add(token); // Add EOF token

    return tokens;
  }

  public Token nextToken() {
    skipWhitespace();

    if (position >= input.length()) {
      return new Token(Token.Type.EOF, "", line, column);
    }

    char ch = input.charAt(position);
    int startLine = line;
    int startColumn = column;

    // Single character tokens
    switch (ch) {
      case ';':
        advance();
        return new Token(Token.Type.SEMICOLON, ";", startLine, startColumn);
      case ',':
        advance();
        return new Token(Token.Type.COMMA, ",", startLine, startColumn);
      case '.':
        // Check if this is a decimal number (. followed by digit)
        if (position + 1 < input.length() && Character.isDigit(input.charAt(position + 1))) {
          return readNumericLiteral();
        }
        advance();
        return new Token(Token.Type.DOT, ".", startLine, startColumn);
      case '(':
        advance();
        return new Token(Token.Type.LPAREN, "(", startLine, startColumn);
      case ')':
        advance();
        return new Token(Token.Type.RPAREN, ")", startLine, startColumn);
      case '+':
        advance();
        return new Token(Token.Type.PLUS, "+", startLine, startColumn);
      case '-':
        if (peek() == '-') {
          return readLineComment();
        }
        advance();
        return new Token(Token.Type.MINUS, "-", startLine, startColumn);
      case '*':
        advance();
        return new Token(Token.Type.MULTIPLY, "*", startLine, startColumn);
      case '/':
        if (peek() == '*') {
          return readBlockComment();
        }
        advance();
        return new Token(Token.Type.DIVIDE, "/", startLine, startColumn);
      case '%':
        advance();
        return new Token(Token.Type.MODULO, "%", startLine, startColumn);
      case '=':
        advance();
        return new Token(Token.Type.EQUALS, "=", startLine, startColumn);
      case '<':
        if (peek() == '=') {
          advance();
          advance();
          return new Token(Token.Type.LESS_EQUALS, "<=", startLine, startColumn);
        } else if (peek() == '>') {
          advance();
          advance();
          return new Token(Token.Type.NOT_EQUALS, "<>", startLine, startColumn);
        }
        advance();
        return new Token(Token.Type.LESS_THAN, "<", startLine, startColumn);
      case '>':
        if (peek() == '=') {
          advance();
          advance();
          return new Token(Token.Type.GREATER_EQUALS, ">=", startLine, startColumn);
        }
        advance();
        return new Token(Token.Type.GREATER_THAN, ">", startLine, startColumn);
      case '?':
        advance();
        return new Token(Token.Type.PLACEHOLDER, "?", startLine, startColumn);
      case '!':
        if (peek() == '=') {
          advance();
          advance();
          return new Token(Token.Type.NOT_EQUALS, "!=", startLine, startColumn);
        }
        break;
    }

    // String literals
    if (ch == '\'') {
      return readStringLiteral();
    }

    // Numeric literals
    if (Character.isDigit(ch)) {
      return readNumericLiteral();
    }

    // Identifiers and keywords
    if (Character.isLetter(ch) || ch == '_') {
      return readIdentifier();
    }

    // Unknown character
    advance();
    return new Token(Token.Type.IDENT, String.valueOf(ch), startLine, startColumn);
  }

  private void skipWhitespace() {
    while (position < input.length() && Character.isWhitespace(input.charAt(position))) {
      if (input.charAt(position) == '\n') {
        line++;
        column = 1;
      } else {
        column++;
      }
      position++;
    }
  }

  private char advance() {
    if (position >= input.length()) {
      return '\0';
    }
    char ch = input.charAt(position++);
    if (ch == '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
    return ch;
  }

  private char peek() {
    if (position + 1 >= input.length()) {
      return '\0';
    }
    return input.charAt(position + 1);
  }

  private Token readStringLiteral() {
    int startLine = line;
    int startColumn = column;
    StringBuilder sb = new StringBuilder();

    advance(); // Skip opening quote

    while (position < input.length()) {
      char ch = input.charAt(position);
      if (ch == '\'') {
        if (peek() == '\'') {
          // Escaped quote
          advance();
          advance();
          sb.append('\'');
        } else {
          // End of string
          advance();
          break;
        }
      } else {
        sb.append(advance());
      }
    }

    return new Token(Token.Type.SCONST, sb.toString(), startLine, startColumn);
  }

  private Token readNumericLiteral() {
    int startLine = line;
    int startColumn = column;
    StringBuilder sb = new StringBuilder();
    boolean hasDecimal = false;
    boolean hasExponent = false;

    // Handle starting with dot
    if (position < input.length() && input.charAt(position) == '.') {
      hasDecimal = true;
      sb.append(advance());
    }

    while (position < input.length()) {
      char ch = input.charAt(position);
      if (Character.isDigit(ch)) {
        sb.append(advance());
      } else if (ch == '.' && !hasDecimal && !hasExponent) {
        hasDecimal = true;
        sb.append(advance());
      } else if ((ch == 'e' || ch == 'E') && !hasExponent) {
        hasExponent = true;
        sb.append(advance());
        // Handle optional + or - after e/E
        if (position < input.length()
            && (input.charAt(position) == '+' || input.charAt(position) == '-')) {
          sb.append(advance());
        }
      } else {
        break;
      }
    }

    Token.Type type = (hasDecimal || hasExponent) ? Token.Type.FCONST : Token.Type.ICONST;
    return new Token(type, sb.toString(), startLine, startColumn);
  }

  private Token readIdentifier() {
    int startLine = line;
    int startColumn = column;
    StringBuilder sb = new StringBuilder();

    while (position < input.length()) {
      char ch = input.charAt(position);
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        sb.append(advance());
      } else {
        break;
      }
    }

    String value = sb.toString();
    String upperValue = value.toUpperCase();
    Token.Type type = KEYWORDS.getOrDefault(upperValue, Token.Type.IDENT);

    return new Token(type, value, startLine, startColumn);
  }

  private Token readLineComment() {
    int startLine = line;
    int startColumn = column;
    StringBuilder sb = new StringBuilder();

    while (position < input.length() && input.charAt(position) != '\n') {
      sb.append(advance());
    }

    return new Token(Token.Type.COMMENT, sb.toString(), startLine, startColumn);
  }

  private Token readBlockComment() {
    int startLine = line;
    int startColumn = column;
    StringBuilder sb = new StringBuilder();

    advance();
    advance(); // Skip /*

    while (position < input.length() - 1) {
      if (input.charAt(position) == '*' && input.charAt(position + 1) == '/') {
        advance();
        advance(); // Skip */
        break;
      }
      sb.append(advance());
    }

    return new Token(Token.Type.COMMENT, sb.toString(), startLine, startColumn);
  }
}
