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

import java.util.*;

public class PostgreSQLParser {
    
    public static class ParseException extends RuntimeException {
        public int position;
        
        public ParseException(String message, int position) {
            super(message + " at position " + position);
            this.position = position;
        }
    }
    
    public enum RawParseMode {
        DEFAULT, TYPE_NAME, PLPGSQL_EXPR, PLPGSQL_ASSIGN1, PLPGSQL_ASSIGN2, PLPGSQL_ASSIGN3
    }
    
    public static class ParseNode {
        public String type;
        public String value;
        public int location;
        public List<ParseNode> children = new ArrayList<>();
        
        public ParseNode(String type, String value, int location) {
            this.type = type;
            this.value = value;
            this.location = location;
        }
    }
    
    public static class Token {
        public String type;
        public String value;
        public int position;
        
        public Token(String type, String value, int position) {
            this.type = type;
            this.value = value;
            this.position = position;
        }
    }
    
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
        "ALTER", "TABLE", "INDEX", "VIEW", "FUNCTION", "PROCEDURE", "TRIGGER",
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "DISTINCT", "ORDER", "BY",
        "GROUP", "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT",
        "FULL", "OUTER", "ON", "AS", "IN", "EXISTS", "BETWEEN", "LIKE", "ILIKE"
    ));
    
    private String input;
    private int position;
    private List<Token> tokens;
    
    public List<ParseNode> rawParser(String sql, RawParseMode mode) {
        this.input = sql;
        this.position = 0;
        this.tokens = tokenize();
        
        List<ParseNode> parseTree = new ArrayList<>();
        
        if (!tokens.isEmpty()) {
            Token firstToken = tokens.get(0);
            if (firstToken.type.equals("PUNCTUATION") && 
                (";".equals(firstToken.value) || ",".equals(firstToken.value))) {
                throw new ParseException("Invalid SQL syntax: unexpected token '" + firstToken.value + "'", firstToken.position);
            }
        }
        
        while (position < tokens.size()) {
            ParseNode stmt = parseStatement();
            if (stmt != null) {
                parseTree.add(stmt);
            }
        }
        
        return parseTree;
    }
    
    private List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        int pos = 0;
        
        while (pos < input.length()) {
            char c = input.charAt(pos);
            
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }
            
            if (c == '-' && pos + 1 < input.length() && input.charAt(pos + 1) == '-') {
                pos = skipLineComment(pos);
                continue;
            }
            
            if (c == '/' && pos + 1 < input.length() && input.charAt(pos + 1) == '*') {
                pos = skipBlockComment(pos);
                continue;
            }
            
            if (c == '\'' || c == '"') {
                Token token = parseString(pos, c);
                tokens.add(token);
                pos = token.position + token.value.length() + 2;
                continue;
            }
            
            if (Character.isLetter(c) || c == '_') {
                Token token = parseIdentifier(pos);
                tokens.add(token);
                pos = token.position + token.value.length();
                continue;
            }
            
            if (Character.isDigit(c)) {
                Token token = parseNumber(pos);
                tokens.add(token);
                pos = token.position + token.value.length();
                continue;
            }
            
            if ("()[]{},.;".indexOf(c) >= 0) {
                tokens.add(new Token("PUNCTUATION", String.valueOf(c), pos));
                pos++;
                continue;
            }
            
            if (c == '?') {
                tokens.add(new Token("PLACEHOLDER", "?", pos));
                pos++;
                continue;
            }
            
            if ("+-*/<>=!#".indexOf(c) >= 0) {
                Token token = parseOperator(pos);
                tokens.add(token);
                pos = token.position + token.value.length();
                continue;
            }
            
            if ("@$%^&".indexOf(c) >= 0) {
                Token token = new Token("INVALID", String.valueOf(c), pos);
                tokens.add(token);
                pos++;
                continue;
            }
            
            pos++;
        }
        
        return tokens;
    }
    
    private ParseNode parseStatement() {
        if (position >= tokens.size()) return null;
        
        Token token = tokens.get(position);
        String keyword = token.value.toUpperCase();
        
        switch (keyword) {
            case "SELECT":
                return parseSelect();
            case "INSERT":
                return parseInsert();
            case "UPDATE":
                return parseUpdate();
            case "DELETE":
                return parseDelete();
            case "CREATE":
                return parseCreate();
            default:
                if (token.type.equals("INVALID")) {
                    throw new ParseException("Invalid SQL syntax: unexpected token '" + token.value + "'", token.position);
                }
                position++;
                return new ParseNode("UNKNOWN", token.value, token.position);
        }
    }
    
    private ParseNode parseSelect() {
        ParseNode select = new ParseNode("SELECT", "SELECT", tokens.get(position).position);
        position++;
        
        if (position >= tokens.size()) {
            throw new ParseException("Incomplete SELECT statement", getCurrentPosition());
        }
        
        if (position < tokens.size() && "DISTINCT".equals(tokens.get(position).value.toUpperCase())) {
            select.children.add(new ParseNode("DISTINCT", "DISTINCT", tokens.get(position).position));
            position++;
        }
        
        ParseNode columns = parseSelectList();
        if (columns != null) select.children.add(columns);
        
        if (position < tokens.size() && "FROM".equals(tokens.get(position).value.toUpperCase())) {
            position++;
            if (position >= tokens.size()) {
                throw new ParseException("Missing table name after FROM", getCurrentPosition());
            }
            ParseNode from = parseFromClause();
            if (from != null) select.children.add(from);
        }
        
        if (position < tokens.size() && "WHERE".equals(tokens.get(position).value.toUpperCase())) {
            position++;
            ParseNode where = parseWhereClause();
            if (where != null) select.children.add(where);
        }
        
        return select;
    }
    
    private ParseNode parseSelectList() {
        ParseNode list = new ParseNode("SELECT_LIST", "", getCurrentPosition());
        
        do {
            if (position >= tokens.size()) break;
            
            // Stop at major SQL keywords
            Token token = tokens.get(position);
            String upperValue = token.value.toUpperCase();
            if ("FROM".equals(upperValue) || "WHERE".equals(upperValue) || 
                "ORDER".equals(upperValue) || "GROUP".equals(upperValue) || 
                "HAVING".equals(upperValue) || "LIMIT".equals(upperValue) ||
                "OFFSET".equals(upperValue) || ";".equals(token.value)) {
                break;
            }
            
            ParseNode expr = parseExpression();
            if (expr != null) list.children.add(expr);
            
            if (position < tokens.size() && ",".equals(tokens.get(position).value)) {
                position++;
            } else {
                break;
            }
        } while (true);
        
        // If we have no columns in the select list, that's an error
        if (list.children.isEmpty()) {
            throw new ParseException("SELECT statement must have at least one column", getCurrentPosition());
        }
        
        return list;
    }
    
    private ParseNode parseFromClause() {
        ParseNode from = new ParseNode("FROM", "FROM", getCurrentPosition());
        ParseNode table = parseTableRef();
        if (table == null) {
            throw new ParseException("Expected table name after FROM", getCurrentPosition());
        }
        from.children.add(table);
        return from;
    }
    
    private ParseNode parseWhereClause() {
        ParseNode where = new ParseNode("WHERE", "WHERE", getCurrentPosition());
        
        while (position < tokens.size()) {
            Token token = tokens.get(position);
            String upperValue = token.value.toUpperCase();
            
            if ("ORDER".equals(upperValue) || "GROUP".equals(upperValue) || 
                "HAVING".equals(upperValue) || "LIMIT".equals(upperValue) ||
                "OFFSET".equals(upperValue) || ";".equals(token.value)) {
                break;
            }
            
            ParseNode expr = parseExpression();
            if (expr != null) {
                where.children.add(expr);
            }
        }
        
        return where;
    }
    
    private ParseNode parseTableRef() {
        if (position >= tokens.size()) return null;
        Token token = tokens.get(position);
        
        if ("(".equals(token.value)) {
            position++;
            ParseNode subquery = new ParseNode("SUBQUERY", "(", token.position);
            
            boolean foundClosing = false;
            
            while (position < tokens.size() && !")".equals(tokens.get(position).value)) {
                ParseNode inner = parseStatement();
                if (inner != null) subquery.children.add(inner);
            }
            
            if (position < tokens.size() && ")".equals(tokens.get(position).value)) {
                position++;
                foundClosing = true;
            }
            
            if (!foundClosing && position >= tokens.size()) {
                throw new ParseException("Unclosed parenthesis in table reference", token.position);
            }
            
            return subquery;
        }
        
        if (token.type.equals("IDENTIFIER") || token.type.equals("KEYWORD") || token.type.equals("STRING")) {
            position++;
            return new ParseNode("TABLE_REF", token.value, token.position);
        }
        
        throw new ParseException("Expected table name, got '" + token.value + "'", token.position);
    }
    
    private ParseNode parseExpression() {
        if (position >= tokens.size()) return null;
        Token token = tokens.get(position);
        
        if ("PLACEHOLDER".equals(token.type)) {
            position++;
            return new ParseNode("PLACEHOLDER", "?", token.position);
        }
        
        if ("(".equals(token.value)) {
            position++;
            ParseNode expr = new ParseNode("PARENTHESES", "(", token.position);
            
            while (position < tokens.size() && !")".equals(tokens.get(position).value)) {
                if (",".equals(tokens.get(position).value)) {
                    position++;
                    continue;
                }
                ParseNode inner = parseExpression();
                if (inner != null) expr.children.add(inner);
            }
            
            if (position < tokens.size() && ")".equals(tokens.get(position).value)) {
                position++;
            }
            
            return expr;
        }
        
        position++;
        return new ParseNode("EXPRESSION", token.value, token.position);
    }
    
    private ParseNode parseInsert() {
        ParseNode insert = new ParseNode("INSERT", "INSERT", tokens.get(position).position);
        position++; // consume INSERT
        
        // Look for INTO table_name
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            Token token = tokens.get(position);
            if ("INTO".equals(token.value.toUpperCase())) {
                position++; // consume INTO
                if (position < tokens.size()) {
                    Token tableToken = tokens.get(position);
                    ParseNode tableNode = new ParseNode("TABLE_REF", tableToken.value, tableToken.position);
                    insert.children.add(tableNode);
                    position++;
                    
                    // Look for column list (col1, col2, ...)
                    if (position < tokens.size() && "(".equals(tokens.get(position).value)) {
                        position++; // consume (
                        ParseNode columnList = new ParseNode("COLUMN_LIST", "", tokens.get(position).position);
                        
                        while (position < tokens.size() && !")".equals(tokens.get(position).value)) {
                            Token colToken = tokens.get(position);
                            if (!",".equals(colToken.value)) {
                                ParseNode colNode = new ParseNode("COLUMN", colToken.value, colToken.position);
                                columnList.children.add(colNode);
                            }
                            position++;
                        }
                        
                        if (position < tokens.size() && ")".equals(tokens.get(position).value)) {
                            position++; // consume )
                        }
                        
                        insert.children.add(columnList);
                    }
                    break;
                }
            } else {
                position++;
            }
        }
        
        // Skip remaining tokens
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            position++;
        }
        
        return insert;
    }
    
    private ParseNode parseUpdate() {
        ParseNode update = new ParseNode("UPDATE", "UPDATE", tokens.get(position).position);
        position++; // consume UPDATE
        
        // Next token should be table name
        if (position < tokens.size()) {
            Token tableToken = tokens.get(position);
            ParseNode tableNode = new ParseNode("TABLE_REF", tableToken.value, tableToken.position);
            update.children.add(tableNode);
            position++;
        }
        
        // Look for SET clause
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            Token token = tokens.get(position);
            if ("SET".equals(token.value.toUpperCase())) {
                position++; // consume SET
                ParseNode setClause = new ParseNode("SET_CLAUSE", "", tokens.get(position).position);
                
                // Parse SET col1 = val1, col2 = val2, ...
                while (position < tokens.size() && 
                       !";".equals(tokens.get(position).value) && 
                       !"WHERE".equals(tokens.get(position).value.toUpperCase())) {
                    
                    Token colToken = tokens.get(position);
                    if (!",".equals(colToken.value) && !"=".equals(colToken.value)) {
                        // Check if this looks like a column name (before =)
                        if (position + 1 < tokens.size() && "=".equals(tokens.get(position + 1).value)) {
                            ParseNode colNode = new ParseNode("COLUMN", colToken.value, colToken.position);
                            setClause.children.add(colNode);
                        }
                    }
                    position++;
                }
                
                update.children.add(setClause);
                break;
            } else {
                position++;
            }
        }
        
        // Skip remaining tokens
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            position++;
        }
        
        return update;
    }
    
    private ParseNode parseDelete() {
        ParseNode delete = new ParseNode("DELETE", "DELETE", tokens.get(position).position);
        position++; // consume DELETE
        
        // Look for FROM table_name
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            Token token = tokens.get(position);
            if ("FROM".equals(token.value.toUpperCase())) {
                position++; // consume FROM
                if (position < tokens.size()) {
                    Token tableToken = tokens.get(position);
                    ParseNode tableNode = new ParseNode("TABLE_REF", tableToken.value, tableToken.position);
                    delete.children.add(tableNode);
                    position++;
                    break;
                }
            } else {
                position++;
            }
        }
        
        // Skip remaining tokens
        while (position < tokens.size() && !";".equals(tokens.get(position).value)) {
            position++;
        }
        
        return delete;
    }
    
    private ParseNode parseCreate() {
        ParseNode create = new ParseNode("CREATE", "CREATE", tokens.get(position).position);
        position++;
        
        if (position < tokens.size() && "TABLE".equals(tokens.get(position).value.toUpperCase())) {
            position++;
            return parseCreateTable(create);
        }
        
        return create;
    }
    
    private ParseNode parseCreateTable(ParseNode create) {
        create.children.add(new ParseNode("TABLE", "TABLE", getCurrentPosition()));
        
        if (position < tokens.size()) {
            create.children.add(new ParseNode("TABLE_NAME", tokens.get(position).value, tokens.get(position).position));
            position++;
        }
        
        if (position < tokens.size() && "(".equals(tokens.get(position).value)) {
            position++;
            ParseNode columns = new ParseNode("COLUMNS", "(", getCurrentPosition());
            
            while (position < tokens.size() && !")".equals(tokens.get(position).value)) {
                ParseNode col = parseExpression();
                if (col != null) columns.children.add(col);
            }
            
            if (position < tokens.size() && ")".equals(tokens.get(position).value)) {
                position++;
            }
            
            create.children.add(columns);
        }
        
        return create;
    }
    
    private Token parseIdentifier(int pos) {
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
            sb.append(input.charAt(pos++));
        }
        String value = sb.toString();
        String type = KEYWORDS.contains(value.toUpperCase()) ? "KEYWORD" : "IDENTIFIER";
        return new Token(type, value, pos - value.length());
    }
    
    private Token parseNumber(int pos) {
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.')) {
            sb.append(input.charAt(pos++));
        }
        String value = sb.toString();
        return new Token("NUMBER", value, pos - value.length());
    }
    
    private Token parseString(int pos, char quote) {
        StringBuilder sb = new StringBuilder();
        pos++;
        while (pos < input.length() && input.charAt(pos) != quote) {
            if (input.charAt(pos) == '\\' && pos + 1 < input.length()) {
                pos++;
            }
            sb.append(input.charAt(pos++));
        }
        return new Token("STRING", sb.toString(), pos - sb.length() - 1);
    }
    
    private Token parseOperator(int pos) {
        StringBuilder sb = new StringBuilder();
        while (pos < input.length() && "+-*/<>=!#".indexOf(input.charAt(pos)) >= 0) {
            sb.append(input.charAt(pos++));
        }
        String value = sb.toString();
        return new Token("OPERATOR", value, pos - value.length());
    }
    
    private int skipLineComment(int pos) {
        while (pos < input.length() && input.charAt(pos) != '\n') {
            pos++;
        }
        return pos;
    }
    
    private int skipBlockComment(int pos) {
        pos += 2;
        while (pos + 1 < input.length()) {
            if (input.charAt(pos) == '*' && input.charAt(pos + 1) == '/') {
                return pos + 2;
            }
            pos++;
        }
        return pos;
    }
    
    private int getCurrentPosition() {
        if (position < tokens.size()) {
            return tokens.get(position).position;
        } else if (!tokens.isEmpty()) {
            Token lastToken = tokens.get(tokens.size() - 1);
            return lastToken.position + lastToken.value.length();
        }
        return input != null ? input.length() : 0;
    }
}
