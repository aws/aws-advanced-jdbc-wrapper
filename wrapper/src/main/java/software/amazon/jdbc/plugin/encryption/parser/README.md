# PostgreSQL Java SQL Parser

A Java SQL parser implementation based on PostgreSQL's `scan.l` (lexer) and `gram.y` (grammar) files. This parser can tokenize and parse basic SQL statements into an Abstract Syntax Tree (AST).

## Features

- **Lexical Analysis**: Tokenizes SQL input based on PostgreSQL's lexer rules
- **Syntax Analysis**: Parses tokens into an AST using PostgreSQL's grammar rules
- **AST Support**: Supports common SQL statements:
  - SELECT (with WHERE, ORDER BY, GROUP BY, HAVING, LIMIT)
  - INSERT (with column lists and VALUES)
  - UPDATE (with SET and WHERE clauses)
  - DELETE (with WHERE clause)
  - CREATE TABLE (with column definitions and constraints)
- **Expression Parsing**: Handles complex expressions with proper operator precedence
- **Pretty Printing**: Formats parsed SQL statements

## Architecture

The parser consists of several key components:

### 1. Lexer (`SqlLexer.java`)
- Tokenizes SQL input into tokens (keywords, identifiers, literals, operators, punctuation)
- Based on PostgreSQL's `scan.l` lexer rules
- Handles string literals, numeric literals, comments, and whitespace

### 2. Token (`Token.java`)
- Represents individual tokens with type, value, and position information
- Supports all major SQL token types

### 3. AST Nodes (`ast/AstNode.java`)
- Defines the Abstract Syntax Tree structure
- Includes nodes for statements, expressions, and other SQL constructs
- Implements the Visitor pattern for AST traversal

### 4. Parser (`SqlParser.java`)
- Recursive descent parser based on PostgreSQL's `gram.y` grammar
- Parses tokens into AST nodes
- Handles operator precedence and associativity

### 5. Main Parser (`PostgreSqlParser.java`)
- High-level interface combining lexer and parser
- Provides formatting and pretty-printing capabilities

## Usage

### Basic Parsing

```java
PostgreSqlParser parser = new PostgreSqlParser();

// Parse a SELECT statement
String sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name";
Statement stmt = parser.parse(sql);

// Pretty print the parsed statement
String formatted = parser.parseAndFormat(sql);
System.out.println(formatted);
```

### Supported SQL Examples

```sql
-- SELECT statements
SELECT id, name, age FROM users WHERE age > 18 ORDER BY name ASC;
SELECT COUNT(*) FROM orders GROUP BY customer_id HAVING COUNT(*) > 5;
SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;

-- INSERT statements
INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30);
INSERT INTO products (name, price) VALUES ('Widget', 19.99);

-- UPDATE statements
UPDATE users SET age = 26, status = 'active' WHERE name = 'John';
UPDATE products SET price = price * 1.1 WHERE category = 'electronics';

-- DELETE statements
DELETE FROM users WHERE age < 18;
DELETE FROM orders WHERE created_date < '2023-01-01';

-- CREATE TABLE statements
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  age INTEGER,
  email VARCHAR UNIQUE
);
```

## Building and Running

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher

### Build the Project
```bash
mvn clean compile
```

### Run Tests
```bash
mvn test
```

### Run the Main Class
```bash
mvn exec:java
```

### Package as JAR
```bash
mvn package
```

## Project Structure

```
java-sql-parser/
├── src/
│   ├── main/java/com/postgresql/parser/
│   │   ├── Token.java                 # Token representation
│   │   ├── SqlLexer.java             # Lexical analyzer
│   │   ├── SqlParser.java            # Syntax analyzer
│   │   ├── PostgreSqlParser.java     # Main parser interface
│   │   └── ast/
│   │       └── AstNode.java          # AST node definitions
│   └── test/java/com/postgresql/parser/
│       └── PostgreSqlParserTest.java # Unit tests
├── pom.xml                           # Maven build configuration
└── README.md                         # This file
```

## Implementation Details

### Lexer Implementation
The lexer is based on PostgreSQL's `scan.l` file and implements:
- Keyword recognition (SELECT, FROM, WHERE, etc.)
- Identifier tokenization (table names, column names)
- String literal parsing with escape sequences
- Numeric literal parsing (integers and floats)
- Operator and punctuation recognition
- Comment handling (line and block comments)

### Parser Implementation
The parser uses recursive descent parsing based on PostgreSQL's `gram.y` grammar:
- Implements operator precedence for expressions
- Handles left-associative and right-associative operators
- Supports complex nested expressions
- Provides detailed error messages with position information

### AST Design
The AST follows the Visitor pattern and includes:
- Base `AstNode` class for all nodes
- Specific statement classes (`SelectStatement`, `InsertStatement`, etc.)
- Expression hierarchy with binary expressions, literals, and identifiers
- Support for complex SQL constructs (joins, subqueries, etc.)

## Limitations

This is a simplified implementation focused on core SQL functionality. Some limitations include:

- Limited support for advanced PostgreSQL-specific syntax
- Simplified handling of complex expressions and functions
- Basic error recovery and reporting
- No semantic analysis or type checking
- Limited support for DDL statements beyond CREATE TABLE

## Contributing

This parser serves as an educational example of how to implement a SQL parser based on PostgreSQL's grammar. Contributions to extend functionality or improve the implementation are welcome.

## License

This project is provided as an educational example. The PostgreSQL grammar and lexer rules it's based on are part of the PostgreSQL project, which is licensed under the PostgreSQL License.
