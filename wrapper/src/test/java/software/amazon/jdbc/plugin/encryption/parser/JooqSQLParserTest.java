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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class JSqlParserTest {

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM users",
      "SELECT name, age FROM users WHERE id = 1",
      "INSERT INTO users (name, age) VALUES ('John', 25)",
      "UPDATE users SET name = 'Jane' WHERE id = 1",
      "DELETE FROM users WHERE id = 1",
      "CREATE TABLE test (id INT, name VARCHAR(50))",
      "DROP TABLE test"
  })
  void testValidSqlParsing(String sql) {
    assertDoesNotThrow(() -> {
      Statement statement = CCJSqlParserUtil.parse(sql);
      assertNotNull(statement);
    });
  }

  @Test
  void testInvalidSqlParsing() {
    assertThrows(JSQLParserException.class, () -> CCJSqlParserUtil.parse("SELECT * FROM"));
    assertThrows(JSQLParserException.class, () -> CCJSqlParserUtil.parse("INVALID SQL STATEMENT"));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM users",
      "SELECT name, age FROM users WHERE id = 1",
      "select * from products",
      "Select Name From Customers"
  })
  void testSelectStatements(String sql) {
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      assertTrue(statement.getClass().getSimpleName().contains("Select"));
    } catch (JSQLParserException e) {
      fail("Should parse valid SELECT statement: " + sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "INSERT INTO users (name) VALUES ('test')",
      "insert into products (name, price) values ('item', 10.99)",
      "Insert Into Customers (Name) Values ('John')"
  })
  void testInsertStatements(String sql) {
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      assertTrue(statement.getClass().getSimpleName().contains("Insert"));
    } catch (JSQLParserException e) {
      fail("Should parse valid INSERT statement: " + sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "UPDATE users SET name = 'test'",
      "update products set price = 15.99 where id = 1",
      "Update Customers Set Name = 'Jane' Where Id = 2"
  })
  void testUpdateStatements(String sql) {
    try {
      Statement statement = CCJSqlParserUtil.parse(sql);
      assertTrue(statement.getClass().getSimpleName().contains("Update"));
    } catch (JSQLParserException e) {
      fail("Should parse valid UPDATE statement: " + sql);
    }
  }
}
