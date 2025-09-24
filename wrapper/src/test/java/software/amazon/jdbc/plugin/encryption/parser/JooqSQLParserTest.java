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

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.ParserException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JooqSQLParserTest {

  private DSLContext dsl;

  @BeforeEach
  public void setUp() {
    dsl = DSL.using(SQLDialect.POSTGRES);
  }

  @Test
  public void testSelectRegression() {
    assertParses("SELECT * FROM onek WHERE onek.unique1 < 10 ORDER BY onek.unique1");
    assertParses("SELECT onek.unique1, onek.stringu1 FROM onek WHERE onek.unique1 < 20");
    assertParses("SELECT DISTINCT string4, two, four FROM onek ORDER BY string4, two, four");
    assertParses("SELECT * FROM onek WHERE (four = 1 OR two = 2) AND ten < 4 ORDER BY unique1");
  }

  @Test
  public void testCreateTableRegression() {
    assertParses("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)");
    assertParses("CREATE TABLE cities (name text, population real, elevation int)");
  }

  @Test
  public void testInsertRegression() {
    assertParses("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')");
    assertParses("INSERT INTO test_table VALUES (1, 'test')");
    assertParses("INSERT INTO target_table SELECT col1, col2 FROM source_table WHERE condition = true");
  }

  @Test
  public void testAggregatesRegression() {
    assertParses("SELECT avg(four) AS avg_1 FROM onek");
    assertParses("SELECT sum(four) AS sum_1500 FROM onek WHERE four > 0");
    assertParses("SELECT count(*) AS cnt_1000 FROM onek");
  }

  @Test
  public void testJoinRegression() {
    assertParses("SELECT * FROM J1_TBL INNER JOIN J2_TBL USING (i)");
    assertParses("SELECT * FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i)");
    assertParses("SELECT * FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i) ORDER BY i, k, t");
    assertParses("SELECT * FROM J1_TBL CROSS JOIN J2_TBL");
  }

  @Test
  public void testConstraintsRegression() {
    assertParses("CREATE TABLE CHECK_TBL (x int, CONSTRAINT CHECK_CON CHECK (x > 3))");
    assertParses("CREATE TABLE UNIQUE_TBL (i int UNIQUE, t text)");
    assertParses("CREATE TABLE PRIMARY_TBL (i int PRIMARY KEY, t text)");
  }

  @Test
  public void testPlaceholderRegression() {
    assertParses("SELECT * FROM users WHERE id = ?");
    assertParses("INSERT INTO users (name, email) VALUES (?, ?)");
    assertParses("UPDATE users SET name = ? WHERE id = ?");
    assertParses("DELETE FROM users WHERE created_date < ? AND status = ?");
  }

  @Test
  public void testInvalidSQLRegression() {
    assertThrows(ParserException.class, () -> dsl.parser().parseQuery("SELECT * FROM"));
    assertThrows(ParserException.class, () -> dsl.parser().parseQuery("INVALID SQL STATEMENT"));
  }

  @Test
  public void testSelectJoinQuery() {
    String sql = "SELECT c.name, c.ssn, o.payment_info FROM customers c JOIN orders o ON c.id = o.customer_id";
    
    try {
      Query query = dsl.parser().parseQuery(sql);
      assertNotNull(query);
      assertTrue(query.getClass().getSimpleName().contains("Select"));
    } catch (Exception e) {
      fail("Failed to parse SELECT JOIN query: " + e.getMessage());
    }
  }

  @Test
  public void testQueryTypeDetection() {
    assertQueryType("SELECT * FROM users", "Select");
    assertQueryType("INSERT INTO users VALUES (1, 'test')", "Insert");
    assertQueryType("UPDATE users SET name = 'test'", "Update");
    assertQueryType("DELETE FROM users WHERE id = 1", "Delete");
    assertQueryType("CREATE TABLE test (id int)", "Create");
    assertQueryType("DROP TABLE test", "Drop");
  }

  private void assertParses(String sql) {
    try {
      Query result = dsl.parser().parseQuery(sql);
      assertNotNull(result, "Parser returned null for: " + sql);
    } catch (ParserException e) {
      fail("Failed to parse SQL: " + sql + " - " + e.getMessage());
    }
  }

  private void assertQueryType(String sql, String expectedType) {
    try {
      Query query = dsl.parser().parseQuery(sql);
      String className = query.getClass().getSimpleName();
      assertTrue(className.contains(expectedType), 
        "Expected query type " + expectedType + " but got " + className + " for SQL: " + sql);
    } catch (ParserException e) {
      fail("Failed to parse SQL: " + sql + " - " + e.getMessage());
    }
  }
}
