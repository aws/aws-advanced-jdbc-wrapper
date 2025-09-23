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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSQLParserTest {

  private PostgreSQLParser parser;

  @BeforeEach
  public void setUp() {
    parser = new PostgreSQLParser();
  }

  @Test
  public void testSelectRegression() {
    assertParses("SELECT * FROM onek WHERE onek.unique1 < 10 ORDER BY onek.unique1");
    assertParses("SELECT onek.unique1, onek.stringu1 FROM onek WHERE onek.unique1 < 20");
    assertParses("SELECT DISTINCT ON (string4) string4, two, four FROM onek ORDER BY string4, two, four");
    assertParses("SELECT * FROM onek WHERE (four = 1 OR two = 2) AND ten < 4 ORDER BY unique1");
    assertParses("SELECT unique1, (SELECT sum(unique1) FROM onek b WHERE b.unique1 < a.unique1) FROM onek a ORDER BY unique1 LIMIT 10");
  }

  @Test
  public void testCreateTableRegression() {
    assertParses("CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)");
    assertParses("CREATE UNLOGGED TABLE unlogged1 (a int primary key)");
    assertParses("CREATE TEMPORARY TABLE unlogged2 (a int primary key)");
    assertParses("CREATE TABLE as_select1 AS SELECT * FROM pg_class WHERE relkind = 'r'");
    assertParses("CREATE TABLE cities (name text, population real, elevation int)");
    assertParses("CREATE TABLE capitals (state char(2) UNIQUE NOT NULL) INHERITS (cities)");
  }

  @Test
  public void testInsertRegression() {
    assertParses("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')");
    assertParses("INSERT INTO test_table VALUES (1, 'test')");
    assertParses("INSERT INTO inserttest (col1, col2, col3) VALUES (DEFAULT, DEFAULT, DEFAULT)");
    assertParses("INSERT INTO inserttest VALUES(10, 20, '40'), (-1, 2, DEFAULT)");
    assertParses("INSERT INTO target_table SELECT col1, col2 FROM source_table WHERE condition = true");
  }

  @Test
  public void testAggregatesRegression() {
    assertParses("SELECT avg(four) AS avg_1 FROM onek");
    assertParses("SELECT sum(four) AS sum_1500 FROM onek WHERE four > 0");
    assertParses("SELECT count(*) AS cnt_1000 FROM onek");
    assertParses("SELECT four, ten, SUM(ten) OVER (PARTITION BY four ORDER BY ten) FROM tenk1 WHERE unique2 < 10");
    assertParses("SELECT salary, sum(salary) OVER (ORDER BY salary) FROM empsalary");
  }

  @Test
  public void testJoinRegression() {
    assertParses("SELECT * FROM J1_TBL INNER JOIN J2_TBL USING (i)");
    assertParses("SELECT * FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i)");
    assertParses("SELECT * FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i) ORDER BY i, k, t");
    assertParses("SELECT * FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i)");
    assertParses("SELECT * FROM J1_TBL CROSS JOIN J2_TBL");
    assertParses("SELECT * FROM J1_TBL NATURAL JOIN J2_TBL");
  }

  @Test
  public void testConstraintsRegression() {
    assertParses("CREATE TABLE CHECK_TBL (x int, CONSTRAINT CHECK_CON CHECK (x > 3))");
    assertParses("CREATE TABLE UNIQUE_TBL (i int UNIQUE, t text)");
    assertParses("CREATE TABLE PRIMARY_TBL (i int PRIMARY KEY, t text)");
    assertParses("CREATE TABLE PKTABLE (ptest1 int PRIMARY KEY, ptest2 text)");
    assertParses("CREATE TABLE FKTABLE (ftest1 int REFERENCES PKTABLE, ftest2 int)");
    assertParses("CREATE TABLE products (product_no integer, name text, price numeric CHECK (price > 0))");
  }

  @Test
  public void testIndexRegression() {
    assertParses("CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops)");
    assertParses("CREATE INDEX test_index ON test_table (column1) WHERE column2 > 100");
    assertParses("CREATE INDEX people_names ON people ((first_name || ' ' || last_name))");
    assertParses("CREATE INDEX gin_test_idx ON gin_test_tbl USING gin (i)");
  }

  @Test
  public void testTriggersRegression() {
    assertParses("CREATE TRIGGER before_ins_stmt_trig BEFORE INSERT ON main_table FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func()");
    assertParses("CREATE TRIGGER after_upd_row_trig AFTER UPDATE ON main_table FOR EACH ROW EXECUTE PROCEDURE trigger_func()");
    assertParses("CREATE TRIGGER conditional_trigger BEFORE UPDATE ON test_table FOR EACH ROW WHEN (OLD.status != NEW.status) EXECUTE PROCEDURE log_status_change()");
  }

  @Test
  public void testPartitionRegression() {
    assertParses("CREATE TABLE measurement (city_id int not null, logdate date not null, peaktemp int, unitsales int) PARTITION BY RANGE (logdate)");
    assertParses("CREATE TABLE measurement_y2006m02 PARTITION OF measurement FOR VALUES FROM ('2006-02-01') TO ('2006-03-01')");
    assertParses("CREATE TABLE cities (city_name varchar(80), country varchar(80)) PARTITION BY LIST (country)");
    assertParses("CREATE TABLE orders (order_id int, customer_id int, order_date date) PARTITION BY HASH (customer_id)");
  }

  @Test
  public void testJsonRegression() {
    assertParses("SELECT '\"hello\"'::json");
    assertParses("SELECT '{\"a\": \"foo\", \"b\": \"bar\"}'::json");
    assertParses("SELECT '[1, 2, \"foo\", null]'::json");
    assertParses("SELECT '{\"a\": 1, \"b\": 2}'::jsonb -> 'a'");
    assertParses("SELECT json_agg(name) FROM users");
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
    assertThrows(PostgreSQLParser.ParseException.class, () -> {
      parser.rawParser(";", PostgreSQLParser.RawParseMode.DEFAULT);
    });

    assertThrows(PostgreSQLParser.ParseException.class, () -> {
      parser.rawParser("SELECT FROM", PostgreSQLParser.RawParseMode.DEFAULT);
    });

    assertThrows(PostgreSQLParser.ParseException.class, () -> {
      parser.rawParser("SELECT * FROM @invalid", PostgreSQLParser.RawParseMode.DEFAULT);
    });
  }

  private void assertParses(String sql) {
    List<PostgreSQLParser.ParseNode> result = parser.rawParser(sql, PostgreSQLParser.RawParseMode.DEFAULT);
    assertNotNull(result, "Parser returned null for: " + sql);
    assertFalse(result.isEmpty(), "Parser returned empty result for: " + sql);
  }
}
