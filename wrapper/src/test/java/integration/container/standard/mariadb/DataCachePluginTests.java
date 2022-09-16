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

package integration.container.standard.mariadb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin.CachedResultSet;

public class DataCachePluginTests extends StandardMariadbBaseTest {

  @Test
  public void testQueryCacheable() throws SQLException {

    DataCacheConnectionPlugin.clearCache();

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataCache");
    props.setProperty(DataCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, ".*testTable.*");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    conn.createStatement().execute("drop table if exists testTable");
    conn.createStatement().execute("create table testTable (id int not null, name varchar(100))");
    conn.createStatement().execute("insert into testTable (id, name) values (1, 'name1')");
    conn.createStatement().execute("insert into testTable (id, name) values (2, 'name2')");
    conn.createStatement().execute("insert into testTable (id, name) values (3, 'name3')");

    Statement statement = conn.createStatement();
    ResultSet resultSet = statement.executeQuery("select id, name from testTable");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getObject(1));
    assertEquals("name1", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(2, resultSet.getObject(1));
    assertEquals("name2", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(3, resultSet.getObject(1));
    assertEquals("name3", resultSet.getObject(2));

    assertFalse(resultSet.next()); // no more data

    conn.createStatement().execute("update testTable set id=id*10, name=concat(\"name\", id)");

    // Actual data in the database table
    // 10, "name10"
    // 20, "name20"
    // 30, "name30"

    Statement testStatement = conn.createStatement();
    ResultSet testResultSet = testStatement.executeQuery("select id, name from testTable");
    assertTrue(testResultSet.isWrapperFor(CachedResultSet.class));

    // It's expected to get cached data
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getObject(1));
    assertEquals("name1", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(2, resultSet.getObject(1));
    assertEquals("name2", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(3, resultSet.getObject(1));
    assertEquals("name3", resultSet.getObject(2));

    // The following SQL statement isn't in the cache so data is fetched from DB
    Statement statementFromDb = conn.createStatement();
    ResultSet resultSetFromDb = statementFromDb.executeQuery("select id, name from testTable where id > 0");
    assertTrue(resultSetFromDb.next());
    assertEquals(10, resultSetFromDb.getObject(1));
    assertEquals("name10", resultSetFromDb.getObject(2));
    assertTrue(resultSetFromDb.next());
    assertEquals(20, resultSetFromDb.getObject(1));
    assertEquals("name20", resultSetFromDb.getObject(2));
    assertTrue(resultSetFromDb.next());
    assertEquals(30, resultSetFromDb.getObject(1));
    assertEquals("name30", resultSetFromDb.getObject(2));

    conn.close();
  }

  @Test
  public void testQueryNotCacheable() throws SQLException {

    DataCacheConnectionPlugin.clearCache();

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataCache");
    props.setProperty(DataCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, ".*WRONG_EXPRESSION.*");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    conn.createStatement().execute("drop table if exists testTable");
    conn.createStatement().execute("create table testTable (id int not null, name varchar(100))");
    conn.createStatement().execute("insert into testTable (id, name) values (1, 'name1')");
    conn.createStatement().execute("insert into testTable (id, name) values (2, 'name2')");
    conn.createStatement().execute("insert into testTable (id, name) values (3, 'name3')");

    Statement statement = conn.createStatement();
    ResultSet resultSet = statement.executeQuery("select id, name from testTable");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getObject(1));
    assertEquals("name1", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(2, resultSet.getObject(1));
    assertEquals("name2", resultSet.getObject(2));
    assertTrue(resultSet.next());
    assertEquals(3, resultSet.getObject(1));
    assertEquals("name3", resultSet.getObject(2));

    assertFalse(resultSet.next()); // no more data

    conn.createStatement().execute("update testTable set id=id*10, name=concat(\"name\", id)");

    // Actual data in the database table
    // 10, "name10"
    // 20, "name20"
    // 30, "name30"

    Statement testStatement = conn.createStatement();
    ResultSet testResultSet = testStatement.executeQuery("select id, name from testTable");
    assertFalse(testResultSet.isWrapperFor(CachedResultSet.class));

    // It's expected to get cached data
    assertTrue(testResultSet.next());
    assertEquals(10, testResultSet.getObject(1));
    assertEquals("name10", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(20, testResultSet.getObject(1));
    assertEquals("name20", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(30, testResultSet.getObject(1));
    assertEquals("name30", testResultSet.getObject(2));

    conn.close();
  }
}
