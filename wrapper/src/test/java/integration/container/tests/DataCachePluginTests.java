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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin;
import software.amazon.jdbc.plugin.DataCacheConnectionPlugin.CachedResultSet;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@MakeSureFirstInstanceWriter
@Order(5)
public class DataCachePluginTests {

  private static final Logger LOGGER = Logger.getLogger(DataCachePluginTests.class.getName());

  @BeforeEach
  public void beforeEach() {
    DataCacheConnectionPlugin.clearCache();
  }

  @TestTemplate
  public void testQueryCacheable() throws SQLException {

    DataCacheConnectionPlugin.clearCache();

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "30000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "30000");

    props.setProperty(PropertyDefinition.PLUGINS.name, "dataCache");
    props.setProperty(DataCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, ".*testTable.*");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    conn.createStatement().execute("drop table if exists testTable");
    conn.createStatement().execute("create table testTable (id int not null primary key, name varchar(100))");
    conn.createStatement().execute("insert into testTable (id, name) values (1, 'name1')");
    conn.createStatement().execute("insert into testTable (id, name) values (2, 'name2')");
    conn.createStatement().execute("insert into testTable (id, name) values (3, 'name3')");

    printTable();

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

    conn.createStatement().execute("update testTable set id=id*10");
    conn.createStatement().execute("update testTable set name=concat('name', id)");

    // Actual data in the database table
    // 10, "name10"
    // 20, "name20"
    // 30, "name30"

    printTable();

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

    printTable();

    // The following SQL statement isn't in the cache so data is fetched from DB
    Statement statementFromDb = conn.createStatement();
    ResultSet resultSetFromDb =
        statementFromDb.executeQuery("select id, name from testTable where id > 0");
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

  private void printTable() {
    LOGGER.finest(
        () -> {
          try {
            final Properties props = ConnectionStringHelper.getDefaultProperties();
            Connection conn = DriverManager.getConnection(ConnectionStringHelper.getUrl(), props);
            Statement statementFromDb = conn.createStatement();
            ResultSet resultSetFromDb =
                statementFromDb.executeQuery("select id, name from testTable where id > 0");
            StringBuilder sb = new StringBuilder("Table content:");
            boolean hasData = false;
            while (resultSetFromDb.next()) {
              hasData = true;
              sb.append("\n\t")
                  .append(resultSetFromDb.getObject(1))
                  .append(", ")
                  .append(resultSetFromDb.getObject(2));
            }
            if (!hasData) {
              sb.append("\n\t<No data>");
            }
            conn.close();
            return sb.toString();

          } catch (SQLException ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  @TestTemplate
  public void testQueryNotCacheable() throws SQLException {

    DataCacheConnectionPlugin.clearCache();

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    PropertyDefinition.CONNECT_TIMEOUT.set(props, "30000");
    PropertyDefinition.SOCKET_TIMEOUT.set(props, "30000");
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataCache");
    props.setProperty(
        DataCacheConnectionPlugin.DATA_CACHE_TRIGGER_CONDITION.name, ".*WRONG_EXPRESSION.*");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    conn.createStatement().execute("drop table if exists testTable");
    conn.createStatement().execute("create table testTable (id int not null primary key, name varchar(100))");
    conn.createStatement().execute("insert into testTable (id, name) values (1, 'name1')");
    conn.createStatement().execute("insert into testTable (id, name) values (2, 'name2')");
    conn.createStatement().execute("insert into testTable (id, name) values (3, 'name3')");

    printTable();

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

    conn.createStatement().execute("update testTable set id=id*10");
    conn.createStatement().execute("update testTable set name=concat('name', id)");

    // Actual data in the database table
    // 10, "name10"
    // 20, "name20"
    // 30, "name30"

    printTable();

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
