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
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.cache.CachedResultSet;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.VALKEY_CACHE)
@DisableOnTestFeature({TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
@Order(5)
public class DataRemoteCachePluginTests {
  @TestTemplate
  public void testQueryCaching() throws SQLException {
    // Fetch the cache server information
    List<TestInstanceInfo> cacheInstances = TestEnvironment.getCurrent().getInfo().getDbCacheInfo().getInstances();
    TestInstanceInfo instance = cacheInstances.get(0);
    final String cacheEndpoint = instance.getHost() + ":" + instance.getPort();

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataRemoteCache");
    props.setProperty("cacheEndpointAddrRw", cacheEndpoint);
    props.setProperty("cacheUseSSL", "false");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
    conn.createStatement().execute("drop table if exists testTable");
    conn.createStatement().execute("create table testTable (id int not null primary key, name varchar(100))");
    conn.createStatement().execute("insert into testTable (id, name) values (1, 'name1')");
    conn.createStatement().execute("/*+ CACHE_PARAM(ttl=300s) */ insert into testTable (id, name) values (2, 'name2')");
    conn.createStatement().execute("/*+ CACHE_PARAM(ttl=300s) */ insert into testTable (id, name) values (3, 'name3')");

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
    conn.createStatement().execute("/* +CACHE_PARAM(ttl=300s) */ update testTable set name=concat('name', id)");

    Statement testStatement = conn.createStatement();
    ResultSet testResultSet = testStatement.executeQuery("select id, name from testTable");
    assertFalse(testResultSet.isWrapperFor(CachedResultSet.class));
    verifyResultSetForTestTable(testResultSet);

    // Now do the same query with caching enabled
    ResultSet cachedResultSet = testStatement.executeQuery("/* +CACHE_PARAM(ttl=300s) */ select id, name from testTable");
    assertTrue(cachedResultSet.isWrapperFor(CachedResultSet.class));
    verifyResultSetForTestTable(cachedResultSet);

    ResultSet cachedResultSet2 = testStatement.executeQuery("/* +CACHE_PARAM(ttl=300s) */ select id, name from testTable");
    assertTrue(cachedResultSet2.isWrapperFor(CachedResultSet.class));
    verifyResultSetForTestTable(cachedResultSet2);

    // Cache the result of an empty query
    ResultSet cachedResultSet3 = testStatement.executeQuery("/* +CACHE_PARAM(ttl=300s) */ select id, name from testTable where id = 4");
    assertTrue(cachedResultSet3.isWrapperFor(CachedResultSet.class));
    assertFalse(cachedResultSet3.next());

    conn.createStatement().execute("insert into testTable (id, name) values (4, 'name4')");

    // Previously cached result is fetched
    ResultSet cachedResultSet4 = testStatement.executeQuery("/* +CACHE_PARAM(ttl=300s) */ select id, name from testTable where id = 4");
    assertTrue(cachedResultSet4.isWrapperFor(CachedResultSet.class));
    assertFalse(cachedResultSet4.next());
    conn.createStatement().execute("delete from testTable");
    conn.close();
  }

  private void verifyResultSetForTestTable(ResultSet testResultSet) throws SQLException {
    assertTrue(testResultSet.next());
    assertEquals(10, testResultSet.getObject(1));
    assertEquals("name10", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(20, testResultSet.getObject(1));
    assertEquals("name20", testResultSet.getObject(2));
    assertTrue(testResultSet.next());
    assertEquals(30, testResultSet.getObject(1));
    assertEquals("name30", testResultSet.getObject(2));
  }
}
