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

package software.amazon;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class ReadWriteSplittingSpringJdbcTemplateMySQLExample {

  private static final String DATABASE_URL = "database-url";
  private static final String DATABASE_NAME = "database-name";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final int QUERY_LOOP_NUM = 32;

  private static final int CONNECTION_POOL_MAXIMUM_POOL_SIZE = 2;

  private static final int CONNECTION_POOL_IDLE_TIMEOUT = 2;

  public static void main(String[] args) throws SQLException {
    scenario1();
    scenario2();
    scenario3();
  }

  private static void scenario1() {
    // Simple MySQL DataSource implementation - readOnly not set
    DataSource dataSource = getSimpleMySQLDataSource();
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    for (int i = 0; i < QUERY_LOOP_NUM; i++) {
      jdbcTemplate.queryForObject("SELECT 1", Integer.class);
    }
  }

  private static void scenario2() throws SQLException {
    // Simple MySQL DataSource implementation - setReadOnly(true)
    DataSource dataSource = getSimpleMySQLDataSource();
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    try (Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(true);
      for (int i = 0; i < QUERY_LOOP_NUM; i++) {
        jdbcTemplate.queryForObject("SELECT 1", Integer.class);
      }
    }
  }

  private static void scenario3() throws SQLException {
    // Hikari MySQL DataSource for connection pooling - setReadOnly(true)
    DataSource dataSource = getHikariCPDataSource();
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    try (Connection conn = dataSource.getConnection()) {
      conn.setReadOnly(true);
      for (int i = 0; i < QUERY_LOOP_NUM; i++) {
        jdbcTemplate.queryForObject("SELECT 1", Integer.class);
      }
    }
  }

  private static DataSource getSimpleMySQLDataSource() {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol("jdbc:mysql:");
    ds.setServerName(DATABASE_URL);
    ds.setDatabase(DATABASE_NAME);
    ds.setServerPort("3306");

    ds.setTargetDataSourceClassName("com.mysql.cj.jdbc.MysqlDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm2");

    ds.setUser(USERNAME);
    ds.setPassword(PASSWORD);

    ds.setTargetDataSourceProperties(targetDataSourceProps);

    return ds;
  }

  private static DataSource getHikariCPDataSource() {
    HikariDataSource ds = new HikariDataSource();

    ds.setUsername(USERNAME);
    ds.setPassword(PASSWORD);
    ds.setMaximumPoolSize(CONNECTION_POOL_MAXIMUM_POOL_SIZE);
    ds.setIdleTimeout(CONNECTION_POOL_IDLE_TIMEOUT);

    ds.setReadOnly(true);

    ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());
    ds.addDataSourceProperty("jdbcProtocol", "jdbc:mysql:");
    ds.addDataSourceProperty("serverName", DATABASE_URL);
    ds.addDataSourceProperty("serverPort", "3306");
    ds.addDataSourceProperty("database", DATABASE_NAME);

    ds.addDataSourceProperty("targetDataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name,
        "readWriteSplitting,failover,efm2");

    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    return ds;
  }
}
