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

import com.zaxxer.hikari.HikariDataSource;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class HikariExample {

  private static final String USER = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {
    HikariDataSource ds = new HikariDataSource();

    // Configure the connection pool:
    ds.setUsername(USER);
    ds.setPassword(PASSWORD);

    // Specify the underlying datasource for HikariCP:
    ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

    // Configure AwsWrapperDataSource:
    ds.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql:");
    ds.addDataSourceProperty("databasePropertyName", "databaseName");
    ds.addDataSourceProperty("portPropertyName", "portNumber");
    ds.addDataSourceProperty("serverPropertyName", "serverName");

    // Specify the driver-specific data source for AwsWrapperDataSource:
    ds.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");

    // Configuring PGSimpleDataSource:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
    targetDataSourceProps.setProperty("databaseName", "postgres");
    targetDataSourceProps.setProperty("portNumber", "5432");

    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    // Try and make a connection:
    try (final Connection conn = ds.getConnection();
        final Statement statement = conn.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
      while (rs.next()) {
        System.out.println(rs.getString("first_name"));
      }
    }
  }
}
