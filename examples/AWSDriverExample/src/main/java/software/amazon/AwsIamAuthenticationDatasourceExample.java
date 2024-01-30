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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

// This file shows how to establish a database connection using HikariCP datasource and the IAM Authentication plugin.
public class AwsIamAuthenticationDatasourceExample {

  private static final String IAM_DATABASE_USER = "iam_user";

  public static void main(String[] args) throws SQLException {
    try (HikariDataSource ds = new HikariDataSource()) {

      // Configure the connection pool:
      ds.setUsername(IAM_DATABASE_USER);

      // Specify the underlying datasource for HikariCP:
      ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

      // Configure AwsWrapperDataSource:
      ds.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql://"); // Set protocol to jdbc:mysql for MySQL
      ds.addDataSourceProperty("serverName", "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
      ds.addDataSourceProperty("serverPort", "5432"); // Set port to 3306 for MySQL
      ds.addDataSourceProperty("database", "postgres");

      // The failover plugin throws failover-related exceptions that need to be handled explicitly by HikariCP,
      // otherwise connections will be closed immediately after failover. Set `ExceptionOverrideClassName` to provide
      // a custom exception class.
      ds.setExceptionOverrideClassName("software.amazon.jdbc.util.HikariCPSQLException");

      // Specify the driver-specific data source for AwsWrapperDataSource:
      ds.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");

      // For MySQL, set the driver-specific data source for AwsWrapperDataSource to com.mysql.cj.jdbc.MysqlDataSource
      // ds.addDataSourceProperty("targetDataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");

      Properties targetDataSourceProps = new Properties();

      // Enable the IAM authentication plugin along with failover and host monitoring plugins.
      targetDataSourceProps.setProperty("wrapperPlugins", "iam,failover,efm2");

      ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

      // Attempt a connection:
      try (final Connection conn = ds.getConnection();
          final Statement statement = conn.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT * from aurora_db_instance_identifier()")) {
        System.out.println(Util.getResult(rs));
      }
    }
  }
}
