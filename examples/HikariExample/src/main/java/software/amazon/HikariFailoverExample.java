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

/**
 * An example to use the AWS Wrapper with Hikari with failover enabled.
 */
public class HikariFailoverExample {

  private static final String USER = "username";
  private static final String PASSWORD = "password";
  private static final String DATABASE_NAME = "postgres";
  private static final String ENDPOINT = "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com";

  public static void main(String[] args) throws SQLException {
    try (HikariDataSource ds = new HikariDataSource()) {

      // Configure the connection pool:
      ds.setUsername(USER);
      ds.setPassword(PASSWORD);

      // Specify the underlying datasource for HikariCP:
      ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

      // Configure AwsWrapperDataSource:
      ds.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql:");
      ds.addDataSourceProperty("serverName", ENDPOINT);
      ds.addDataSourceProperty("serverPort", "5432");
      ds.addDataSourceProperty("database", DATABASE_NAME);

      // Alternatively, the AwsWrapperDataSource can be configured with a JDBC URL instead of individual properties as
      // seen above.
      ds.addDataSourceProperty(
          "jdbcUrl",
          "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres");

      // The failover plugin throws failover-related exceptions that need to be handled explicitly by HikariCP,
      // otherwise connections will be closed immediately after failover. Set `ExceptionOverrideClassName` to provide
      // a custom exception class.
      ds.setExceptionOverrideClassName("software.amazon.jdbc.util.HikariCPSQLException");

      // Specify the driver-specific data source for AwsWrapperDataSource:
      ds.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");

      Properties targetDataSourceProps = new Properties();

      // Configuring PGSimpleDataSource if needed:
      // targetDataSourceProps.setProperty("ssl", "true");

      // Enable the failover and host monitoring connection plugins.
      targetDataSourceProps.setProperty("wrapperPlugins", "failover,efm2");

      ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

      // Attempt a connection:
      try (final Connection conn = ds.getConnection();
          final Statement statement = conn.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT * from aurora_db_instance_identifier()")) {
        while (rs.next()) {
          System.out.println(rs.getString(1));
        }
      }
    }
  }
}
