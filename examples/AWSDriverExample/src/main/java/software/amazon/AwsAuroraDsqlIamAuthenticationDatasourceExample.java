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

// This file shows how to establish an Aurora DSQL connection using HikariCP datasource and the Aurora DSQL IAM
// authentication plugin.
public class AwsAuroraDsqlIamAuthenticationDatasourceExample {

  private static final String IAM_DATABASE_USER = "admin";

  public static void main(String[] args) throws SQLException {
    try (HikariDataSource ds = new HikariDataSource()) {

      // Configure the connection pool:
      ds.setUsername(IAM_DATABASE_USER);

      // Specify the underlying datasource for HikariCP:
      ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

      // Configure AwsWrapperDataSource:
      ds.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql://");
      ds.addDataSourceProperty("serverName", "cluster-identifier.dsql.us-east-1.on.aws");
      ds.addDataSourceProperty("serverPort", "5432");
      ds.addDataSourceProperty("database", "postgres");

      // Specify the driver-specific data source for AwsWrapperDataSource:
      ds.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");

      Properties targetDataSourceProps = new Properties();

      // Enable the Aurora DSQL IAM authentication plugin.
      targetDataSourceProps.setProperty("wrapperPlugins", "iamDsql");

      ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

      // Attempt a connection:
      try (final Connection conn = ds.getConnection();
        final Statement statement = conn.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT 1")) {
        System.out.println(Util.getResult(rs));
      }
    }
  }
}
