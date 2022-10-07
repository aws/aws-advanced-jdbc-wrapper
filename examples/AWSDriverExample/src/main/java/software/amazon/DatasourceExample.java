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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class DatasourceExample {
  private static final String USER = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    // Configure the property names for the underlying driver-specific data source:
    ds.setJdbcProtocol("jdbc:postgresql:");
    ds.setDatabasePropertyName("databaseName");
    ds.setServerPropertyName("serverName");
    ds.setPortPropertyName("port");

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    // Configure the driver-specific data source:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
    targetDataSourceProps.setProperty("databaseName", "employees");
    targetDataSourceProps.setProperty("port", "5432");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    // Try and make a connection:
    try (final Connection conn = ds.getConnection(USER, PASSWORD);
        final Statement statement = conn.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
