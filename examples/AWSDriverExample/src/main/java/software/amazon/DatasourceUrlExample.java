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
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class DatasourceUrlExample {
  private static final String USER = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    // Configure basic information and any driver-specific and AWS JDBC Driver properties:
    // Configure any AWS JDBC Driver properties:
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com/employees?ssl=true&wrapperLoggerLevel=ALL");

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");


    // Try and make a connection:
    try (final Connection conn = ds.getConnection(USER, PASSWORD);
        final Statement statement = conn.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
