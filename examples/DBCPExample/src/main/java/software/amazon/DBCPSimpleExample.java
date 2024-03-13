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
import org.apache.commons.dbcp2.BasicDataSource;

public class DBCPSimpleExample {

  private static final String USER = "user";
  private static final String PASSWORD = "pass";

  public static void main(String[] args) throws SQLException {

    try (BasicDataSource ds = new BasicDataSource()) {
      ds.setUsername(USER);
      ds.setPassword(PASSWORD);
      ds.setDriverClassName("software.amazon.jdbc.Driver");
      ds.setUrl(
          "jdbc:aws-wrapper:postgresql://database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/postgres");
      ds.setConnectionProperties("socketTimeout=10");

      try (final Connection conn = ds.getConnection();
          final Statement statement = conn.createStatement();
          final ResultSet rs = statement.executeQuery("SELECT * from aurora_db_instance_identifier()")) {
        if (rs.next()) {
          System.out.println(rs.getString(1));
        }
      }
    }
  }
}
