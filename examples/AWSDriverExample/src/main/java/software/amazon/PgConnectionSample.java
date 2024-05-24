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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Simple Connection Test.
 */
public class PgConnectionSample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://database-pg-name.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/connectionSample";
  // Alternatively, all connection properties can be set in the connection string:
  // private static final String CONNECTION_STRING =
  //     "jdbc:aws-wrapper:postgresql://database-pg-name.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/connectionSample"
  //         + "?user=username"
  //         + "&password=password"
  //         + "&loginTimeout=100"
  //         + "&wrapperPlugins=failover,efm2"
  //         + "&wrapperLogUnclosedConnections=true";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();

    // Configuring connection properties for the underlying JDBC driver.
    properties.setProperty("user", USERNAME);
    properties.setProperty("password", PASSWORD);
    properties.setProperty("loginTimeout", "100");

    // Configuring connection properties for the JDBC Wrapper.
    properties.setProperty("wrapperPlugins", "failover,efm2");
    properties.setProperty("wrapperLogUnclosedConnections", "true");

    try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
    }
  }
}
