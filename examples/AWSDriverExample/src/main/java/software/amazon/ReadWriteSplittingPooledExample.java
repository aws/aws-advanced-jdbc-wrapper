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

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class ReadWriteSplittingPooledExample {

  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/readWriteSplittingPooledExample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {
    Properties props = new Properties();
    PropertyDefinition.USER.set(props, USERNAME);
    PropertyDefinition.PASSWORD.set(props, PASSWORD);
    PropertyDefinition.PLUGINS.set(props, "readWriteSplitting,failover,efm");
    props.setProperty("databasePropertyName", "databaseName");
    props.setProperty("portPropertyName", "portNumber");
    props.setProperty("serverPropertyName", "serverName");

    ConnectionProviderManager.setConnectionProvider(
        new HikariPooledConnectionProvider((ReadWriteSplittingPooledExample::getHikariConfig)));

    try (Connection conn =
             DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      Statement stmt = conn.createStatement();
      stmt.execute("CREATE TABLE IF NOT EXISTS poolTest (id int, employee varchar(255))");
      stmt.execute("DELETE FROM poolTest WHERE id=1");
      stmt.execute("INSERT INTO poolTest VALUES (1, 'George')");

      conn.setReadOnly(true);
      stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM poolTest WHERE id=1");
      rs.next();
      System.out.println("Name: " + rs.getString(2));
    }
  }

  private static HikariConfig getHikariConfig(
      HikariConfig defaultConfig, HostSpec hostSpec, Properties props) {
    defaultConfig.setMaximumPoolSize(10);
    defaultConfig.setInitializationFailTimeout(75000);
    defaultConfig.setConnectionTimeout(1000);

    return defaultConfig;
  }
}
