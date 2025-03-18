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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.C3P0PooledConnectionProvider;
import software.amazon.jdbc.Driver;

public class C3P0Example {
  private static final String PROTOCOL = "jdbc:aws-wrapper:mysql://";
  private static final String URL = "mydbname.cluster-xyz.us-east-1.rds.amazonaws.com";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String DB = "dbname";
  private static final String CONN_STRING = PROTOCOL + URL + "/" + DB;

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("user", USERNAME);
    props.put("password", PASSWORD);
    props.put("wrapperPlugins", "failover2,efm2,auroraConnectionTracker");
    props.put("connectTimeout", "60000");
    props.put("socketTimeout", "360000");
    props.put("initializationFailTimeout", "30000");
    props.put("maxLifeTime", "500000");
    props.put("idleTimeout", "300000");

    // runExternalPoolExample(props);
    runInternalPoolExample(props);
  }

  private static void runExternalPoolExample(Properties props) {
    ComboPooledDataSource dataSource = new ComboPooledDataSource();
    dataSource.setProperties(props);
    dataSource.setJdbcUrl(CONN_STRING);
    try (Connection conn = dataSource.getConnection()) {
      Statement stmt = conn.createStatement();
      final ResultSet rs = stmt.executeQuery("SELECT 1");
      System.out.println(Util.getResult(rs));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void runInternalPoolExample(Properties props) {
    C3P0PooledConnectionProvider provider = new C3P0PooledConnectionProvider();
    Driver.setCustomConnectionProvider(provider);
    try (Connection conn = DriverManager.getConnection(CONN_STRING, props)) {
      Statement stmt = conn.createStatement();
      final ResultSet rs = stmt.executeQuery("SELECT 1");
      System.out.println(Util.getResult(rs));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
