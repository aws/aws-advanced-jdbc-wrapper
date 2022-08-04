/*
 *    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import com.amazon.awslabs.jdbc.PropertyDefinition;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.postgresql.PGProperty;

/**
 * Example Throwing Failover Exception Codes.
 */
public class FailoverSample {

  public static void main(String[] args) throws SQLException {

    final Properties props = new Properties();

    // User configures connection properties here
    public static final String POSTGRESQL_CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://database-pg-name.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/failoverSample";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    // Enable failover plugin and set properties
    props.setProperty(PropertyDefinition.PLUGINS.name, "failover");
    props.setProperty(PropertyDefinition.USER.name, USERNAME);
    props.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      setInitialSessionSettings(conn);

      try {
        // Begin business transaction
        conn.setAutoCommit(false);

        // Comment out if test database and values already created
        updateQueryWithFailoverHandling(conn, "CREATE TABLE bank_test (name varchar(40), account_balance int)");
        updateQueryWithFailoverHandling(conn, "INSERT INTO bank_test VALUES ('Jane Doe', 200), ('John Smith', 200)");

        // Example business transaction
        updateQueryWithFailoverHandling(conn, "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name='Jane Doe'");
        updateQueryWithFailoverHandling(conn, "UPDATE bank_test SET account_balance=account_balance + 100 WHERE name='John Smith'");

        // Commit business transaction
        updateQueryWithFailoverHandling(conn, "commit");

      } catch (SQLException e) {
        conn.rollback();
      }
    }
  }

  public static void setInitialSessionSettings(Connection conn) throws SQLException {
    try (Statement stmt1 = conn.createStatement()) {
      // User can edit settings
      stmt1.executeUpdate("SET TIME ZONE 'UTC'");
    }
  }

  public static void updateQueryWithFailoverHandling(Connection conn, String query) throws SQLException {
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(query);
    } catch (SQLException e) {
      // Connection failed, and JDBC wrapper failed to reconnect to a new instance.
      if ("08S01".equalsIgnoreCase(e.getSQLState())) {
        // User application should reconnect to new instance and restart business transaction here.
        throw e;
      }
      // Connection failed and JDBC wrapper successfully failed over to a new elected writer node
      if ("08S02".equalsIgnoreCase(e.getSQLState())) {
        // Reconfigure the connection.
        setInitialSessionSettings(conn);

        // Re-run query
        Statement stmt = conn.createStatement();
        stmt.executeQuery(query);
      }

      // Connection failed while executing a business transaction.
      // Transaction status is unknown. The driver has successfully reconnected to a new writer.
      if ("08007".equalsIgnoreCase(e.getSQLState())) {
        // User application should restart the business transaction here.
        throw e;
      }
      throw e;
    }
  }
}
