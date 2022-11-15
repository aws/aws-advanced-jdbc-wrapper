/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package software.amazon;

import java.sql.ResultSet;
import software.amazon.jdbc.PropertyDefinition;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;


public class ReadWriteSplittingPostgresExample {

  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/readWriteSplittingExample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {

    final Properties props = new Properties();

    // Enable readWriteSplitting, failover, and efm plugins and set properties
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm");
    props.setProperty(PropertyDefinition.USER.name, USERNAME);
    props.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    // Setup Step: Open connection and create tables - uncomment this section to create table and test values
    // try (final Connection connection = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
    // setInitialSessionSettings(connection);
    // executeWithFailoverHandling(connection,
    //    "CREATE TABLE bank_test (id int primary key, name varchar(40), account_balance int)");
    // executeWithFailoverHandling(connection,
    //    "INSERT INTO bank_test VALUES (0, 'Jane Doe', 200), (1, 'John Smith', 200), (2, 'Sally Smith', 200), (3, 'Joe Smith', 200)");
    // }

    // Uncomment to enable reader load balancing
    // props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");

    // Example Step: Open connection and perform transaction
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      setInitialSessionSettings(conn);
      // Begin business transaction
      conn.setAutoCommit(false);

      // Example business transaction
      executeWithFailoverHandling(
          conn,
          "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name='Jane Doe'");
      executeWithFailoverHandling(
          conn,
          "UPDATE bank_test SET account_balance=account_balance + 100 WHERE name='John Smith'");

      // Commit business transaction
      executeWithFailoverHandling(conn, "commit");
      // Change connection to the reader connection internally
      conn.setReadOnly(true);

      // If reader load-balancing was enabled, each execute will be performed against a new reader.
      for (int i = 0; i < 4; i++) {
        ResultSet rs = executeWithFailoverHandling(conn, "SELECT * FROM bank_test WHERE id = " + i);
        processResults(rs);
      }

    } catch (FailoverFailedSQLException e) {
      // User application should open a new connection, check the results of the failed transaction and re-run it if
      // needed. See:
      // https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#08001---unable-to-establish-sql-connection
      throw e;
    } catch (TransactionStateUnknownSQLException e) {
      // User application should check the status of the failed transaction and restart it if needed. See:
      // https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#08007---transaction-resolution-unknown
      throw e;
    } catch (SQLException e) {
      // Unexpected exception unrelated to failover. This should be handled by the user application.
      throw e;
    }
  }

  public static void processResults(ResultSet results) {
    // User can process results as needed
  }

  public static void setInitialSessionSettings(Connection conn) throws SQLException {
    try (Statement stmt1 = conn.createStatement()) {
      // User can edit settings
      stmt1.executeUpdate("SET TIME ZONE 'UTC'");
    }
  }

  public static ResultSet executeWithFailoverHandling(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      boolean hasResults = stmt.execute(query);
      return hasResults ? stmt.getResultSet() : null;
    } catch (FailoverFailedSQLException e) {
      // Connection failed, and JDBC wrapper failed to reconnect to a new instance.
      throw e;
    } catch (FailoverSuccessSQLException e) {
      // Query execution failed and JDBC wrapper successfully failed over to a new elected writer node.
      // Reconfigure the connection
      setInitialSessionSettings(conn);
      // Re-run query
      try (Statement stmt = conn.createStatement()) {
        boolean hasResults = stmt.execute(query);
        return hasResults ? stmt.getResultSet() : null;
      }
    } catch (TransactionStateUnknownSQLException e) {
      // Connection failed while executing a business transaction.
      // Transaction status is unknown. The driver has successfully reconnected to a new writer.
      throw e;
    }
  }
}

