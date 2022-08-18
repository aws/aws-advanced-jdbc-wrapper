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

import software.amazon.jdbc.PropertyDefinition;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;

public class FailoverSample {

  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://database-pg-name.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/failoverSample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {

    final Properties props = new Properties();

    // Enable failover plugin and set properties
    props.setProperty(PropertyDefinition.PLUGINS.name, "failover");
    props.setProperty(PropertyDefinition.USER.name, USERNAME);
    props.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    // AWS Advanced JDBC Wrapper configuration
    props.setProperty(PropertyDefinition.TARGET_DRIVER_USER_PROPERTY_NAME.name, "user");
    props.setProperty(PropertyDefinition.TARGET_DRIVER_PASSWORD_PROPERTY_NAME.name, "password");

    // Setup Step: Open connection and create tables - uncomment this section to create table and test values
    // try (final Connection connection = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
    //   setInitialSessionSettings(connection);
    //   updateQueryWithFailoverHandling(connection,
    //       "CREATE TABLE bank_test (name varchar(40), account_balance int)");
    //   updateQueryWithFailoverHandling(connection,
    //       "INSERT INTO bank_test VALUES ('Jane Doe', 200), ('John Smith', 200)");
    // }

    // Transaction Step: Open connection and perform transaction
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
      setInitialSessionSettings(conn);
      // Begin business transaction
      conn.setAutoCommit(false);

      // Example business transaction
      updateQueryWithFailoverHandling(conn,
          "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name='Jane Doe'");
      updateQueryWithFailoverHandling(conn,
          "UPDATE bank_test SET account_balance=account_balance + 100 WHERE name='John Smith'");

      // Commit business transaction
      updateQueryWithFailoverHandling(conn, "commit");
    }  catch (SQLException e) {
      // Unexpected exception unrelated to failover. This should be handled by the user application.
      throw e;
    }
  }

  public static void setInitialSessionSettings(Connection conn) throws SQLException {
    try (Statement stmt1 = conn.createStatement()) {
      // User can edit settings
      stmt1.executeUpdate("SET TIME ZONE 'UTC'");
    }
  }

  public static void updateQueryWithFailoverHandling(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(query);
    } catch (FailoverFailedSQLException e) {
      // Connection failed, and JDBC wrapper failed to reconnect to a new instance.
      // User application should open a new connection, check the results of the failed transaction and re-run it if
      // needed. See:
      // https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-wrapper/using-plugins/UsingTheFailoverPlugin.md#08001---unable-to-establish-sql-connection
      throw e;
    } catch (FailoverSuccessSQLException e) {
      // Query execution failed and JDBC wrapper successfully failed over to a new elected writer node.
      // Reconfigure the connection
      setInitialSessionSettings(conn);
      // Re-run query
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(query);
      }
      return;
    } catch (TransactionStateUnknownSQLException e) {
      // Connection failed while executing a business transaction.
      // Transaction status is unknown. The driver has successfully reconnected to a new writer.
      // User application should check the status of the failed transaction and restart it if needed. See:
      // https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-wrapper/using-plugins/UsingTheFailoverPlugin.md#08007---transaction-resolution-unknown
      throw e;
    }
  }
}
