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
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;


public class ReadWriteSplittingPostgresExample {

  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/readWriteSplittingExample";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException {

    final Properties props = new Properties();

    // Enable readWriteSplitting, failover, and efm2 plugins and set properties
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm2");
    props.setProperty(PropertyDefinition.USER.name, USERNAME);
    props.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    /**
     * Optional: configure read-write splitting to use internal connection pools (the getPoolKey
     * parameter is optional, see UsingTheReadWriteSplittingPlugin.md for more info).
     */
    // props.setProperty("somePropertyValue", "1"); // used in getPoolKey
    // final HikariPooledConnectionProvider connProvider =
    //     new HikariPooledConnectionProvider(
    //         ReadWriteSplittingPostgresExample::getHikariConfig,
    //         ReadWriteSplittingPostgresExample::getPoolKey
    //     );
    // ConnectionProviderManager.setConnectionProvider(connProvider);

    /* Setup Step: Open connection and create tables - uncomment this section to create table and test values */
    // try (final Connection connection = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, props)) {
    // setInitialSessionSettings(connection);
    // executeWithFailoverHandling(connection,
    //    "CREATE TABLE bank_test (id int primary key, name varchar(40), account_balance int)");
    // executeWithFailoverHandling(connection,
    //    "INSERT INTO bank_test VALUES (0, 'Jane Doe', 200), (1, 'John Smith', 200), (2, 'Sally Smith', 200), (3, 'Joe Smith', 200)");
    // }

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
      conn.commit();
      // Internally switch to a reader connection
      conn.setReadOnly(true);

      for (int i = 0; i < 4; i++) {
        executeWithFailoverHandling(conn, "SELECT * FROM bank_test WHERE id = " + i);
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

    /* Optional: if configured to use internal connection pools, close them here. */
    // ConnectionProviderManager.releaseResources();
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

  public static void executeWithFailoverHandling(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      boolean hasResults = stmt.execute(query);
      if (hasResults) {
        processResults(stmt.getResultSet());
      }
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
        if (hasResults) {
          processResults(stmt.getResultSet());
        }
      }
    } catch (TransactionStateUnknownSQLException e) {
      // Connection failed while executing a business transaction.
      // Transaction status is unknown. The driver has successfully reconnected to a new writer.
      throw e;
    }
  }

  // Optional methods: only required if configured to use internal connection pools.
  // The configuration in these methods are only examples - you can configure as you needed in your own code.
  private static HikariConfig getHikariConfig(HostSpec hostSpec, Properties props) {
    final HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(10);
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(10000);
    return config;
  }

  // This method is an optional parameter to `ConnectionProviderManager.setConnectionProvider`.
  // It can be omitted if you do not require it.
  private static String getPoolKey(HostSpec hostSpec, Properties props) {
    // Include the URL, user, and somePropertyValue in the connection pool key so that a new
    // connection pool will be opened for each different instance-user-somePropertyValue
    // combination.
    final String user = props.getProperty(PropertyDefinition.USER.name);
    final String somePropertyValue = props.getProperty("somePropertyValue");
    return hostSpec.getUrl() + user + somePropertyValue;
  }
}
