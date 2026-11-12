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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.MakeSureFirstInstanceWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

/**
 * Verifies the transaction state the target drivers report through
 * {@link TargetDriverDialect#isInTransaction(Connection)} against a real database, and that
 * {@link PluginService#isInTransaction()} follows it. Covers the driver connection held directly,
 * the same connection held through the internal connection pool's proxy, and a driver that exposes
 * no transaction state, where the wrapper falls back to what it inferred from the SQL and the JDBC
 * calls.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
// The table below is created on the first instance, so it must be the writer.
@MakeSureFirstInstanceWriter
@Order(5)
public class TransactionStateTests {

  private static final Logger LOGGER = Logger.getLogger(TransactionStateTests.class.getName());

  private static final String TABLE = "transaction_state_test";

  /**
   * The table is recreated per test. This cannot run in {@code @BeforeAll}: the connection URL
   * depends on the current test driver, which {@link TestDriverProvider} sets for each test
   * invocation, before {@code @BeforeEach}.
   */
  @BeforeEach
  void recreateTable() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      stmt.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
    }
  }

  @TestTemplate
  public void test_autoCommit_noTransaction() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      assertTransactionState(conn, false);

      stmt.executeQuery("SELECT * FROM " + TABLE);
      assertTransactionState(conn, false);

      stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");
      assertTransactionState(conn, false);
    }
  }

  @TestTemplate
  public void test_autoCommitOff_commitAndRollbackEndTransaction() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      conn.setAutoCommit(false);

      stmt.executeQuery("SELECT * FROM " + TABLE);
      assertTransactionState(conn, true);

      conn.commit();
      assertTransactionState(conn, false);

      stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");
      assertTransactionState(conn, true);

      conn.rollback();
      assertTransactionState(conn, false);
    }
  }

  @TestTemplate
  public void test_explicitBeginAndCommitStatements() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      stmt.execute("BEGIN");
      assertTransactionState(conn, true);

      stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");
      assertTransactionState(conn, true);

      stmt.execute("COMMIT");
      assertTransactionState(conn, false);

      stmt.execute("START TRANSACTION");
      assertTransactionState(conn, true);

      stmt.execute("ROLLBACK");
      assertTransactionState(conn, false);
    }
  }

  /**
   * PostgreSQL keeps an aborted transaction block open until it is explicitly ended, and pgjdbc
   * records it as {@code TransactionState.FAILED}. That still counts as being in a transaction.
   */
  @TestTemplate
  @EnableOnDatabaseEngine({DatabaseEngine.PG})
  public void test_failedStatementKeepsTransactionOpen() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      conn.setAutoCommit(false);

      stmt.executeQuery("SELECT * FROM " + TABLE);
      assertTransactionState(conn, true);

      assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT * FROM no_such_table_" + TABLE));
      assertTransactionState(conn, true);

      conn.rollback();
      assertTransactionState(conn, false);
    }
  }

  @TestTemplate
  public void test_closedConnection_driverReportsNothing() throws SQLException {
    final Connection conn = openConnection();
    final PluginService pluginService = conn.unwrap(PluginService.class);
    final Connection targetConnection = pluginService.getCurrentConnection();
    final TargetDriverDialect dialect = pluginService.getTargetDriverDialect();

    conn.setAutoCommit(false);
    try (final Statement stmt = conn.createStatement()) {
      stmt.executeQuery("SELECT * FROM " + TABLE);
    }
    assertTransactionState(conn, true);

    conn.close();

    // The status recorded from the last server message is left over on the closed connection, so
    // the driver must no longer be trusted for it.
    assertNull(dialect.isInTransaction(targetConnection));
  }

  /**
   * With an internal connection pool, the wrapper's current connection is the pool's proxy, not the
   * driver connection. The proxy has to pass the state query through to the driver connection it
   * wraps, and has to stop answering once it is returned to the pool.
   */
  @TestTemplate
  public void test_internalConnectionPool_proxyPassesDriverStateThrough() throws SQLException {
    final HikariPooledConnectionProvider provider = new HikariPooledConnectionProvider(
        (hostSpec, props) -> {
          final HikariConfig config = new HikariConfig();
          config.setMaximumPoolSize(2);
          config.setInitializationFailTimeout(75000);
          return config;
        },
        null,
        // Pool every host. By default only RDS instance endpoints are pooled, which would leave the
        // containerized databases on a plain driver connection and bypass the proxy under test.
        (hostSpec, props) -> true,
        TimeUnit.MINUTES.toNanos(30),
        TimeUnit.MINUTES.toNanos(30));
    Driver.setCustomConnectionProvider(provider);

    try {
      final Connection conn = openConnection();
      final PluginService pluginService = conn.unwrap(PluginService.class);
      final Connection targetConnection = pluginService.getCurrentConnection();
      final TargetDriverDialect dialect = pluginService.getTargetDriverDialect();

      final String targetClass = targetConnection.getClass().getName();
      LOGGER.finest("Current connection class: " + targetClass);
      assertTrue(targetClass.startsWith("com.zaxxer.hikari"),
          "Expected the current connection to be the internal pool's proxy, but was " + targetClass);

      try (final Statement stmt = conn.createStatement()) {
        assertTransactionState(conn, false);

        conn.setAutoCommit(false);
        stmt.executeQuery("SELECT * FROM " + TABLE);
        assertTransactionState(conn, true);

        conn.commit();
        assertTransactionState(conn, false);

        stmt.execute("BEGIN");
        assertTransactionState(conn, true);

        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");
        assertTransactionState(conn, true);

        conn.rollback();
        assertTransactionState(conn, false);
      }

      conn.close();

      // The proxy has been handed back to the pool and no longer wraps a driver connection.
      assertNull(dialect.isInTransaction(targetConnection));
    } finally {
      ConnectionProviderManager.releaseResources();
      Driver.resetCustomConnectionProvider();
    }
  }

  private static Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        ConnectionStringHelper.getDefaultPropertiesWithNoPlugins());
  }

  /**
   * Whether the current test driver exposes its transaction state through its
   * {@link TargetDriverDialect}. pgjdbc and MySQL Connector/J do; the MariaDB driver does not, so
   * the wrapper stays on its own tracking there.
   */
  private static boolean driverReportsTransactionState() {
    final TestDriver driver = TestEnvironment.getCurrent().getCurrentDriver();
    return driver == TestDriver.PG || driver == TestDriver.MYSQL;
  }

  /**
   * Asserts the transaction state the target driver reports for the wrapper's current connection,
   * and the state {@link PluginService#isInTransaction()} derives from it. For a driver that reports
   * nothing, the plugin service must still arrive at the expected answer from its own tracking.
   */
  private static void assertTransactionState(final Connection conn, final boolean expected)
      throws SQLException {
    final PluginService pluginService = conn.unwrap(PluginService.class);
    final Connection targetConnection = pluginService.getCurrentConnection();
    final Boolean driverState = pluginService.getTargetDriverDialect().isInTransaction(targetConnection);

    if (driverReportsTransactionState()) {
      assertEquals(expected, driverState,
          "Transaction state reported by " + TestEnvironment.getCurrent().getCurrentDriver());
    } else {
      assertNull(driverState,
          TestEnvironment.getCurrent().getCurrentDriver() + " is not expected to report transaction state");
    }

    assertEquals(expected, pluginService.isInTransaction(), "PluginService.isInTransaction()");
  }
}
