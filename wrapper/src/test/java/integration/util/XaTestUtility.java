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

package integration.util;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import integration.DatabaseEngine;
import integration.container.ConnectionStringHelper;
import integration.container.TestEnvironment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/** Shared helpers for the XA ({@code XADataSource}) integration tests. */
public final class XaTestUtility {

  private static final Logger LOGGER = Logger.getLogger(XaTestUtility.class.getName());

  /** How long a statement waits for a lock before failing instead of hanging. See {@link #boundLockWait}. */
  private static final int LOCK_WAIT_SECONDS = 30;

  /** MySQL's {@code ER_SPECIFIC_ACCESS_DENIED_ERROR}, raised when a dynamic privilege is missing. */
  private static final int ACCESS_DENIED_FOR_PRIVILEGE = 1227;

  private XaTestUtility() {
  }

  /**
   * Skips the calling test when the database cannot prepare (two-phase) transactions.
   *
   * <p>PostgreSQL disables prepared transactions by default ({@code max_prepared_transactions = 0}),
   * in which case {@code XAResource.prepare} always fails with "prepared transactions are disabled".
   * That is a server prerequisite (set through the RDS/Aurora parameter group), not something the
   * driver can work around, so tests that need a prepared branch are skipped instead of failing.
   * MySQL/InnoDB supports XA out of the box, so nothing is skipped there.
   */
  public static void assumePreparedTransactionsSupported() throws SQLException {
    final DatabaseEngine engine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    if (!DatabaseEngine.PG.equals(engine)) {
      return;
    }

    final int maxPreparedTransactions = queryMaxPreparedTransactions();
    assumeTrue(
        maxPreparedTransactions > 0,
        "PostgreSQL prepared (two-phase) transactions are disabled on this server "
            + "(max_prepared_transactions=" + maxPreparedTransactions + "). Set max_prepared_transactions "
            + "to a value greater than 0 in the DB (cluster) parameter group to run the XA prepare tests. "
            + "The test framework enables it on the PostgreSQL databases it creates, so this test is only "
            + "skipped when running against a database that was created without it (for example a reused one).");
  }

  /**
   * Skips the calling test when the database will not let this user run {@code XA RECOVER}.
   *
   * <p>MySQL 8 restricts {@code XA RECOVER} to holders of {@code XA_RECOVER_ADMIN}, on the reasoning that
   * listing another session's in-doubt branches is an administrative act. The Docker container grants it to
   * the test user on first boot, so this normally passes; on a managed deployment the user is whichever one
   * the provider created, and whether it holds the privilege is the provider's decision rather than ours.
   * Nothing the driver does can work around it, so a test that needs to recover is skipped rather than
   * failed - the same treatment {@link #assumePreparedTransactionsSupported} gives a server with prepared
   * transactions disabled.
   *
   * <p>Asked of the server rather than derived from {@code SHOW GRANTS}. A privilege can arrive through a
   * role, in which case it does not appear among the user's own grants, so reading them could report absent
   * for a user that can in fact recover. Running the statement is the only answer that cannot disagree with
   * the server.
   *
   * <p>PostgreSQL has no equivalent restriction: {@code pg_prepared_xacts} is readable and a session may
   * commit or roll back the branches it prepared, so nothing is skipped there.
   */
  public static void assumeXaRecoverSupported() throws SQLException {
    final DatabaseEngine engine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    if (!DatabaseEngine.MYSQL.equals(engine)) {
      return;
    }

    boolean permitted = true;
    String reason = "";
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement()) {
      // Read-only, and safe to run when nothing is prepared: it returns an empty result rather than failing.
      stmt.execute("XA RECOVER");
    } catch (final SQLException e) {
      // Narrow on purpose. Only a refusal to run this statement is a reason to skip; anything else - the
      // server being unreachable, say - is a real failure and must stay one.
      //
      // Matched on either the code or the privilege name, because two drivers report this and only the
      // server's own message is common to both: 1227 is MySQL's ER_SPECIFIC_ACCESS_DENIED_ERROR, and a driver
      // that maps the code differently still carries the text the server sent.
      final String message = e.getMessage() == null ? "" : e.getMessage();
      if (e.getErrorCode() != ACCESS_DENIED_FOR_PRIVILEGE
          && !message.contains("XA_RECOVER_ADMIN")) {
        throw e;
      }
      permitted = false;
      reason = message;
    }

    assumeTrue(
        permitted,
        "This user may not run XA RECOVER on this server, so a prepared branch cannot be discovered: "
            + reason + ". Grant XA_RECOVER_ADMIN to run the XA recovery tests.");
  }

  /**
   * Bounds how long a statement waits for a lock held by a prepared transaction.
   *
   * <p>Called before the DDL that the XA tests recreate their tables with, and it exists because of how badly
   * the default behaves. A prepared branch holds its locks until it is committed or rolled back, so a test
   * that fails between {@code prepare} and {@code commit} leaves the table locked; the next test's
   * {@code DROP TABLE} then waits for a lock that nothing will release. MySQL's {@code lock_wait_timeout}
   * defaults to a year and PostgreSQL's {@code lock_timeout} to no limit, so the run neither fails nor
   * finishes - it stops, with the worker blocked in a socket read and no test named as the cause. That cost
   * eighteen minutes of a suite run and a thread dump to diagnose.
   *
   * <p>A bound turns that into a failure that names the lock, on the test that could not get it. The first
   * failure - the one that left the branch prepared - is still the interesting one, and it is now reported
   * first because the run keeps going.
   *
   * <p>Session scope, so it applies to this connection and nothing else. Long enough that a slow but healthy
   * DDL is not failed for being slow.
   */
  public static void boundLockWait(final Statement stmt) throws SQLException {
    final DatabaseEngine engine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();

    if (DatabaseEngine.MYSQL.equals(engine)) {
      stmt.execute("SET SESSION lock_wait_timeout = " + LOCK_WAIT_SECONDS);
    } else if (DatabaseEngine.PG.equals(engine)) {
      stmt.execute("SET lock_timeout = '" + LOCK_WAIT_SECONDS + "s'");
    }
  }

  private static Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  private static int queryMaxPreparedTransactions() throws SQLException {
    try (final Connection conn = openConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SHOW max_prepared_transactions")) {
      if (!rs.next()) {
        return 0;
      }
      final String value = rs.getString(1);
      try {
        return Integer.parseInt(value.trim());
      } catch (final NumberFormatException e) {
        LOGGER.finest(() -> "Unexpected max_prepared_transactions value: " + value);
        return 0;
      }
    }
  }
}
