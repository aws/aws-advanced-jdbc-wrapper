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

  private static int queryMaxPreparedTransactions() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
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
