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

import integration.DatabaseEngine;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.XAConnection;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperXADataSource;

/**
 * Integration tests that run a real two-resource, two-phase-commit transaction through
 * {@link AwsWrapperXADataSource}, coordinated by a real transaction manager. Each scenario runs
 * against BOTH Narayana and Atomikos to confirm compatibility with both, exercising each
 * transaction manager the idiomatic way: Narayana with manual {@code enlistResource}, and Atomikos
 * through its {@code AtomikosDataSourceBean} (which registers the resource for recovery and enlists
 * automatically).
 *
 * <p>Restricted to PostgreSQL and MySQL (which provide XA datasource implementations). Requires a
 * real database environment to execute.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnDatabaseEngine({DatabaseEngine.PG, DatabaseEngine.MYSQL})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(25)
@Tag("xa")
public class XaTwoPhaseCommitTest {

  private static final String TABLE_1 = "xa_2pc_table_1";
  private static final String TABLE_2 = "xa_2pc_table_2";

  /** Ensures each Atomikos resource gets a unique name across test iterations. */
  private static final AtomicLong RESOURCE_SEQ = new AtomicLong();

  /** The transaction managers exercised by these tests. */
  enum Tm {
    /** Narayana, driven with manual {@code enlistResource}/{@code delistResource}. */
    NARAYANA {
      @Override
      void runTwoResourceTransaction(
          final AwsWrapperXADataSource ds1,
          final AwsWrapperXADataSource ds2,
          final String table1,
          final String table2,
          final int id,
          final boolean commit)
          throws Exception {
        final TransactionManager tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
        XAConnection xaConn1 = null;
        XAConnection xaConn2 = null;
        try {
          xaConn1 = ds1.getXAConnection();
          xaConn2 = ds2.getXAConnection();

          // Obtain the logical connection handles before enlisting, matching how a managed
          // environment checks out the connection: this establishes the wrapper pipeline over the
          // target XA connection and leaves the physical session clean so XA START succeeds.
          final Connection conn1 = xaConn1.getConnection();
          final Connection conn2 = xaConn2.getConnection();
          // getXAResource() must be called once and the same instance reused for enlist/delist so
          // the transaction manager tracks a single resource per branch.
          final XAResource res1 = xaConn1.getXAResource();
          final XAResource res2 = xaConn2.getXAResource();

          tm.begin();
          final Transaction txn = tm.getTransaction();
          txn.enlistResource(res1);
          txn.enlistResource(res2);

          try (final Statement s1 = conn1.createStatement()) {
            s1.executeUpdate("INSERT INTO " + table1 + " (id) VALUES (" + id + ")");
          }
          try (final Statement s2 = conn2.createStatement()) {
            s2.executeUpdate("INSERT INTO " + table2 + " (id) VALUES (" + id + ")");
          }

          txn.delistResource(res1, XAResource.TMSUCCESS);
          txn.delistResource(res2, XAResource.TMSUCCESS);

          if (commit) {
            tm.commit();
          } else {
            tm.rollback();
          }
        } finally {
          if (xaConn1 != null) {
            xaConn1.close();
          }
          if (xaConn2 != null) {
            xaConn2.close();
          }
        }
      }
    },

    /** Atomikos, driven through {@code AtomikosDataSourceBean} (idiomatic JTA usage). */
    ATOMIKOS {
      @Override
      void runTwoResourceTransaction(
          final AwsWrapperXADataSource ds1,
          final AwsWrapperXADataSource ds2,
          final String table1,
          final String table2,
          final int id,
          final boolean commit)
          throws Exception {
        final com.atomikos.jdbc.AtomikosDataSourceBean bean1 = newBean(ds1);
        final com.atomikos.jdbc.AtomikosDataSourceBean bean2 = newBean(ds2);
        final com.atomikos.icatch.jta.UserTransactionManager utm =
            new com.atomikos.icatch.jta.UserTransactionManager();
        utm.setForceShutdown(true);
        utm.init();
        try {
          utm.begin();
          boolean thrown = true;
          try {
            // AtomikosDataSourceBean.getConnection() enlists the resource in the active JTA
            // transaction automatically. Closing the handle returns it to the pool but keeps the
            // branch enlisted until the transaction completes.
            try (final Connection c1 = bean1.getConnection();
                final Statement s1 = c1.createStatement()) {
              s1.executeUpdate("INSERT INTO " + table1 + " (id) VALUES (" + id + ")");
            }
            try (final Connection c2 = bean2.getConnection();
                final Statement s2 = c2.createStatement()) {
              s2.executeUpdate("INSERT INTO " + table2 + " (id) VALUES (" + id + ")");
            }
            thrown = false;
          } finally {
            if (commit && !thrown) {
              utm.commit();
            } else {
              utm.rollback();
            }
          }
        } finally {
          bean1.close();
          bean2.close();
          utm.close();
        }
      }

      private com.atomikos.jdbc.AtomikosDataSourceBean newBean(final AwsWrapperXADataSource ds) {
        final com.atomikos.jdbc.AtomikosDataSourceBean bean =
            new com.atomikos.jdbc.AtomikosDataSourceBean();
        bean.setUniqueResourceName("xa-res-" + RESOURCE_SEQ.incrementAndGet());
        bean.setXaDataSource(ds);
        bean.setMinPoolSize(1);
        bean.setMaxPoolSize(1);
        return bean;
      }
    };

    abstract void runTwoResourceTransaction(
        AwsWrapperXADataSource ds1,
        AwsWrapperXADataSource ds2,
        String table1,
        String table2,
        int id,
        boolean commit)
        throws Exception;
  }

  private AwsWrapperXADataSource createXaDataSource() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setTargetDataSourceClassName(DriverHelper.getXaDataSourceClassname());
    ds.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());
    ds.setUser(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    // No connection-switching / Aurora plugins on the XA path.
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  private Connection openPlainConnection() throws SQLException {
    return java.sql.DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  private void recreateTables() throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_1);
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_2);
      stmt.execute("CREATE TABLE " + TABLE_1 + " (id INT NOT NULL PRIMARY KEY)");
      stmt.execute("CREATE TABLE " + TABLE_2 + " (id INT NOT NULL PRIMARY KEY)");
    }
  }

  private int countRows(final String table, final int id) throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE id = " + id)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @TestTemplate
  public void test_twoResourceCommit_bothTransactionManagers() throws Exception {
    for (final Tm tm : Tm.values()) {
      recreateTables();
      final int id = 10;
      tm.runTwoResourceTransaction(createXaDataSource(), createXaDataSource(), TABLE_1, TABLE_2, id, true);

      assertEquals(1, countRows(TABLE_1, id), tm + ": table 1 should be committed");
      assertEquals(1, countRows(TABLE_2, id), tm + ": table 2 should be committed");
    }
  }

  @TestTemplate
  public void test_twoResourceRollback_bothTransactionManagers() throws Exception {
    for (final Tm tm : Tm.values()) {
      recreateTables();
      final int id = 20;
      tm.runTwoResourceTransaction(createXaDataSource(), createXaDataSource(), TABLE_1, TABLE_2, id, false);

      assertEquals(0, countRows(TABLE_1, id), tm + ": table 1 should be rolled back");
      assertEquals(0, countRows(TABLE_2, id), tm + ": table 2 should be rolled back");
    }
  }
}
