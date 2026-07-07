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
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperXADataSource;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.failover.XaFailoverNotSupportedSQLException;

/**
 * Integration test verifying that a failover forced during an active XA branch surfaces the
 * XA-specific fail-fast exception ({@link XaFailoverNotSupportedSQLException}), and that a new XA
 * transaction started afterward succeeds against the promoted writer. Requires a real Aurora/RDS
 * environment with failover support and multiple instances.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@EnableOnDatabaseEngine({DatabaseEngine.PG, DatabaseEngine.MYSQL})
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@MakeSureFirstInstanceWriter
@Order(26)
@Tag("xa")
public class XaFailoverTest {

  private static final String TABLE = "xa_failover_table";
  private static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  private ExecutorService executor;
  private String currentWriter;

  @BeforeEach
  public void setUpEach() {
    this.currentWriter =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances().get(0).getInstanceId();
    this.executor = Executors.newFixedThreadPool(1, r -> {
      final Thread thread = new Thread(r);
      thread.setDaemon(true);
      return thread;
    });
  }

  @AfterEach
  public void tearDownEach() {
    this.executor.shutdownNow();
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_failoverDuringXaBranch_failsFast() throws Exception {
    recreateTable();

    final TestInstanceInfo writer =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(this.currentWriter);

    final AwsWrapperXADataSource ds = createProxiedFailoverXaDataSource(writer);
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new XaTransactionTest.TestXid(30);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (30)");
      }

      // Crash the writer; the next statement triggers driver failover, which must fail fast because
      // an XA branch is active (the branch cannot be preserved across a node change).
      auroraUtil.crashInstance(executor, this.currentWriter);
      auroraUtil.assertFirstQueryThrows(conn, XaFailoverNotSupportedSQLException.class);

      // The transaction manager would now roll the branch back.
      try {
        xaResource.end(xid, XAResource.TMFAIL);
      } catch (final Exception ignore) {
        // The connection is broken; the branch is abandoned and will be rolled back / recovered.
      }
      try {
        xaResource.rollback(xid);
      } catch (final Exception ignore) {
        // Best-effort rollback on a broken connection.
      }
    } finally {
      try {
        xaConn.close();
      } catch (final SQLException ignore) {
        // ignore
      }
    }

    // A new XA transaction started after failover should succeed against the promoted writer.
    // Use the cluster endpoint so the wrapper connects to the current writer.
    final AwsWrapperXADataSource newDs = new AwsWrapperXADataSource();
    newDs.setTargetDataSourceClassName(DriverHelper.getXaDataSourceClassname());
    newDs.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());
    newDs.setUser(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    newDs.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    final Properties newProps = new Properties();
    newProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    newDs.setTargetDataSourceProperties(newProps);

    final XAConnection xaConn2 = newDs.getXAConnection();
    try {
      final Connection conn = xaConn2.getConnection();
      final XAResource xaResource = xaConn2.getXAResource();
      final Xid xid = new XaTransactionTest.TestXid(31);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (31)");
      }
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.prepare(xid);
      xaResource.commit(xid, false);
    } finally {
      xaConn2.close();
    }

    assertEquals(1, countRows(31), "a new XA transaction after failover should commit on the promoted writer");
  }

  /**
   * Opens an XA branch, writes a row, then forces a cluster failover before the branch is prepared or
   * committed. Because the physical session cannot be moved to the promoted node, the wrapper fails
   * fast and the unprepared branch is rolled back by the database. The test asserts the row is NOT
   * present on the promoted writer, i.e. the XA transaction was effectively rolled back.
   */
  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_failoverDuringXaBranch_transactionIsRolledBack() throws Exception {
    recreateTable();
    final int id = 40;

    final TestInstanceInfo writer =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(this.currentWriter);

    final AwsWrapperXADataSource ds = createProxiedFailoverXaDataSource(writer);
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new XaTransactionTest.TestXid(id);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (" + id + ")");
      }

      // Crash the writer mid-branch. The in-flight branch was never prepared, so failing fast leaves
      // it to be rolled back by the database (an unprepared XA transaction on a lost session).
      auroraUtil.crashInstance(executor, this.currentWriter);
      auroraUtil.assertFirstQueryThrows(conn, XaFailoverNotSupportedSQLException.class);

      // A transaction manager would now roll the branch back; on a broken connection this is
      // best-effort and the database discards the unprepared branch regardless.
      try {
        xaResource.end(xid, XAResource.TMFAIL);
      } catch (final Exception ignore) {
        // expected on a broken connection
      }
      try {
        xaResource.rollback(xid);
      } catch (final Exception ignore) {
        // expected on a broken connection
      }
    } finally {
      try {
        xaConn.close();
      } catch (final SQLException ignore) {
        // ignore
      }
    }

    // The branch was never prepared/committed, so after failover the promoted writer must not contain
    // the row: the XA transaction was effectively rolled back.
    assertEquals(
        0,
        countRows(id),
        "the XA branch active during failover must be rolled back (its row must not be persisted)");
  }

  private AwsWrapperXADataSource createProxiedFailoverXaDataSource(final TestInstanceInfo writer) {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(writer.getHost());
    ds.setServerPort(Integer.toString(writer.getPort()));
    ds.setDatabase(TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceClassName(DriverHelper.getXaDataSourceClassname());
    ds.setUser(TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getPassword());

    final Properties targetProps = ConnectionStringHelper.getDefaultProperties();
    targetProps.setProperty(PropertyDefinition.PLUGINS.name, "failover2,efm2");
    PropertyDefinition.CONNECT_TIMEOUT.set(targetProps, "10000");
    PropertyDefinition.SOCKET_TIMEOUT.set(targetProps, "2000");
    RdsHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(
        targetProps,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    ds.setTargetDataSourceProperties(targetProps);
    return ds;
  }

  private Connection openPlainConnection() throws SQLException {
    return java.sql.DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  private void recreateTable() throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      stmt.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
    }
  }

  private int countRows(final int id) throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement();
        final java.sql.ResultSet rs =
            stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE + " WHERE id = " + id)) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
