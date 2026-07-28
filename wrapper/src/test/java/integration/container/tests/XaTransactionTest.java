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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnNumOfInstances;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperXADataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * Integration tests that run real XA (two-phase commit) transactions through
 * {@link AwsWrapperXADataSource} against a real database, driving the {@link XAResource} lifecycle
 * directly (no external transaction manager). Restricted to PostgreSQL and MySQL, which provide XA
 * datasource implementations.
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
@Order(24)
@Tag("xa")
public class XaTransactionTest {

  private static final String TABLE = "xa_test_table";

  private AwsWrapperXADataSource createXaDataSource() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setTargetDataSourceClassName(DriverHelper.getXaDataSourceClassname());
    ds.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());
    ds.setUser(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    // No connection-switching plugins on the XA path (see the two-datasource pattern). An empty
    // plugin list also avoids the default Aurora/topology plugins interfering with XA semantics
    // against a plain database.
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  private void recreateTable() throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE);
      stmt.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL PRIMARY KEY)");
    }
  }

  private Connection openPlainConnection() throws SQLException {
    return java.sql.DriverManager.getConnection(
        ConnectionStringHelper.getWrapperUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  private int countRows(final int id) throws SQLException {
    try (final Connection conn = openPlainConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE + " WHERE id = " + id)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @TestTemplate
  public void test_getConnection_returnsPipelinedWrapper() throws SQLException {
    final AwsWrapperXADataSource ds = createXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      assertTrue(conn instanceof ConnectionWrapper);
      assertNotNull(xaConn.getXAResource());
    } finally {
      xaConn.close();
    }
  }

  @TestTemplate
  public void test_xaLifecycle_commitPersists() throws Exception {
    recreateTable();
    final int id = 1;
    final AwsWrapperXADataSource ds = createXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(1);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (" + id + ")");
      }
      xaResource.end(xid, XAResource.TMSUCCESS);

      final int prepareResult = xaResource.prepare(xid);
      assertTrue(prepareResult == XAResource.XA_OK || prepareResult == XAResource.XA_RDONLY);
      xaResource.commit(xid, false);
    } finally {
      xaConn.close();
    }

    assertEquals(1, countRows(id));
  }

  @TestTemplate
  public void test_xaLifecycle_rollbackDiscards() throws Exception {
    recreateTable();
    final int id = 2;
    final AwsWrapperXADataSource ds = createXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(2);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (" + id + ")");
      }
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.prepare(xid);
      xaResource.rollback(xid);
    } finally {
      xaConn.close();
    }

    assertEquals(0, countRows(id));
  }

  @TestTemplate
  public void test_xaLifecycle_onePhaseCommit() throws Exception {
    recreateTable();
    final int id = 3;
    final AwsWrapperXADataSource ds = createXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(3);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (" + id + ")");
      }
      xaResource.end(xid, XAResource.TMSUCCESS);
      // One-phase commit: no separate prepare.
      xaResource.commit(xid, true);
    } finally {
      xaConn.close();
    }

    assertEquals(1, countRows(id));
  }

  @TestTemplate
  public void test_recover_returnsPreparedBranch() throws Exception {
    recreateTable();
    final int id = 4;
    final AwsWrapperXADataSource ds = createXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(4);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (" + id + ")");
      }
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.prepare(xid);

      // The prepared (in-doubt) branch should be discoverable via recover and resolvable by Xid.
      final Xid[] recovered = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      final boolean found = recovered != null && Arrays.stream(recovered).anyMatch(r -> xidEquals(r, xid));
      assertTrue(found, "prepared branch should be returned by recover()");

      xaResource.commit(xid, false);
    } finally {
      xaConn.close();
    }

    assertEquals(1, countRows(id));
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  public void test_readWriteSplittingDoesNotSwitchDuringXaBranch() throws Exception {
    final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
    final AwsWrapperXADataSource ds = createXaDataSource();
    // Enable read/write splitting on the XA datasource; it must be skipped during the XA branch.
    final Properties targetProps = new Properties();
    targetProps.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting");
    ds.setTargetDataSourceProperties(targetProps);

    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(5);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      final String instanceInBranch = auroraUtil.queryInstanceId(conn);

      // Requesting read-only inside an XA branch must NOT switch to a reader: the transaction-aware
      // gate pins the physical connection for the branch. If the connection had switched, the
      // reported instance id would change. We assert on the instance id rather than performing a
      // write, because when the switch is skipped the underlying driver still applies read-only to
      // the pinned session, which would block a subsequent INSERT (that is expected driver behavior,
      // not a switch).
      conn.setReadOnly(true);
      final String instanceAfterSetReadOnly = auroraUtil.queryInstanceId(conn);
      assertEquals(instanceInBranch, instanceAfterSetReadOnly,
          "read/write splitting must not switch the connection during an XA branch");

      conn.setReadOnly(false);
      xaResource.end(xid, XAResource.TMSUCCESS);
      xaResource.rollback(xid);
    } finally {
      xaConn.close();
    }
  }

  private static boolean xidEquals(final Xid a, final Xid b) {
    return a.getFormatId() == b.getFormatId()
        && Arrays.equals(a.getGlobalTransactionId(), b.getGlobalTransactionId())
        && Arrays.equals(a.getBranchQualifier(), b.getBranchQualifier());
  }

  /** A minimal {@link Xid} implementation for driving XA transactions directly in tests. */
  static final class TestXid implements Xid {
    private static final int FORMAT_ID = 0xA5A5;
    private final byte[] gtrid;
    private final byte[] bqual;

    TestXid(final int seed) {
      this.gtrid = new byte[] {(byte) seed, 0x1, 0x2, 0x3};
      this.bqual = new byte[] {(byte) seed, 0x4, 0x5, 0x6};
    }

    @Override
    public int getFormatId() {
      return FORMAT_ID;
    }

    @Override
    public byte[] getGlobalTransactionId() {
      return this.gtrid.clone();
    }

    @Override
    public byte[] getBranchQualifier() {
      return this.bqual.clone();
    }
  }
}
