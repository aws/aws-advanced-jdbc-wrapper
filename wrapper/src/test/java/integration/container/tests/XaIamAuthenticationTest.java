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

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngine;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperXADataSource;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;

/**
 * Integration test proving that IAM database authentication works through
 * {@link AwsWrapperXADataSource}. The IAM authentication plugin runs in the wrapper's connect
 * pipeline for the XA connection, generates a short-lived token, and that token must reach the
 * target driver's {@code XADataSource} so the physical XA connection authenticates. This exercises
 * the wrapper's connect-time value on the XA path (IAM auth is one of the reasons to use the XA
 * datasource even though connection-switching plugins are not supported there).
 *
 * <p>Requires an IAM-enabled RDS/Aurora environment (feature {@code IAM}). Restricted to PostgreSQL
 * and MySQL (which provide XA datasource implementations). The MariaDB driver is disabled because it
 * does not force {@code mysql_clear_password} authentication, which IAM requires (see
 * {@code AwsIamIntegrationTest}).
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.IAM)
@EnableOnDatabaseEngine({DatabaseEngine.PG, DatabaseEngine.MYSQL})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(26)
@Tag("xa")
public class XaIamAuthenticationTest {

  @BeforeEach
  void clearIamCache() {
    IamAuthConnectionPlugin.clearCache();
  }

  private AwsWrapperXADataSource createIamXaDataSource() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setTargetDataSourceClassName(DriverHelper.getXaDataSourceClassname());
    ds.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());

    final Properties props = new Properties();
    // IAM only: no password. The IAM plugin generates a token and sets it as the password; the XA
    // connection provider applies it to the target XADataSource before opening the physical session.
    props.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    props.setProperty(
        IamAuthConnectionPlugin.IAM_REGION.name, TestEnvironment.getCurrent().getInfo().getRegion());
    // The IAM username must be on the pipeline properties so the plugin generates a token for it.
    props.setProperty(
        PropertyDefinition.USER.name, TestEnvironment.getCurrent().getInfo().getIamUsername());
    props.setProperty(PropertyDefinition.TCP_KEEP_ALIVE.name, "false");
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  /** A valid IAM-authenticated logical connection can be obtained from the XA datasource. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iamAuth_getConnection() throws Exception {
    final AwsWrapperXADataSource ds = createIamXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      assertTrue(conn.isValid(10), "IAM-authenticated XA connection should be valid");
    } finally {
      xaConn.close();
    }
  }

  /** The XA control surface works over an IAM-authenticated XA connection. */
  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void test_iamAuth_xaBranchLifecycle() throws Exception {
    final AwsWrapperXADataSource ds = createIamXaDataSource();
    final XAConnection xaConn = ds.getXAConnection();
    try {
      final Connection conn = xaConn.getConnection();
      final XAResource xaResource = xaConn.getXAResource();
      final Xid xid = new TestXid(1);

      xaResource.start(xid, XAResource.TMNOFLAGS);
      try (final Statement stmt = conn.createStatement();
          final ResultSet rs = stmt.executeQuery("SELECT 1")) {
        assertTrue(rs.next());
      }
      xaResource.end(xid, XAResource.TMSUCCESS);

      final int prepareResult = xaResource.prepare(xid);
      // A read-only branch is fully resolved by prepare (XA_RDONLY); otherwise commit it.
      if (prepareResult == XAResource.XA_OK) {
        xaResource.commit(xid, false);
      }
    } finally {
      xaConn.close();
    }
  }

  /** A minimal {@link Xid} implementation for driving an XA branch directly in tests. */
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
