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

import static org.junit.jupiter.api.Assertions.assertFalse;

import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.gdbfailover.GlobalDbFailoverConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@EnableOnNumOfInstances(min = 2)
@MakeSureFirstInstanceWriter
@Order(16)
public class GdbFailoverTest extends FailoverTest {

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  @Disabled
  @Override
  public void test_readerFailover_readerOrWriter() throws SQLException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "home-reader-or-writer");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "home-reader-or-writer");

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()),
            props)) {
      ProxyHelper.disableConnectivity(initialWriterId);

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  @Override
  public void test_readerFailover_strictReader() throws SQLException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-home-reader");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-home-reader");

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()),
            props)) {
      auroraUtil.crashInstance(executor, initialWriterId);

      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);

      String currentConnectionId = auroraUtil.queryInstanceId(conn);
      assertFalse(auroraUtil.isDBInstanceWriter(currentConnectionId));
    }
  }

  @TestTemplate
  @EnableOnNumOfInstances(min = 2)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  @Override
  public void test_readerFailover_writerReelected() throws SQLException {
    final String initialWriterId = this.currentWriter;
    TestInstanceInfo initialWriterInstanceInfo =
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstance(initialWriterId);

    final Properties props = initDefaultProxiedProps();
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "home-reader-or-writer");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "home-reader-or-writer");

    try (final Connection conn =
        DriverManager.getConnection(
            ConnectionStringHelper.getWrapperUrl(
                initialWriterInstanceInfo.getHost(),
                initialWriterInstanceInfo.getPort(),
                TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName()),
            props)) {
      // Failover usually changes the writer instance, but we want to test re-election of the same writer, so we will
      // simulate this by temporarily disabling connectivity to the writer.
      auroraUtil.simulateTemporaryFailure(executor, initialWriterId);
      auroraUtil.assertFirstQueryThrows(conn, FailoverSuccessSQLException.class);
    }
  }

  @Override
  protected String getFailoverPlugin() {
    return "gdbFailover";
  }

  @Override
  protected Properties initDefaultProxiedProps() {
    final Properties props = super.initDefaultProxiedProps();

    // These settings mimic failover/failover2 plugin logic when connecting to non-GDB Aurora or RDS DB clusters.
    props.setProperty(GlobalDbFailoverConnectionPlugin.ACTIVE_HOME_FAILOVER_MODE.name, "strict-writer");
    props.setProperty(GlobalDbFailoverConnectionPlugin.INACTIVE_HOME_FAILOVER_MODE.name, "strict-writer");
    props.remove(FailoverConnectionPlugin.FAILOVER_MODE.name);

    return props;
  }

}
