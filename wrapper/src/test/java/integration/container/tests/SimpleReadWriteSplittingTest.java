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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
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
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.plugin.srw.SimpleReadWriteSplittingPlugin;

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
@Order(15)


public class SimpleReadWriteSplittingTest extends ReadWriteSplittingTests {
  String pluginCode = "srw";
  String pluginCodesWithFailover = "failover,efm2,srw,initialConnection";

  protected Properties getSrwProps(boolean proxied, String plugins) {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, plugins);
    if (proxied) {
      props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, getWriterEndpoint());
      props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, getReaderEndpoint());
    } else {
      props.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");
      props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, getWriterEndpoint());
      props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, getReaderEndpoint());
    }
    return props;
  }

  @TestTemplate
  public void test_NoReaderEndpoint() throws SQLException {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, pluginCode);
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, getWriterEndpoint());
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getProxyWrapperUrl(), props)) {
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);

      // Switch to reader successfully
      conn.setReadOnly(true);
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      // Should stay on writer as fallback since reader endpoint does not exist.
      assertEquals(writerConnectionId, readerConnectionId);

      // Going to the write endpoint will be the same connection again.
      conn.setReadOnly(false);
      final String finalConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, finalConnectionId);
    }
  }

  @TestTemplate
  public void test_autoCommitStatePreserved_acrossConnectionSwitches() throws SQLException {
    try (final Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), getProps())) {

      // Set autoCommit to false on writer
      conn.setAutoCommit(false);
      assertFalse(conn.getAutoCommit());
      final String writerConnectionId = auroraUtil.queryInstanceId(conn);
      conn.commit();

      // Switch to reader - autoCommit should remain false
      conn.setReadOnly(true);
      assertFalse(conn.getAutoCommit());
      final String readerConnectionId = auroraUtil.queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      // Change autoCommit on reader
      conn.setAutoCommit(true);
      assertTrue(conn.getAutoCommit());

      // Switch back to writer - autoCommit should be true
      conn.setReadOnly(false);
      assertTrue(conn.getAutoCommit());
      final String finalWriterConnectionId = auroraUtil.queryInstanceId(conn);
      assertEquals(writerConnectionId, finalWriterConnectionId);
    }
  }

  @Override
  protected Properties getProps() {
    return getSrwProps(false, pluginCode);
  }

  @Override
  protected Properties getProxiedPropsWithFailover() {
    final Properties props = getSrwProps(true, pluginCodesWithFailover);
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    return props;
  }

  @Override
  protected Properties getProxiedProps() {
    final Properties props = getSrwProps(true, pluginCode);
    AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    return props;
  }

  @Override
  protected Properties getPropsWithFailover() {
    return getSrwProps(false, pluginCodesWithFailover);
  }

  @Override
  protected boolean connectedToCorrectReaderInstance(String readerConnectionId, String currentConnectionId) {
    // On conn.setReadOnly(true), the SimpleReadWriteSplittingPlugin ensures connection to the reader endpoint.
    // If connected to a reader instance originally, the connection will change: return true no matter values.
    return true;
  }

  @Override
  protected String getWriterEndpoint() {
    return TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getClusterEndpoint();
  }

  @TestTemplate
  @Disabled("Skipping because it's not applicable to SimpleReadWriteSplitting.")
  @Override
  public void test_failoverToNewReader_setReadOnlyFalseTrue() {
    // Skip this test for simple read write splitting as disabling connectivity to a reader cluster endpoint does not
    // trigger reader to reader failover but rather forces defaulting to the writer.
  }

  @TestTemplate
  @Disabled("Skipping because it's not applicable to SimpleReadWriteSplitting.")
  @Override
  public void test_pooledConnection_leastConnectionsStrategy() {
    // Skip this test for simple read write splitting as there is no reader selection strategy.
  }

  @TestTemplate
  @Disabled("Skipping because it's not applicable to SimpleReadWriteSplitting.")
  @Override
  public void test_pooledConnection_leastConnectionsWithPoolMapping() {
    // Skip this test for simple read write splitting as there is no reader selection strategy.
  }
}

