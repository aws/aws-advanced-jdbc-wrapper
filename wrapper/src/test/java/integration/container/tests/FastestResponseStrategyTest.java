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
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.ProxyHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@MakeSureFirstInstanceWriter
@EnableOnDatabaseEngineDeployment({
    DatabaseEngineDeployment.AURORA,
    DatabaseEngineDeployment.RDS,
    DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
    DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE
})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(18)
public class FastestResponseStrategyTest {
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  @EnableOnNumOfInstances(min = 3)
  @EnableOnTestFeature(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)
  public void test_fastestHostAlwaysSelected() throws SQLException, InterruptedException {
    final Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, "10000");
    props.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, "10000");
    RdsHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN.set(props,
        "?." + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointSuffix()
            + ":" + TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstanceEndpointPort());
    props.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,fastestResponseStrategy");
    props.setProperty(ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.name, "fastestResponse");

    String url = ConnectionStringHelper.getProxyWrapperUrl();
    List<TestInstanceInfo> instances = TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getInstances();
    assertTrue(instances.size() >= 3);
    // The writer is stored at index 0, so we'll choose the reader at index 1 to be the fast one.
    TestInstanceInfo fastestReader = instances.get(1);
    // Add latencies to all readers except for the first reader in the list.
    for (int i = 2; i < instances.size(); i++) {
      ProxyHelper.setLatency(instances.get(i).getInstanceId(), 500);
    }

    // Verify that the fastest reader is always selected.
    for (int i = 0; i < 10; i++) {
      try (final Connection conn = DriverManager.getConnection(url, props)) {
        if (i == 0) {
          // Give the monitors some time to determine the response time of each host.
          TimeUnit.SECONDS.sleep(20);
        }

        conn.setReadOnly(true);
        String instanceId = auroraUtil.queryInstanceId(conn);
        assertEquals(fastestReader.getInstanceId(), instanceId);
      }
    }
  }
}

