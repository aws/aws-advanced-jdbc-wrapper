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

import integration.TestEnvironmentFeatures;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import java.util.Properties;

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

  @Override
  protected Properties getProps() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "srw");
    props.setProperty("readWriteEndpoint", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
    props.setProperty("readOnlyEndpoint", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterReadOnlyEndpoint());
    return props;
  }

  @Override
  protected Properties getPropsWithFailover() {
    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "failover,efm2,srw");
    props.setProperty("readWriteEndpoint", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
    props.setProperty("readOnlyEndpoint", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterReadOnlyEndpoint());
    return props;
  }

  @Override
  protected boolean connectedToCorrectReaderInstance(String readerConnectionId, String currentConnectionId) {
    // On conn.setReadOnly(true), the SimpleReadWriteSplittingPlugin ensures connection to the reader endpoint.
    // If connected to a reader instance originally, the connection will change: return true no matter values.
    return true;
  }
}

