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
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@EnableOnNumOfInstances(min = 2)
@MakeSureFirstInstanceWriter
@Order(15)
public class Failover2Test extends FailoverTest {

  @Override
  protected String getFailoverPlugin() {
    return "failover2";
  }
}
