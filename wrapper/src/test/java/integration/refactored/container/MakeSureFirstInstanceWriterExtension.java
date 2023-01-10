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

package integration.refactored.container;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.TestEnvironmentFeatures;
import integration.util.AuroraTestUtility;
import java.util.List;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MakeSureFirstInstanceWriterExtension implements BeforeAllCallback {

  private static final Logger LOGGER =
      Logger.getLogger(MakeSureFirstInstanceWriterExtension.class.getName());

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {

    if (TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment()
        == DatabaseEngineDeployment.AURORA) {
      // Database instances should be rearranged to have a writer node at position 0.
      // Many tests expect a writer node to be specified at the first position in the host list.

      if (!TestEnvironment.getCurrent()
          .getInfo()
          .getRequest()
          .getFeatures()
          .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
        throw new RuntimeException("AWS_CREDENTIALS_ENABLED feature is required for this test.");
      }

      AuroraTestUtility auroraUtil =
          new AuroraTestUtility(TestEnvironment.getCurrent().getInfo().getAuroraRegion());
      auroraUtil.waitUntilClusterHasRightState(
          TestEnvironment.getCurrent().getInfo().getAuroraClusterName());

      List<String> latestTopology = auroraUtil.getAuroraInstanceIds();

      assertTrue(
          auroraUtil.isDBInstanceWriter(
              TestEnvironment.getCurrent().getInfo().getAuroraClusterName(),
              latestTopology.get(0)));
      String currentWriter = latestTopology.get(0);
      LOGGER.finest("Current writer: " + currentWriter);

      // Adjust database info to reflect a current writer and to move corresponding instance to
      // position 0.
      TestEnvironment.getCurrent().getInfo().getDatabaseInfo().moveInstanceFirst(currentWriter);
      TestEnvironment.getCurrent()
          .getInfo()
          .getProxyDatabaseInfo()
          .moveInstanceFirst(currentWriter);
    }
  }
}
