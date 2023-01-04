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

import static java.util.Arrays.asList;

import integration.refactored.DriverHelper;
import integration.refactored.GenericTypedParameterResolver;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.condition.EnableBasedOnEnvironmentFeatureExtension;
import integration.refactored.container.condition.EnableBasedOnTestDriverExtension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class TestDriverProvider implements TestTemplateInvocationContextProvider {

  private static final Logger LOGGER = Logger.getLogger(TestDriverProvider.class.getName());

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {

    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();
    for (TestDriver testDriver : TestEnvironment.getCurrent().getAllowedTestDrivers()) {
      TestTemplateInvocationContext testTemplateInvocationContext =
          getEnvironment(context, testDriver);
      if (testTemplateInvocationContext != null) {
        resultContextList.add(testTemplateInvocationContext);
      }
    }
    return Arrays.stream(resultContextList.toArray(new TestTemplateInvocationContext[0]));
  }

  private TestTemplateInvocationContext getEnvironment(
      ExtensionContext context, final TestDriver testDriver) {
    return new TestTemplateInvocationContext() {
      @Override
      public String getDisplayName(int invocationIndex) {
        return String.format(
            "[%d] - [Driver: %-7s] - [%s]",
            invocationIndex,
            testDriver,
            TestEnvironment.getCurrent().getInfo().getRequest().getDisplayName());
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
        return asList(
            new GenericTypedParameterResolver(testDriver),
            new EnableBasedOnTestDriverExtension(testDriver),
            new EnableBasedOnEnvironmentFeatureExtension(testDriver),
            new BeforeEachCallback() {
              @Override
              public void beforeEach(ExtensionContext context) throws Exception {
                DriverHelper.unregisterAllDrivers();
                DriverHelper.registerDriver(testDriver);
                TestEnvironment.getCurrent().setCurrentDriver(testDriver);
                LOGGER.finest("Registered " + testDriver + " driver.");

                if (TestEnvironment.getCurrent()
                    .getInfo()
                    .getRequest()
                    .getFeatures()
                    .contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
                  // Enable all proxies
                  ProxyHelper.enableAllConnectivity();
                }
              }
            });
      }
    };
  }
}
