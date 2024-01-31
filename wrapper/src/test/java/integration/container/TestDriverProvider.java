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

package integration.container;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.commons.util.AnnotationUtils.isAnnotated;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.entities.Segment;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.GenericTypedParameterResolver;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestEnvironmentRequest;
import integration.TestInstanceInfo;
import integration.container.aurora.TestAuroraHostListProvider;
import integration.container.aurora.TestPluginServiceImpl;
import integration.container.condition.EnableBasedOnEnvironmentFeatureExtension;
import integration.container.condition.EnableBasedOnTestDriverExtension;
import integration.container.condition.MakeSureFirstInstanceWriter;
import integration.util.AuroraTestUtility;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.AnnotationUtils;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.plugin.efm.MonitorThreadContainer;
import software.amazon.jdbc.plugin.efm2.MonitorServiceImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.RdsUtils;

public class TestDriverProvider implements TestTemplateInvocationContextProvider {
  private static final Logger LOGGER = Logger.getLogger(TestDriverProvider.class.getName());

  private static final String POSTGRES_AUTH_ERROR_CODE = "28P01";

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
            "[%d] - %s - [Driver: %-7s] - [%s]",
            invocationIndex,
            context.getDisplayName(),
            testDriver,
            TestEnvironment.getCurrent().getInfo().getRequest().getDisplayName());
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
        return asList(
            new GenericTypedParameterResolver<>(testDriver),
            new EnableBasedOnTestDriverExtension(testDriver),
            new EnableBasedOnEnvironmentFeatureExtension(testDriver),
            new BeforeEachCallback() {
              @Override
              public void beforeEach(ExtensionContext context) throws Exception {
                boolean tracesEnabled = TestEnvironment.getCurrent()
                    .getInfo()
                    .getRequest()
                    .getFeatures()
                    .contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED);

                if (tracesEnabled) {
                  String testName = null;
                  if (context.getElement().isPresent() && context.getElement().get() instanceof Method) {
                    Method method = (Method) context.getElement().get();
                    testName = method.getDeclaringClass().getSimpleName() + "." + method.getName();
                  }

                  Segment segment = AWSXRay.beginSegment("test: setup");
                  segment.putAnnotation("engine",
                      TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine().toString());
                  segment.putAnnotation("deployment",
                      TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment().toString());
                  segment.putAnnotation("targetJVM",
                      TestEnvironment.getCurrent().getInfo().getRequest().getTargetJvm().toString());
                  if (testName != null) {
                    segment.putAnnotation("testName", testName);
                  }
                  LOGGER.finest("[XRay] Test setup trace ID: " + segment.getTraceId());
                }

                DriverHelper.unregisterAllDrivers();
                DriverHelper.registerDriver(testDriver);
                TestEnvironment.getCurrent().setCurrentDriver(testDriver);
                LOGGER.finest("Registered " + testDriver + " driver.");

                TestEnvironmentInfo testInfo = TestEnvironment.getCurrent().getInfo();
                TestEnvironmentRequest testRequest = testInfo.getRequest();
                if (testRequest.getFeatures()
                    .contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
                  // Enable all proxies
                  ProxyHelper.enableAllConnectivity();
                }

                if (testRequest.getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA) {
                  AuroraTestUtility auroraUtil = new AuroraTestUtility(testInfo.getAuroraRegion());
                  auroraUtil.waitUntilClusterHasRightState(testInfo.getAuroraClusterName());

                  boolean makeSureFirstInstanceWriter =
                      AnnotationUtils.isAnnotated(context.getElement(), MakeSureFirstInstanceWriter.class)
                      || isAnnotated(context.getTestClass(), MakeSureFirstInstanceWriter.class);
                  List<String> instanceIDs;
                  if (makeSureFirstInstanceWriter) {

                    instanceIDs = new ArrayList<>();

                    // Need to ensure that cluster details through API matches topology fetched through SQL
                    // Wait up to 5min
                    long startTimeNano = System.nanoTime();
                    while ((instanceIDs.size() != testRequest.getNumOfInstances()
                        || instanceIDs.size() == 0
                        || !auroraUtil.isDBInstanceWriter(instanceIDs.get(0)))
                        && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {

                      Thread.sleep(5000);

                      try {
                        instanceIDs = auroraUtil.getAuroraInstanceIds();
                      } catch (SQLException ex) {
                        if (POSTGRES_AUTH_ERROR_CODE.equals(ex.getSQLState())) {
                          // This authentication error for PG is caused by test environment configuration.
                          throw ex;
                        }
                        instanceIDs = new ArrayList<>();
                      }
                    }
                    assertTrue(instanceIDs.size() > 0);
                    assertTrue(
                        auroraUtil.isDBInstanceWriter(
                            testInfo.getAuroraClusterName(),
                            instanceIDs.get(0)));
                    String currentWriter = instanceIDs.get(0);

                    // Adjust database info to reflect a current writer and to move corresponding
                    // instance to position 0.
                    TestDatabaseInfo dbInfo = testInfo.getDatabaseInfo();
                    dbInfo.moveInstanceFirst(currentWriter);
                    testInfo.getProxyDatabaseInfo().moveInstanceFirst(currentWriter);

                    // Wait for cluster URL to resolve to the writer
                    startTimeNano = System.nanoTime();
                    String clusterInetAddress = auroraUtil.hostToIP(dbInfo.getClusterEndpoint());
                    String writerInetAddress =
                        auroraUtil.hostToIP(dbInfo.getInstances().get(0).getHost());
                    while (!writerInetAddress.equals(clusterInetAddress)
                        && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startTimeNano) < 5) {
                      clusterInetAddress = auroraUtil.hostToIP(dbInfo.getClusterEndpoint());
                      writerInetAddress =
                          auroraUtil.hostToIP(dbInfo.getInstances().get(0).getHost());
                      Thread.sleep(5000);
                    }
                    assertTrue(writerInetAddress.equals(clusterInetAddress));
                  } else {
                    instanceIDs =
                        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances()
                            .stream().map(TestInstanceInfo::getInstanceId)
                            .collect(Collectors.toList());
                  }

                  auroraUtil.makeSureInstancesUp(instanceIDs);
                  TestAuroraHostListProvider.clearCache();
                  TestPluginServiceImpl.clearHostAvailabilityCache();
                  DialectManager.resetEndpointCache();
                  TargetDriverDialectManager.resetCustomDialect();
                  MonitorThreadContainer.releaseInstance();
                  MonitorServiceImpl.clearCache();
                  software.amazon.jdbc.plugin.efm2.MonitorServiceImpl.clearCache();
                  RdsUtils.clearCache();
                }
                if (tracesEnabled) {
                    AWSXRay.endSegment();
                }
              }
            },
            new AfterEachCallback() {
              @Override
              public void afterEach(ExtensionContext context) throws Exception {
                Set<TestEnvironmentFeatures> features = TestEnvironment.getCurrent()
                    .getInfo()
                    .getRequest()
                    .getFeatures();

                if (features.contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)
                    || features.contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {

                  TimeUnit.SECONDS.sleep(3); // let OTLP container to send all collected metrics and traces
                }
              }
            });
      }
    };
  }
}
