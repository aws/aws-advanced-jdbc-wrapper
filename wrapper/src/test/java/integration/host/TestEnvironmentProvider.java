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

package integration.host;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DatabaseInstances;
import integration.GenericTypedParameterResolver;
import integration.TargetJvm;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class TestEnvironmentProvider implements TestTemplateInvocationContextProvider {

  static final ArrayList<EnvPreCreateInfo> preCreateInfos = new ArrayList<>();
  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentProvider.class.getName());

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {

    preCreateInfos.clear();
    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();

    TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();

    for (DatabaseEngineDeployment deployment : DatabaseEngineDeployment.values()) {
      if (deployment == DatabaseEngineDeployment.DOCKER && config.noDocker) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.AURORA && config.noAurora) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS) {
        // Not in use.
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && config.noMultiAzCluster) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE && config.noMultiAzInstance) {
        continue;
      }

      for (DatabaseEngine engine : DatabaseEngine.values()) {
        if (engine == DatabaseEngine.PG && config.noPgEngine) {
          continue;
        }
        if (engine == DatabaseEngine.MYSQL && config.noMysqlEngine) {
          continue;
        }
        if (engine == DatabaseEngine.MARIADB && config.noMariadbEngine) {
          continue;
        }

        for (DatabaseInstances instances : DatabaseInstances.values()) {
          if (deployment == DatabaseEngineDeployment.DOCKER
              && instances != DatabaseInstances.SINGLE_INSTANCE) {
            continue;
          }

          for (int numOfInstances : Arrays.asList(1, 2, 3, 5)) {
            if (instances == DatabaseInstances.SINGLE_INSTANCE && numOfInstances > 1) {
              continue;
            }
            if (instances == DatabaseInstances.MULTI_INSTANCE && numOfInstances == 1) {
              continue;
            }
            if (numOfInstances == 1 && config.noInstances1) {
              continue;
            }
            if (numOfInstances == 2 && config.noInstances2) {
              continue;
            }
            if (numOfInstances == 3 && config.noInstances3) {
              continue;
            }
            if (numOfInstances == 5 && config.noInstances5) {
              continue;
            }
            if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && numOfInstances != 3) {
              // Multi-AZ clusters supports only 3 instances
              continue;
            }
            if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE && numOfInstances != 1) {
              // Multi-AZ Instances supports only 1 instance
              continue;
            }
            if (deployment == DatabaseEngineDeployment.AURORA && numOfInstances == 3) {
              // Aurora supports clusters with 3 instances but running such tests is similar
              // to running tests on 5-instance cluster.
              // Let's save some time and skip tests for this configuration
              continue;
            }

            for (TargetJvm jvm : TargetJvm.values()) {
              if ((jvm == TargetJvm.OPENJDK8 || jvm == TargetJvm.OPENJDK11) && config.noOpenJdk) {
                continue;
              }
              if (jvm == TargetJvm.OPENJDK8 && config.noOpenJdk8) {
                continue;
              }
              if (jvm == TargetJvm.OPENJDK11 && config.noOpenJdk11) {
                continue;
              }
              if (jvm != TargetJvm.OPENJDK11 && config.testHibernateOnly) {
                // Run hibernate tests with OPENJDK11 only.
                continue;
              }
              if (jvm == TargetJvm.GRAALVM && config.noGraalVm) {
                continue;
              }

              for (boolean withBlueGreenFeature : Arrays.asList(true, false)) {
                if (!withBlueGreenFeature) {
                  if (config.testBlueGreenOnly) {
                    continue;
                  }
                }
                if (withBlueGreenFeature) {
                  if (config.noBlueGreen && !config.testBlueGreenOnly) {
                    continue;
                  }
                  // Run BlueGreen test only for MultiAz Instances with 1 node or for Aurora
                  if (deployment != DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE
                      && deployment != DatabaseEngineDeployment.AURORA) {
                    continue;
                  }
                }

                resultContextList.add(
                    getEnvironment(
                        new TestEnvironmentRequest(
                            engine,
                            instances,
                            instances == DatabaseInstances.SINGLE_INSTANCE ? 1 : numOfInstances,
                            deployment,
                            jvm,
                            TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                            deployment == DatabaseEngineDeployment.DOCKER
                                && config.noTracesTelemetry
                                && config.noMetricsTelemetry
                                ? null
                                : TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                            deployment == DatabaseEngineDeployment.DOCKER || config.noFailover
                                ? null
                                : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                            deployment == DatabaseEngineDeployment.DOCKER
                                || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER
                                || config.noIam
                                ? null
                                : TestEnvironmentFeatures.IAM,
                            config.noSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                            config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                            config.noPerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                            config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                            config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                            config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                            config.testHibernateOnly ? TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY : null,
                            config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null,
                            config.noTracesTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED,
                            config.noMetricsTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED,
                            withBlueGreenFeature ? TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT : null)));
              }
            }
          }
        }
      }
    }

    int index = 1;
    for (TestTemplateInvocationContext testTemplateInvocationContext : resultContextList) {
      LOGGER.finest(
          "Added to the test queue: " + testTemplateInvocationContext.getDisplayName(index++));
    }

    return Arrays.stream(resultContextList.toArray(new TestTemplateInvocationContext[0]));
  }

  private TestTemplateInvocationContext getEnvironment(TestEnvironmentRequest info) {
    return new AwsWrapperTestTemplateInvocationContext(info) {

      @Override
      public String getDisplayName(int invocationIndex) {
        return String.format("[%d] - %s", invocationIndex, info.getDisplayName());
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
        return Collections.singletonList(new GenericTypedParameterResolver(info));
      }
    };
  }

  public abstract static class AwsWrapperTestTemplateInvocationContext
      implements TestTemplateInvocationContext {
    AwsWrapperTestTemplateInvocationContext(final TestEnvironmentRequest info) {
      int index = preCreateInfos.size();
      info.setEnvPreCreateIndex(index);

      EnvPreCreateInfo envPreCreateInfo = new EnvPreCreateInfo();
      envPreCreateInfo.request = info;
      preCreateInfos.add(envPreCreateInfo);
    }

    public abstract String getDisplayName(int invocationIndex);

    public abstract List<Extension> getAdditionalExtensions();
  }

  public static class EnvPreCreateInfo {
    public TestEnvironmentRequest request;
    public Future envPreCreateFuture;
  }
}
