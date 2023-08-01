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

  private final TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {

    preCreateInfos.clear();
    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();

    if (!config.noDocker) {
      if (!config.noMysqlEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    config.testHibernateOnly ? TargetJvm.OPENJDK11 : TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testHibernateOnly ? TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
      }
      if (!config.noPgEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    config.testHibernateOnly ? TargetJvm.OPENJDK11 : TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testHibernateOnly ? TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
      }
      if (!config.noMariadbEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MARIADB,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noMysqlEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noPgEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noMariadbEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MARIADB,
                    DatabaseInstances.SINGLE_INSTANCE,
                    1,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }

      // multiple instances

      if (!config.noMysqlEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    config.testHibernateOnly ? TargetJvm.OPENJDK11 : TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testHibernateOnly ? TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
      }
      if (!config.noPgEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    config.testHibernateOnly ? TargetJvm.OPENJDK11 : TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testHibernateOnly ? TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
      }
      if (!config.noMariadbEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MARIADB,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noMysqlEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noPgEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
      if (!config.noMariadbEngine && !config.noGraalVm) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MARIADB,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.DOCKER,
                    TargetJvm.GRAALVM,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null)));
      }
    }

    if (!config.noAurora) {
      if (!config.noMysqlEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.MULTI_INSTANCE,
                    5,
                    DatabaseEngineDeployment.AURORA,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                    TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                    config.noIam ? null : TestEnvironmentFeatures.IAM,
                    config.noSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noPerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));

        // Tests for HIKARI, IAM, SECRETS_MANAGER and PERFORMANCE are covered by
        // cluster configuration above, so it's safe to skip these tests for configurations below.
        // The main goal of the following cluster configurations is to check failover.
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.MYSQL,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.AURORA,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                    TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
      }
      if (!config.noPgEngine && !config.noOpenJdk) {
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.MULTI_INSTANCE,
                    5,
                    DatabaseEngineDeployment.AURORA,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                    TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                    config.noIam ? null : TestEnvironmentFeatures.IAM,
                    config.noSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                    config.noHikari ? null : TestEnvironmentFeatures.HIKARI,
                    config.noPerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));

        // Tests for HIKARI, IAM, SECRETS_MANAGER and PERFORMANCE are covered by
        // cluster configuration above, so it's safe to skip these tests for configurations below.
        // The main goal of the following cluster configurations is to check failover.
        resultContextList.add(
            getEnvironment(
                new TestEnvironmentRequest(
                    DatabaseEngine.PG,
                    DatabaseInstances.MULTI_INSTANCE,
                    2,
                    DatabaseEngineDeployment.AURORA,
                    TargetJvm.OPENJDK8,
                    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                    config.noFailover ? null : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                    TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                    config.noMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                    config.noPgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                    config.noMariadbDriver ? TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS : null,
                    config.testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null)));
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
