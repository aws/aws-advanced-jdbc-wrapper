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

package integration.refactored.host;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import integration.refactored.DatabaseEngine;
import integration.refactored.DatabaseEngineDeployment;
import integration.refactored.DriverHelper;
import integration.refactored.TestDatabaseInfo;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestEnvironmentInfo;
import integration.refactored.TestEnvironmentRequest;
import integration.refactored.TestInstanceInfo;
import integration.refactored.TestProxyDatabaseInfo;
import integration.util.AuroraTestUtility;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import org.testcontainers.utility.MountableFile;
import software.amazon.jdbc.util.StringUtils;

public class TestEnvironment implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironment.class.getName());

  private static final String DATABASE_CONTAINER_NAME_PREFIX = "database-container-";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  protected static final int PROXY_CONTROL_PORT = 8474;
  private static final String HIBERNATE_VERSION = "6.2.0.CR2";

  private final TestEnvironmentInfo info =
      new TestEnvironmentInfo(); // only this info is passed to test container

  // The following variables are local to host portion of test environment. They are not shared with a
  // test container.

  private int numOfInstances;
  private boolean reuseAuroraDbCluster;
  private String auroraClusterName; // "cluster-mysql"
  private String auroraClusterDomain; // "XYZ.us-west-2.rds.amazonaws.com"

  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private String awsSessionToken;

  private GenericContainer<?> testContainer;
  private final ArrayList<GenericContainer<?>> databaseContainers = new ArrayList<>();
  private ArrayList<ToxiproxyContainer> proxyContainers;

  private String runnerIP;

  private final Network network = Network.newNetwork();

  private AuroraTestUtility auroraUtil;

  private TestEnvironment(TestEnvironmentRequest request) {
    this.info.setRequest(request);
  }

  public static TestEnvironment build(TestEnvironmentRequest request) {
    TestEnvironment env = new TestEnvironment(request);

    switch (request.getDatabaseEngineDeployment()) {
      case DOCKER:
        initDatabaseParams(env);
        createDatabaseContainers(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          throw new UnsupportedOperationException(TestEnvironmentFeatures.IAM.toString());
        }

        if (request.getFeatures().contains(TestEnvironmentFeatures.FAILOVER_SUPPORTED)) {
          throw new UnsupportedOperationException(
              TestEnvironmentFeatures.FAILOVER_SUPPORTED.toString());
        }

        break;
      case AURORA:
        initDatabaseParams(env);
        createAuroraDbCluster(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          configureIamAccess(env);
        }

        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      createProxyContainers(env);
    }

    createTestContainer(env);

    return env;
  }

  private static void createDatabaseContainers(TestEnvironment env) {
    ContainerHelper containerHelper = new ContainerHelper();

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        env.numOfInstances = 1;
        break;
      case MULTI_INSTANCE:
        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseInstances().toString());
    }

    switch (env.info.getRequest().getDatabaseEngine()) {
      case MYSQL:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createMysqlContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      3306));
        }
        break;

      case PG:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createPostgresContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      5432));
        }
        break;

      case MARIADB:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createMariadbContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      3306));
        }
        break;

      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createAuroraDbCluster(TestEnvironment env) {

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        initAwsCredentials(env);
        env.numOfInstances = 1;
        createAuroraDbCluster(env, 1);
        break;
      case MULTI_INSTANCE:
        initAwsCredentials(env);

        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }

        createAuroraDbCluster(env, env.numOfInstances);
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createAuroraDbCluster(TestEnvironment env, int numOfInstances) {

    env.info.setAuroraRegion(
        !StringUtils.isNullOrEmpty(System.getenv("AURORA_DB_REGION"))
            ? System.getenv("AURORA_DB_REGION")
            : "us-east-2");

    env.reuseAuroraDbCluster =
        !StringUtils.isNullOrEmpty(System.getenv("REUSE_AURORA_CLUSTER"))
            && Boolean.parseBoolean(System.getenv("REUSE_AURORA_CLUSTER"));
    env.auroraClusterName = System.getenv("AURORA_CLUSTER_NAME"); // "cluster-mysql"
    env.auroraClusterDomain =
        System.getenv("AURORA_CLUSTER_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"

    if (StringUtils.isNullOrEmpty(env.auroraClusterDomain)) {
      throw new RuntimeException("Environment variable AURORA_CLUSTER_DOMAIN is required.");
    }

    env.auroraUtil =
        new AuroraTestUtility(
            env.info.getAuroraRegion(),
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);

    ArrayList<TestInstanceInfo> instances = new ArrayList<>();

    if (env.reuseAuroraDbCluster) {
      if (!env.auroraUtil.doesClusterExist(env.auroraClusterName)) {
        throw new RuntimeException(
            "It's requested to reuse existing DB cluster but it doesn't exist: "
                + env.auroraClusterName
                + "."
                + env.auroraClusterDomain);
      }
      LOGGER.finer(
          "Reuse existing cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);

      DatabaseEngine existingClusterDatabaseEngine =
          env.auroraUtil.getClusterDatabaseEngine(env.auroraClusterName);
      if (existingClusterDatabaseEngine != env.info.getRequest().getDatabaseEngine()) {
        throw new RuntimeException(
            "Existing cluster is "
                + existingClusterDatabaseEngine
                + " cluster. "
                + env.info.getRequest().getDatabaseEngine()
                + " is expected.");
      }

      instances.addAll(env.auroraUtil.getClusterInstanceIds(env.auroraClusterName));

    } else {
      if (StringUtils.isNullOrEmpty(env.auroraClusterName)) {
        env.auroraClusterName = getRandomName(env.info.getRequest());
        LOGGER.finer("Cluster to create: " + env.auroraClusterName);
      }

      try {
        env.auroraClusterDomain =
            env.auroraUtil.createCluster(
                env.info.getDatabaseInfo().getUsername(),
                env.info.getDatabaseInfo().getPassword(),
                env.info.getDatabaseInfo().getDefaultDbName(),
                env.auroraClusterName,
                getAuroraDbEngine(env.info.getRequest()),
                getAuroraInstanceClass(env.info.getRequest()),
                getAuroraDbEngineVersion(env.info.getRequest()),
                numOfInstances,
                instances);
        LOGGER.finer(
            "Created a new cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);

      } catch (Exception e) {

        LOGGER.finer("Error creating a cluster " + env.auroraClusterName + ". " + e.getMessage());

        // remove cluster and instances
        LOGGER.finer("Deleting cluster " + env.auroraClusterName);
        env.auroraUtil.deleteCluster(env.auroraClusterName);
        LOGGER.finer("Deleted cluster " + env.auroraClusterName);

        throw new RuntimeException(e);
      }
    }

    env.info.setAuroraClusterName(env.auroraClusterName);

    int port = getPort(env.info.getRequest());

    env.info
        .getDatabaseInfo()
        .setClusterEndpoint(env.auroraClusterName + ".cluster-" + env.auroraClusterDomain, port);
    env.info
        .getDatabaseInfo()
        .setClusterReadOnlyEndpoint(
            env.auroraClusterName + ".cluster-ro-" + env.auroraClusterDomain, port);
    env.info.getDatabaseInfo().setInstanceEndpointSuffix(env.auroraClusterDomain, port);

    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    try {
      env.runnerIP = env.auroraUtil.getPublicIPAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    env.auroraUtil.ec2AuthorizeIP(env.runnerIP);
  }

  private static String getRandomName(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "test-mysql-" + System.nanoTime();
      case PG:
        return "test-pg-" + System.nanoTime();
      default:
        return String.valueOf(System.nanoTime());
    }
  }

  private static String getAuroraDbEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "aurora-mysql";
      case PG:
        return "aurora-postgresql";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getAuroraDbEngineVersion(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "8.0.mysql_aurora.3.03.0";
      case PG:
        return "15.2";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getAuroraInstanceClass(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
      case PG:
        return "db.r6g.large";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static int getPort(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
      case MARIADB:
        return 3306;
      case PG:
        return 5432;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static void initDatabaseParams(TestEnvironment env) {
    final String dbName =
        !StringUtils.isNullOrEmpty(System.getenv("DB_DATABASE_NAME"))
            ? System.getenv("DB_DATABASE_NAME")
            : "test_database";
    final String dbUsername =
        !StringUtils.isNullOrEmpty(System.getenv("DB_USERNAME"))
            ? System.getenv("DB_USERNAME")
            : "test_user";
    final String dbPassword =
        !StringUtils.isNullOrEmpty(System.getenv("DB_PASSWORD"))
            ? System.getenv("DB_PASSWORD")
            : "secret_password";

    env.info.setDatabaseInfo(new TestDatabaseInfo());
    env.info.getDatabaseInfo().setUsername(dbUsername);
    env.info.getDatabaseInfo().setPassword(dbPassword);
    env.info.getDatabaseInfo().setDefaultDbName(dbName);
  }

  private static void initAwsCredentials(TestEnvironment env) {
    env.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    env.awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    env.awsSessionToken = System.getenv("AWS_SESSION_TOKEN");

    if (StringUtils.isNullOrEmpty(env.awsAccessKeyId)) {
      throw new RuntimeException("Environment variable AWS_ACCESS_KEY_ID is required.");
    }
    if (StringUtils.isNullOrEmpty(env.awsSecretAccessKey)) {
      throw new RuntimeException("Environment variable AWS_SECRET_ACCESS_KEY is required.");
    }

    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.info.setAwsAccessKeyId(env.awsAccessKeyId);
      env.info.setAwsSecretAccessKey(env.awsSecretAccessKey);
      if (!StringUtils.isNullOrEmpty(env.awsSessionToken)) {
        env.info.setAwsSessionToken(env.awsSessionToken);
      }
    }
  }

  private static void createProxyContainers(TestEnvironment env) {
    ContainerHelper containerHelper = new ContainerHelper();

    int port = getPort(env.info.getRequest());

    env.info.setProxyDatabaseInfo(new TestProxyDatabaseInfo());
    env.info.getProxyDatabaseInfo().setControlPort(PROXY_CONTROL_PORT);
    env.info.getProxyDatabaseInfo().setUsername(env.info.getDatabaseInfo().getUsername());
    env.info.getProxyDatabaseInfo().setPassword(env.info.getDatabaseInfo().getPassword());
    env.info.getProxyDatabaseInfo().setDefaultDbName(env.info.getDatabaseInfo().getDefaultDbName());

    env.proxyContainers = new ArrayList<>();

    int proxyPort = 0;
    for (TestInstanceInfo instance : env.info.getDatabaseInfo().getInstances()) {
      ToxiproxyContainer container =
          containerHelper.createProxyContainer(env.network, instance, PROXIED_DOMAIN_NAME_SUFFIX);

      container.start();
      env.proxyContainers.add(container);

      ToxiproxyContainer.ContainerProxy proxy =
          container.getProxy(instance.getEndpoint(), instance.getEndpointPort());

      if (proxyPort != 0 && proxyPort != proxy.getOriginalProxyPort()) {
        throw new RuntimeException("DB cluster proxies should be on the same port.");
      }
      proxyPort = proxy.getOriginalProxyPort();
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-cluster",
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterEndpoint(
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterReadOnlyEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-ro-cluster",
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterReadOnlyEndpoint(
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getInstanceEndpointSuffix())) {
      env.info
          .getProxyDatabaseInfo()
          .setInstanceEndpointSuffix(
              env.info.getDatabaseInfo().getInstanceEndpointSuffix() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    for (TestInstanceInfo instanceInfo : env.info.getDatabaseInfo().getInstances()) {
      TestInstanceInfo proxyInstanceInfo =
          new TestInstanceInfo(
              instanceInfo.getInstanceId(),
              instanceInfo.getEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
      env.info.getProxyDatabaseInfo().getInstances().add(proxyInstanceInfo);
    }
  }

  private static void createTestContainer(TestEnvironment env) {
    final ContainerHelper containerHelper = new ContainerHelper();

    if (env.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY)) {
      env.testContainer =
          containerHelper.createTestContainer(
                  "aws/rds-test-container",
                  getContainerBaseImageName(env.info.getRequest()),
                  builder -> builder
                      .run("apk", "add", "git")
                      .run("git", "clone", "--depth", "1", "--branch", HIBERNATE_VERSION,
                          "https://github.com/hibernate/hibernate-orm.git", "/app/hibernate-orm"))
              .withCopyFileToContainer(MountableFile.forHostPath(
                      "src/test/resources/hibernate_files/databases.gradle"),
                  "app/hibernate-orm/gradle/databases.gradle")
              .withCopyFileToContainer(MountableFile.forHostPath(
                      "src/test/resources/hibernate_files/hibernate-core.gradle"),
                  "hibernate-core/hibernate-core.gradle")
              .withCopyFileToContainer(MountableFile.forHostPath(
                      "src/test/resources/hibernate_files/java-module.gradle"),
                  "app/hibernate-orm/gradle/java-module.gradle")
              .withCopyFileToContainer(MountableFile.forHostPath(
                      "src/test/resources/hibernate_files/collect_test_results.sh"),
                  "app/collect_test_results.sh");
    } else {
      env.testContainer = containerHelper.createTestContainer(
          "aws/rds-test-container",
          getContainerBaseImageName(env.info.getRequest()));
    }

    env.testContainer
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(env.network)
        .withEnv("TEST_ENV_INFO_JSON", getEnvironmentInfoAsString(env))
        .withEnv("TEST_ENV_DESCRIPTION", env.info.getRequest().getDisplayName());
    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.testContainer
          .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
          .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
          .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken);
    }

    env.testContainer.start();
  }

  private static String getContainerBaseImageName(TestEnvironmentRequest request) {
    switch (request.getTargetJvm()) {
      case OPENJDK8:
        return "openjdk:8-jdk-alpine";
      case OPENJDK11:
        return "adoptopenjdk/openjdk11:alpine";
      case GRAALVM:
        return "ghcr.io/graalvm/jdk:22.2.0";
      default:
        throw new NotImplementedException(request.getTargetJvm().toString());
    }
  }

  private static void configureIamAccess(TestEnvironment env) {

    if (env.info.getRequest().getDatabaseEngineDeployment() != DatabaseEngineDeployment.AURORA) {
      throw new UnsupportedOperationException(
          env.info.getRequest().getDatabaseEngineDeployment().toString());
    }

    env.info.setIamUsername(
        !StringUtils.isNullOrEmpty(System.getenv("IAM_USER"))
            ? System.getenv("IAM_USER")
            : "jane_doe");

    if (!env.reuseAuroraDbCluster) {
      try {
        Class.forName(DriverHelper.getDriverClassname(env.info.getRequest().getDatabaseEngine()));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(
            "Driver not found: "
                + DriverHelper.getDriverClassname(env.info.getRequest().getDatabaseEngine()),
            e);
      }

      final String url =
          String.format(
              "%s%s:%d/%s",
              DriverHelper.getDriverProtocol(env.info.getRequest().getDatabaseEngine()),
              env.info.getDatabaseInfo().getClusterEndpoint(),
              env.info.getDatabaseInfo().getClusterEndpointPort(),
              env.info.getDatabaseInfo().getDefaultDbName());

      try {
        env.auroraUtil.addAuroraAwsIamUser(
            env.info.getRequest().getDatabaseEngine(),
            url,
            env.info.getDatabaseInfo().getUsername(),
            env.info.getDatabaseInfo().getPassword(),
            env.info.getIamUsername(),
            env.info.getDatabaseInfo().getDefaultDbName());

      } catch (SQLException e) {
        throw new RuntimeException("Error configuring IAM access.", e);
      }
    }
  }

  private static String getEnvironmentInfoAsString(TestEnvironment env) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(env.info);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing environment details.", e);
    }
  }

  public void runTests(String taskName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();

    if (this.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY)) {
      final Long exitCode = containerHelper.runCmdInDirectory(
          this.testContainer,
          "/app/hibernate-orm",
          buildHibernateCommands(false));
      containerHelper.runCmd(this.testContainer, "./collect_test_results.sh");
      assertEquals(0, exitCode, "Hibernate ORM tests failed");
    } else {
      containerHelper.runTest(this.testContainer, taskName);
    }
  }

  public void debugTests(String taskName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();

    if (this.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY)) {
      final Long exitCode = containerHelper.runCmdInDirectory(
          this.testContainer,
          "/app/hibernate-orm",
          buildHibernateCommands(true));
      containerHelper.runCmd(this.testContainer, "./collect_test_results.sh");
      assertEquals(0, exitCode, "Hibernate ORM tests failed");
    } else {
      containerHelper.debugTest(this.testContainer, taskName);
    }
  }

  private String[] buildHibernateCommands(boolean debugMode) {
    final TestDatabaseInfo dbInfo = this.info.getDatabaseInfo();
    final List<String> command = new ArrayList<>(Arrays.asList(
        "./gradlew", "test",
        "-DdbHost=" + DATABASE_CONTAINER_NAME_PREFIX + 1, // Hibernate ORM tests only support 1 database instance
        "-DdbUser=" + dbInfo.getUsername(),
        "-DdbPass=" + dbInfo.getPassword(),
        "-DdbName=" + dbInfo.getDefaultDbName(),
        "--no-parallel", "--no-daemon"
    ));

    if (debugMode) {
      command.add("--debug-jvm");
    }

    switch (this.info.getRequest().getDatabaseEngine()) {
      case PG:
        command.add("-Pdb=amazon_ci");
        command.add("-PexcludeTests=PostgreSQLSkipAutoCommitTest");
        break;
      case MYSQL:
      default:
        command.add("-Pdb=amazon_mysql_ci");
        command.add("-PexcludeTests=MySQLSkipAutoCommitTest");
        break;
    }
    return command.toArray(new String[] {});
  }

  @Override
  public void close() throws Exception {
    if (this.databaseContainers != null) {
      for (GenericContainer<?> container : this.databaseContainers) {
        try {
          container.stop();
        } catch (Exception ex) {
          // ignore
        }
      }
      this.databaseContainers.clear();
    }

    if (this.testContainer != null) {
      this.testContainer.stop();
      this.testContainer = null;
    }

    if (this.proxyContainers != null) {
      for (ToxiproxyContainer proxyContainer : this.proxyContainers) {
        proxyContainer.stop();
      }
      this.proxyContainers = null;
    }

    switch (this.info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
        deleteAuroraDbCluster();
        break;
      case RDS:
        throw new NotImplementedException(this.info.getRequest().getTargetJvm().toString());
      default:
        // do nothing
    }
  }

  private void deleteAuroraDbCluster() {
    if (!this.reuseAuroraDbCluster && !StringUtils.isNullOrEmpty(this.runnerIP)) {
      auroraUtil.ec2DeauthorizesIP(runnerIP);
    }

    if (!this.reuseAuroraDbCluster) {
      LOGGER.finest("Deleting cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
      auroraUtil.deleteCluster(this.auroraClusterName);
      LOGGER.finest("Deleted cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
    }
  }
}
