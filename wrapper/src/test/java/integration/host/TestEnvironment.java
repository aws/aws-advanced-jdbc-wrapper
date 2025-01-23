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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestEnvironmentRequest;
import integration.TestInstanceInfo;
import integration.TestProxyDatabaseInfo;
import integration.TestTelemetryInfo;
import integration.host.TestEnvironmentProvider.EnvPreCreateInfo;
import integration.util.AuroraTestUtility;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.jdbc.util.StringUtils;

public class TestEnvironment implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironment.class.getName());
  private static final int NUM_OR_ENV_PRE_CREATE = 1; // create this number of environments in advance

  private static final ExecutorService envPreCreateExecutor = Executors.newCachedThreadPool();

  private static final String DATABASE_CONTAINER_NAME_PREFIX = "database-container-";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String TELEMETRY_XRAY_CONTAINER_NAME = "xray-daemon";
  private static final String TELEMETRY_OTLP_CONTAINER_NAME = "otlp-daemon";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  protected static final int PROXY_CONTROL_PORT = 8474;
  protected static final int PROXY_PORT = 8666;
  private static final String HIBERNATE_VERSION = "6.2.0.CR2";

  private static final TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();
  private static final boolean USE_OTLP_CONTAINER_FOR_TRACES = true;

  private static final AtomicInteger ipAddressUsageRefCount = new AtomicInteger(0);

  private final TestEnvironmentInfo info =
      new TestEnvironmentInfo(); // only this info is passed to test container

  // The following variables are local to host portion of test environment. They are not shared with a
  // test container.

  private int numOfInstances;
  private boolean reuseAuroraDbCluster;
  private String auroraClusterName; // "cluster-mysql"
  private String auroraClusterDomain; // "XYZ.us-west-2.rds.amazonaws.com"
  private String rdsEndpoint; // "https://rds-int.amazon.com"

  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private String awsSessionToken;

  private GenericContainer<?> testContainer;
  private final ArrayList<GenericContainer<?>> databaseContainers = new ArrayList<>();
  private ArrayList<ToxiproxyContainer> proxyContainers;
  private GenericContainer<?> telemetryXRayContainer;
  private GenericContainer<?> telemetryOtlpContainer;

  private String runnerIP;

  private final Network network = Network.newNetwork();

  private AuroraTestUtility auroraUtil;

  private TestEnvironment(TestEnvironmentRequest request) {
    this.info.setRequest(request);
  }

  public static TestEnvironment build(TestEnvironmentRequest request) throws IOException, URISyntaxException {

    DatabaseEngineDeployment deployment = request.getDatabaseEngineDeployment();
    if (deployment == DatabaseEngineDeployment.AURORA
        || deployment == DatabaseEngineDeployment.RDS
        || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
      // These environment require creating external database cluster that should be publicly available.
      // Corresponding AWS Security Groups should be configured and the test task runner IP address
      // should be whitelisted.
      ipAddressUsageRefCount.incrementAndGet();
    }

    LOGGER.finest("Building test env: " + request.getEnvPreCreateIndex());
    preCreateEnvironment(request.getEnvPreCreateIndex());

    TestEnvironment env;

    switch (deployment) {
      case DOCKER:
        env = new TestEnvironment(request);
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
      case RDS_MULTI_AZ_CLUSTER:
        env = createAuroraOrMultiAzEnvironment(request);
        authorizeIP(env);

        break;

      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      createProxyContainers(env);
    }

    if (!USE_OTLP_CONTAINER_FOR_TRACES
        && request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      createTelemetryXRayContainer(env);
    }

    if ((USE_OTLP_CONTAINER_FOR_TRACES
        && request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED))
        || request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {
      createTelemetryOtlpContainer(env);
    }

    createTestContainer(env);

    return env;
  }

  private static TestEnvironment createAuroraOrMultiAzEnvironment(TestEnvironmentRequest request) {

    EnvPreCreateInfo preCreateInfo =
        TestEnvironmentProvider.preCreateInfos.get(request.getEnvPreCreateIndex());

    if (preCreateInfo.envPreCreateFuture != null) {
      /*
       This environment has being created in advance.
       We need to wait for results and apply details of newly created environment to the current
       environment.
      */
      Object result;
      try {
        // Effectively waits till the future completes and returns results.
        final long startTime = System.nanoTime();
        result = preCreateInfo.envPreCreateFuture.get();
        final long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
        LOGGER.finest(() ->
            String.format("Additional wait time for test environment to be ready (pre-create): %d sec", duration));
      } catch (ExecutionException | InterruptedException ex) {
        throw new RuntimeException("Test environment create error.", ex);
      }

      preCreateInfo.envPreCreateFuture = null;

      if (result == null) {
        throw new RuntimeException("Test environment create error. Results are empty.");
      }
      if (result instanceof Exception) {
        throw new RuntimeException((Exception) result);
      }
      if (result instanceof TestEnvironment) {
        TestEnvironment resultTestEnvironment = (TestEnvironment) result;
        LOGGER.finer(() -> String.format("Use pre-created DB cluster: %s.cluster-%s",
            resultTestEnvironment.auroraClusterName, resultTestEnvironment.auroraClusterDomain));

        return resultTestEnvironment;
      }
      throw new RuntimeException(
          "Test environment create error. Unrecognized result type: " + result.getClass().getName());

    } else {
      TestEnvironment env = new TestEnvironment(request);
      initDatabaseParams(env);
      createDbCluster(env);

      if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
        if (request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
          throw new RuntimeException("IAM isn't supported by " + DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER);
        }
        configureIamAccess(env);
      }

      return env;
    }

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

  private static void createDbCluster(TestEnvironment env) {

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        initAwsCredentials(env);
        env.numOfInstances = 1;
        createDbCluster(env, 1);
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

        createDbCluster(env, env.numOfInstances);
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createDbCluster(TestEnvironment env, int numOfInstances) {

    env.info.setRegion(
        !StringUtils.isNullOrEmpty(config.rdsDbRegion)
            ? config.rdsDbRegion
            : "us-east-2");

    env.reuseAuroraDbCluster = config.reuseRdsCluster;
    env.auroraClusterName = config.rdsClusterName; // "cluster-mysql"
    env.auroraClusterDomain = config.rdsClusterDomain; // "XYZ.us-west-2.rds.amazonaws.com"
    env.rdsEndpoint = config.rdsEndpoint; // "XYZ.us-west-2.rds.amazonaws.com"
    env.info.setRdsEndpoint(env.rdsEndpoint);

    env.auroraUtil =
        new AuroraTestUtility(
            env.info.getRegion(),
            env.rdsEndpoint,
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);
    if (env.reuseAuroraDbCluster) {
      if (StringUtils.isNullOrEmpty(env.auroraClusterDomain)) {
        throw new RuntimeException("Environment variable RDS_CLUSTER_DOMAIN is required.");
      }

      if (!env.auroraUtil.doesClusterExist(env.auroraClusterName)) {
        throw new RuntimeException(
            "It's requested to reuse existing DB cluster but it doesn't exist: "
                + env.auroraClusterName
                + "."
                + env.auroraClusterDomain);
      }
      LOGGER.finer(
          "Reuse existing cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);

      DBCluster clusterInfo = env.auroraUtil.getClusterInfo(env.auroraClusterName);

      DatabaseEngine existingClusterDatabaseEngine = env.auroraUtil.getClusterEngine(clusterInfo);
      if (existingClusterDatabaseEngine != env.info.getRequest().getDatabaseEngine()) {
        throw new RuntimeException(
            "Existing cluster is "
                + existingClusterDatabaseEngine
                + " cluster. "
                + env.info.getRequest().getDatabaseEngine()
                + " is expected.");
      }

      env.info.setDatabaseEngine(clusterInfo.engine());
      env.info.setDatabaseEngineVersion(clusterInfo.engineVersion());
    } else {
      if (StringUtils.isNullOrEmpty(env.auroraClusterName)) {
        int remainingTries = 5;
        boolean clusterExists = false;
        while (remainingTries-- > 0) {
          env.auroraClusterName = getRandomName(env.info.getRequest());
          if (env.auroraUtil.doesClusterExist(env.auroraClusterName)) {
            clusterExists = true;
            LOGGER.finest("Cluster " + env.auroraClusterName + " already exists. Pick up another name.");
          } else {
            clusterExists = false;
            LOGGER.finer("Cluster to create: " + env.auroraClusterName);
            break;
          }
        }
        if (clusterExists) {
          throw new RuntimeException("Can't pick up a cluster name.");
        }
      }

      try {
        String engine = getDbEngine(env.info.getRequest());
        String engineVersion = getDbEngineVersion(engine, env);
        if (StringUtils.isNullOrEmpty(engineVersion)) {
          throw new RuntimeException("Failed to get engine version.");
        }
        String instanceClass = env.auroraUtil.getDbInstanceClass(env.info.getRequest());

        LOGGER.finer(
            "Using " + engine + " " + engineVersion);

        env.auroraUtil.createCluster(
            env.info.getDatabaseInfo().getUsername(),
            env.info.getDatabaseInfo().getPassword(),
            env.info.getDatabaseInfo().getDefaultDbName(),
            env.auroraClusterName,
            env.info.getRequest().getDatabaseEngineDeployment(),
            env.info.getRegion(),
            engine,
            instanceClass,
            engineVersion,
            numOfInstances);

        List<DBInstance> dbInstances = env.auroraUtil.getDBInstances(env.auroraClusterName);
        if (dbInstances.isEmpty()) {
          throw new RuntimeException("Failed to get instance information for cluster " + env.auroraClusterName);
        }

        final String instanceEndpoint = dbInstances.get(0).endpoint().address();
        env.auroraClusterDomain = instanceEndpoint.substring(instanceEndpoint.indexOf(".") + 1);
        env.info.setDatabaseEngine(engine);
        env.info.setDatabaseEngineVersion(engineVersion);
        LOGGER.finer(
            "Created a new cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);
      } catch (Exception e) {

        LOGGER.finer("Error creating a cluster " + env.auroraClusterName + ". " + e.getMessage());

        // remove cluster and instances
        LOGGER.finer("Deleting cluster " + env.auroraClusterName);
        env.auroraUtil.deleteCluster(env.auroraClusterName, env.info.getRequest().getDatabaseEngineDeployment());
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

    List<TestInstanceInfo> instances = env.auroraUtil.getTestInstancesInfo(env.auroraClusterName);
    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    authorizeIP(env);

    final DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    final DatabaseEngine engine = env.info.getRequest().getDatabaseEngine();
    if (DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(deployment) && DatabaseEngine.PG.equals(engine)) {
      DriverHelper.registerDriver(engine);

      try (Connection conn = DriverHelper.getDriverConnection(env.info);
          Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE EXTENSION IF NOT EXISTS rds_tools");
      } catch (SQLException e) {
        throw new RuntimeException("An exception occurred while creating the rds_tools extension.", e);
      }
    }
  }

  private static void authorizeIP(TestEnvironment env) {
    try {
      env.runnerIP = env.auroraUtil.getPublicIPAddress();
      LOGGER.finest("Test runner IP: " + env.runnerIP);
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

  private static String getDbEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return getAuroraDbEngine(request);
      case RDS:
      case RDS_MULTI_AZ_CLUSTER:
        return getRdsEngine(request);
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
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

  private static String getRdsEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "mysql";
      case PG:
        return "postgres";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getDbEngineVersion(String engineName, TestEnvironment env) {
    String systemPropertyVersion;
    TestEnvironmentRequest request = env.info.getRequest();
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        systemPropertyVersion = config.mysqlVersion;
        break;
      case PG:
        systemPropertyVersion = config.pgVersion;
        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
    return findEngineVersion(env, engineName, systemPropertyVersion);
  }

  private static String findEngineVersion(
      TestEnvironment env,
      String engineName,
      String systemPropertyVersion) {

    if (StringUtils.isNullOrEmpty(systemPropertyVersion)) {
      return env.auroraUtil.getDefaultVersion(engineName);
    }
    switch (systemPropertyVersion.toLowerCase()) {
      case "default":
        return env.auroraUtil.getDefaultVersion(engineName);
      case "latest":
        return env.auroraUtil.getLatestVersion(engineName);
      default:
        return systemPropertyVersion;
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
        !StringUtils.isNullOrEmpty(config.dbName)
            ? config.dbName
            : "test_database";
    final String dbUsername =
        !StringUtils.isNullOrEmpty(config.dbUsername)
            ? config.dbUsername
            : "test_user";
    final String dbPassword =
        !StringUtils.isNullOrEmpty(config.dbPassword)
            ? config.dbPassword
            : "secret_password";

    env.info.setDatabaseInfo(new TestDatabaseInfo());
    env.info.getDatabaseInfo().setUsername(dbUsername);
    env.info.getDatabaseInfo().setPassword(dbPassword);
    env.info.getDatabaseInfo().setDefaultDbName(dbName);
  }

  private static void initAwsCredentials(TestEnvironment env) {
    env.awsAccessKeyId = config.awsAccessKeyId;
    env.awsSecretAccessKey = config.awsSecretAccessKey;
    env.awsSessionToken = config.awsSessionToken;

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

  private static void createProxyContainers(TestEnvironment env) throws IOException {
    ContainerHelper containerHelper = new ContainerHelper();

    int port = getPort(env.info.getRequest());

    env.info.setProxyDatabaseInfo(new TestProxyDatabaseInfo());
    env.info.getProxyDatabaseInfo().setControlPort(PROXY_CONTROL_PORT);
    env.info.getProxyDatabaseInfo().setUsername(env.info.getDatabaseInfo().getUsername());
    env.info.getProxyDatabaseInfo().setPassword(env.info.getDatabaseInfo().getPassword());
    env.info.getProxyDatabaseInfo().setDefaultDbName(env.info.getDatabaseInfo().getDefaultDbName());

    env.proxyContainers = new ArrayList<>();

    for (TestInstanceInfo instance : env.info.getDatabaseInfo().getInstances()) {
      ToxiproxyContainer container =
          containerHelper.createProxyContainer(env.network, instance, PROXIED_DOMAIN_NAME_SUFFIX);

      container.start();
      env.proxyContainers.add(container);
      final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
          container.getHost(),
          container.getMappedPort(PROXY_CONTROL_PORT));

      containerHelper.createProxy(
          toxiproxyClient,
          instance.getHost(),
          instance.getPort());
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-cluster",
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterEndpoint(),
              port));

      env.info
          .getProxyDatabaseInfo()
          .setClusterEndpoint(
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              PROXY_PORT);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterReadOnlyEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-ro-cluster",
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint(),
              port));

      env.info
          .getProxyDatabaseInfo()
          .setClusterReadOnlyEndpoint(
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              PROXY_PORT);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getInstanceEndpointSuffix())) {
      env.info
          .getProxyDatabaseInfo()
          .setInstanceEndpointSuffix(
              env.info.getDatabaseInfo().getInstanceEndpointSuffix() + PROXIED_DOMAIN_NAME_SUFFIX,
              PROXY_PORT);
    }

    for (TestInstanceInfo instanceInfo : env.info.getDatabaseInfo().getInstances()) {
      TestInstanceInfo proxyInstanceInfo =
          new TestInstanceInfo(
              instanceInfo.getInstanceId(),
              instanceInfo.getHost() + PROXIED_DOMAIN_NAME_SUFFIX,
              PROXY_PORT);
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

  private static void createTelemetryXRayContainer(TestEnvironment env) {
    String xrayAwsRegion =
        !StringUtils.isNullOrEmpty(System.getenv("XRAY_AWS_REGION"))
            ? System.getenv("XRAY_AWS_REGION")
            : "us-east-2";

    LOGGER.finest("Creating XRay telemetry container");
    final ContainerHelper containerHelper = new ContainerHelper();

    env.telemetryXRayContainer = containerHelper.createTelemetryXrayContainer(
        xrayAwsRegion,
        env.network,
        TELEMETRY_XRAY_CONTAINER_NAME);

    if (!env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      throw new RuntimeException("AWS_CREDENTIALS_ENABLED is required for XRay telemetry.");
    }

    env.telemetryXRayContainer
        .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
        .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
        .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken);

    env.info.setTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_XRAY_CONTAINER_NAME, 2000));
    LOGGER.finest("Starting XRay telemetry container");
    env.telemetryXRayContainer.start();
  }

  private static void createTelemetryOtlpContainer(TestEnvironment env) {

    LOGGER.finest("Creating OTLP telemetry container");
    final ContainerHelper containerHelper = new ContainerHelper();

    env.telemetryOtlpContainer = containerHelper.createTelemetryOtlpContainer(
        env.network,
        TELEMETRY_OTLP_CONTAINER_NAME);

    if (!env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      throw new RuntimeException("AWS_CREDENTIALS_ENABLED is required for OTLP telemetry.");
    }

    String otlpRegion = !StringUtils.isNullOrEmpty(System.getenv("OTLP_AWS_REGION"))
        ? System.getenv("OTLP_AWS_REGION")
        : "us-east-2";

    env.telemetryOtlpContainer
        .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
        .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
        .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken)
        .withEnv("AWS_REGION", otlpRegion);

    if (USE_OTLP_CONTAINER_FOR_TRACES) {
      env.info.setTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 2000));
    }
    env.info.setMetricsTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 4317));

    LOGGER.finest("Starting OTLP telemetry container");
    env.telemetryOtlpContainer.start();
  }

  private static String getContainerBaseImageName(TestEnvironmentRequest request) {
    switch (request.getTargetJvm()) {
      case OPENJDK8:
        return "openjdk:8-jdk-alpine";
      case OPENJDK11:
        return "amazoncorretto:11.0.19-alpine3.17";
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
        !StringUtils.isNullOrEmpty(config.iamUser)
            ? config.iamUser
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
      TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();
      containerHelper.runTest(this.testContainer, taskName, config.includeTags, config.excludeTags);
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
      TestEnvironmentConfiguration config = new TestEnvironmentConfiguration();
      containerHelper.debugTest(this.testContainer, taskName, config.includeTags, config.excludeTags);
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
    for (GenericContainer<?> container : this.databaseContainers) {
      try {
        container.stop();
      } catch (Exception ex) {
        // ignore
      }
    }
    this.databaseContainers.clear();

    if (this.telemetryXRayContainer != null) {
      this.telemetryXRayContainer.stop();
      this.telemetryXRayContainer = null;
    }

    if (this.telemetryOtlpContainer != null) {
      this.telemetryOtlpContainer.stop();
      this.telemetryOtlpContainer = null;
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
      case RDS_MULTI_AZ_CLUSTER:
        deleteDbCluster();
        break;
      case RDS:
        throw new NotImplementedException(this.info.getRequest().getTargetJvm().toString());
      default:
        // do nothing
    }
  }

  private void deleteDbCluster() {
    if (!this.reuseAuroraDbCluster && !StringUtils.isNullOrEmpty(this.runnerIP)) {
      if (ipAddressUsageRefCount.decrementAndGet() == 0) {
        // Another test environments are still in use of test task runner IP address.
        // The last execute tst environment will do the cleanup.
        auroraUtil.ec2DeauthorizesIP(runnerIP);
      }
    }

    if (!this.reuseAuroraDbCluster) {
      LOGGER.finest("Deleting cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
      auroraUtil.deleteCluster(this.auroraClusterName, this.info.getRequest().getDatabaseEngineDeployment());
      LOGGER.finest("Deleted cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
    }
  }

  private static void preCreateEnvironment(int currentEnvIndex) {
    int index = currentEnvIndex + 1; // inclusive
    int endIndex = index + NUM_OR_ENV_PRE_CREATE; //  exclusive
    if (endIndex > TestEnvironmentProvider.preCreateInfos.size()) {
      endIndex = TestEnvironmentProvider.preCreateInfos.size();
    }

    while (index < endIndex) {
      EnvPreCreateInfo preCreateInfo = TestEnvironmentProvider.preCreateInfos.get(index);

      if (preCreateInfo.envPreCreateFuture == null
          && (preCreateInfo.request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA
            || preCreateInfo.request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS
            || preCreateInfo.request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER)) {

        // run environment creation in advance
        int finalIndex = index;
        LOGGER.finest(() -> String.format("Pre-create environment for [%d] - %s",
            finalIndex, preCreateInfo.request.getDisplayName()));

        final TestEnvironment env = new TestEnvironment(preCreateInfo.request);

        preCreateInfo.envPreCreateFuture = envPreCreateExecutor.submit(() -> {
          final long startTime = System.nanoTime();
          try {
            initDatabaseParams(env);
            createDbCluster(env);
            if (env.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.IAM)) {
              if (env.info.getRequest().getDatabaseEngineDeployment()
                  == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
                throw new RuntimeException("IAM isn't supported by " + DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER);
              }
              configureIamAccess(env);
            }
            return env;

          } catch (Exception ex) {
            return ex;
          } finally {
            final long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
            LOGGER.finest(() -> String.format(
                "Pre-create environment task [%d] run in background for %d sec.",
                finalIndex,
                duration));
          }
        });

      }
      index++;
    }
  }
}
