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

import static org.junit.jupiter.api.Assertions.fail;

import integration.util.AuroraTestUtility;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.util.StringUtils;

/**
 * Integration tests against RDS Aurora cluster. Uses {@link AuroraTestUtility} which requires AWS
 * Credentials to create/destroy clusters & set EC2 Whitelist.
 *
 * <p>The following environment variables are REQUIRED for AWS IAM tests - AWS_ACCESS_KEY_ID, AWS
 * access key - AWS_SECRET_ACCESS_KEY, AWS secret access key - AWS_SESSION_TOKEN, AWS Session token
 *
 * <p>The following environment variables are optional but suggested differentiating between runners
 * Provided values are just examples. Assuming cluster endpoint is
 * "database-cluster-name.XYZ.us-east-2.rds.amazonaws.com"
 *
 * <p>TEST_DB_CLUSTER_IDENTIFIER=database-cluster-name TEST_USERNAME=user-name
 * TEST_PASSWORD=user-secret-password
 */
public class AuroraMysqlContainerTest {

  private static final int AURORA_MYSQL_PORT = 3306;
  private static final String AURORA_MYSQL_TEST_HOST_NAME = "test-container";

  private static final String AURORA_MYSQL_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_USERNAME"))
          ? System.getenv("AURORA_MYSQL_USERNAME")
          : "my_test_username";
  private static final String AURORA_MYSQL_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_PASSWORD"))
          ? System.getenv("AURORA_MYSQL_PASSWORD")
          : "my_test_password";
  protected static final String AURORA_MYSQL_DB =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_DB"))
          ? System.getenv("AURORA_MYSQL_DB")
          : "test";
  private static final String AURORA_MYSQL_DB_USER =
      !com.mysql.cj.util.StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_DB_USER"))
          ? System.getenv("AURORA_MYSQL_DB_USER") : "jane_doe";

  protected static final String EXISTING_DB_CONN_SUFFIX = System.getenv("DB_CONN_SUFFIX");

  private static final String REPEAT_TIMES = System.getenv("REPEAT_TIMES");
  private static final String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
  private static final String AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
  private static final String AWS_SESSION_TOKEN = System.getenv("AWS_SESSION_TOKEN");

  private static final String DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mysql://";
  private static String dbConnStrSuffix = "";
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2";

  private static final String AURORA_MYSQL_DB_REGION =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_DB_REGION"))
          ? System.getenv("AURORA_MYSQL_DB_REGION")
          : "us-east-1";
  private static final String AURORA_MYSQL_CLUSTER_IDENTIFIER =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_MYSQL_CLUSTER_IDENTIFIER"))
          ? System.getenv("AURORA_MYSQL_CLUSTER_IDENTIFIER")
          : "test-identifier";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static List<String> mysqlInstances = new ArrayList<>();

  private static int mysqlProxyPort;
  private static GenericContainer<?> integrationTestContainer;
  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";
  private static String runnerIP = null;

  private static Network network;
  private static final boolean TEST_WITH_EXISTING_DB = EXISTING_DB_CONN_SUFFIX != null;

  private static final ContainerHelper containerHelper = new ContainerHelper();
  private static final AuroraTestUtility auroraUtil = new AuroraTestUtility(AURORA_MYSQL_DB_REGION);

  private static final String TEST_CONTAINER_TYPE = System.getenv("TEST_CONTAINER_TYPE");

  @BeforeAll
  static void setUp() throws SQLException, InterruptedException, UnknownHostException {
    Assertions.assertNotNull(AWS_ACCESS_KEY_ID);
    Assertions.assertNotNull(AWS_SECRET_ACCESS_KEY);

    if (TEST_WITH_EXISTING_DB) {
      dbConnStrSuffix = EXISTING_DB_CONN_SUFFIX;
    } else {
      dbConnStrSuffix = auroraUtil.createCluster(AURORA_MYSQL_USERNAME, AURORA_MYSQL_PASSWORD, AURORA_MYSQL_DB,
          AURORA_MYSQL_CLUSTER_IDENTIFIER);
      runnerIP = auroraUtil.getPublicIPAddress();
      auroraUtil.ec2AuthorizeIP(runnerIP);
    }

    dbHostCluster = AURORA_MYSQL_CLUSTER_IDENTIFIER + ".cluster-" + dbConnStrSuffix;
    dbHostClusterRo = AURORA_MYSQL_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnStrSuffix;

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      fail("MySQL driver not found");
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    containerHelper.addAuroraAwsIamUser(
        DB_CONN_STR_PREFIX + dbHostCluster + "/" + AURORA_MYSQL_DB + DB_CONN_PROP,
        AURORA_MYSQL_USERNAME,
        AURORA_MYSQL_PASSWORD,
        AURORA_MYSQL_DB_USER);

    network = Network.newNetwork();
    mysqlInstances =
        containerHelper.getAuroraInstanceEndpoints(
            DB_CONN_STR_PREFIX + dbHostCluster + "/" + AURORA_MYSQL_DB + DB_CONN_PROP,
            AURORA_MYSQL_USERNAME,
            AURORA_MYSQL_PASSWORD,
            dbConnStrSuffix);
    proxyContainers =
        containerHelper.createProxyContainers(
            network, mysqlInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    mysqlProxyPort =
        containerHelper.createInstanceProxies(
            mysqlInstances, proxyContainers, AURORA_MYSQL_PORT);

    proxyContainers.add(
        containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-instance-cluster",
            dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostCluster,
            AURORA_MYSQL_PORT,
            mysqlProxyPort));

    proxyContainers.add(
        containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-ro-instance-cluster",
            dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostClusterRo,
            AURORA_MYSQL_PORT,
            mysqlProxyPort));

    integrationTestContainer = initializeTestContainer(network, mysqlInstances);
  }

  @AfterAll
  static void tearDown() {
    if (!TEST_WITH_EXISTING_DB) {
      if (StringUtils.isNullOrEmpty(AURORA_MYSQL_CLUSTER_IDENTIFIER)) {
        auroraUtil.deleteCluster();
      } else {
        auroraUtil.deleteCluster(AURORA_MYSQL_CLUSTER_IDENTIFIER);
      }

      auroraUtil.ec2DeauthorizesIP(runnerIP);
    }

    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-mysql");
  }

  @Test
  public void runPerformanceTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-mysql-performance");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora-mysql");
  }

  @Test
  public void debugPerformanceTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora-mysql-performance");
  }

  protected static GenericContainer<?> initializeTestContainer(
      final Network network, List<String> mysqlInstances) {

    GenericContainer<?> container =
        containerHelper
            .createTestContainerByType(TEST_CONTAINER_TYPE, "aws/rds-test-container")
            .withNetworkAliases(AURORA_MYSQL_TEST_HOST_NAME)
            .withNetwork(network)
            .withEnv("AURORA_MYSQL_USERNAME", AURORA_MYSQL_USERNAME)
            .withEnv("AURORA_MYSQL_PASSWORD", AURORA_MYSQL_PASSWORD)
            .withEnv("AURORA_MYSQL_DB", AURORA_MYSQL_DB)
            .withEnv("AURORA_MYSQL_DB_USER", AURORA_MYSQL_DB_USER)
            .withEnv("AURORA_MYSQL_DB_REGION", AURORA_MYSQL_DB_REGION)
            .withEnv("DB_CLUSTER_CONN", dbHostCluster)
            .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
            .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
            .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
            .withEnv(
                "PROXIED_CLUSTER_TEMPLATE", "?." + dbConnStrSuffix + PROXIED_DOMAIN_NAME_SUFFIX)
            .withEnv("DB_CONN_STR_SUFFIX", "." + dbConnStrSuffix)
            .withEnv("REPEAT_TIMES", REPEAT_TIMES)
            .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
            .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY);
    if (AWS_SESSION_TOKEN != null) {
      container = container.withEnv("AWS_SESSION_TOKEN", AWS_SESSION_TOKEN);
    }

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mysqlInstances.size(); i++) {
      // Add instance
      container.addEnv("MYSQL_INSTANCE_" + (i + 1) + "_URL", mysqlInstances.get(i));

      // Add proxies
      container.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    container.addEnv("AURORA_MYSQL_PORT", Integer.toString(AURORA_MYSQL_PORT));
    container.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    container.addEnv("MYSQL_PROXY_PORT", Integer.toString(mysqlProxyPort));

    System.out.println("Toxiproxy Instances port: " + mysqlProxyPort);
    System.out.println("Instances Proxied: " + mysqlInstances.size());

    container.start();

    return container;
  }
}
