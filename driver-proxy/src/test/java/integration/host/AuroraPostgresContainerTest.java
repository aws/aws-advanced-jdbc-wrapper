/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.host;

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
import software.aws.rds.jdbc.proxydriver.util.StringUtils;

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
public class AuroraPostgresContainerTest {

  private static final int AURORA_POSTGRES_PORT = 5432;
  private static final String AURORA_POSTGRES_TEST_HOST_NAME = "test-container";

  private static final String AURORA_POSTGRES_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_USERNAME"))
          ? System.getenv("AURORA_POSTGRES_USERNAME")
          : "my_test_username";
  private static final String AURORA_POSTGRES_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_PASSWORD"))
          ? System.getenv("AURORA_POSTGRES_PASSWORD")
          : "my_test_password";
  protected static final String AURORA_POSTGRES_DB =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_DB"))
          ? System.getenv("AURORA_POSTGRES_DB")
          : "test";

  private static final String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
  private static final String AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
  private static final String AWS_SESSION_TOKEN = System.getenv("AWS_SESSION_TOKEN");

  private static final String DB_CONN_STR_PREFIX = "aws-proxy-jdbc:postgresql://";
  private static String dbConnStrSuffix = "";
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2";

  private static final String AURORA_POSTGRES_DB_REGION =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_DB_REGION"))
          ? System.getenv("AURORA_POSTGRES_DB_REGION")
          : "us-east-1";
  private static final String AURORA_POSTGRES_CLUSTER_IDENTIFIER =
      !StringUtils.isNullOrEmpty(System.getenv("AURORA_POSTGRES_CLUSTER_IDENTIFIER"))
          ? System.getenv("AURORA_POSTGRES_CLUSTER_IDENTIFIER")
          : "test-identifier";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static List<String> postgresInstances = new ArrayList<>();

  private static int postgresProxyPort;
  private static GenericContainer<?> integrationTestContainer;
  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";
  private static String runnerIP = null;

  private static Network network;

  private static final ContainerHelper containerHelper = new ContainerHelper();
  private static final AuroraTestUtility auroraUtil = new AuroraTestUtility(AURORA_POSTGRES_DB_REGION);

  @BeforeAll
  static void setUp() throws SQLException, InterruptedException, UnknownHostException {
    Assertions.assertNotNull(AWS_ACCESS_KEY_ID);
    Assertions.assertNotNull(AWS_SECRET_ACCESS_KEY);
    Assertions.assertNotNull(AWS_SESSION_TOKEN);

    // Comment out below to not create a new cluster & instances
    // Note: You will need to set it to the proper DB Conn Suffix
    // i.e. For "database-cluster-name.XYZ.us-east-2.rds.amazonaws.com"
    // dbConnStrSuffix = "XYZ.us-east-2.rds.amazonaws.com"
    dbConnStrSuffix =
        auroraUtil.createCluster(AURORA_POSTGRES_USERNAME, AURORA_POSTGRES_PASSWORD, AURORA_POSTGRES_DB,
            AURORA_POSTGRES_CLUSTER_IDENTIFIER);

    // Comment out getting public IP to not add & remove from EC2 whitelist
    runnerIP = auroraUtil.getPublicIPAddress();
    auroraUtil.ec2AuthorizeIP(runnerIP);

    dbHostCluster = AURORA_POSTGRES_CLUSTER_IDENTIFIER + ".cluster-" + dbConnStrSuffix;
    dbHostClusterRo = AURORA_POSTGRES_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnStrSuffix;

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    network = Network.newNetwork();
    postgresInstances =
        containerHelper.getAuroraInstanceEndpoints(
            DB_CONN_STR_PREFIX + dbHostCluster + "/" + AURORA_POSTGRES_DB + DB_CONN_PROP,
            AURORA_POSTGRES_USERNAME,
            AURORA_POSTGRES_PASSWORD,
            dbConnStrSuffix);
    proxyContainers =
        containerHelper.createProxyContainers(
            network, postgresInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    postgresProxyPort =
        containerHelper.createInstanceProxies(
            postgresInstances, proxyContainers, AURORA_POSTGRES_PORT);

    proxyContainers.add(
        containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-instance-cluster",
            dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostCluster,
            AURORA_POSTGRES_PORT,
            postgresProxyPort));

    proxyContainers.add(
        containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-ro-instance-cluster",
            dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostClusterRo,
            AURORA_POSTGRES_PORT,
            postgresProxyPort));

    integrationTestContainer = initializeTestContainer(network, postgresInstances);
  }

  @AfterAll
  static void tearDown() {
    // Comment below out if you don't want to delete cluster after tests finishes
    if (StringUtils.isNullOrEmpty(AURORA_POSTGRES_CLUSTER_IDENTIFIER)) {
      auroraUtil.deleteCluster();
    } else {
      auroraUtil.deleteCluster(AURORA_POSTGRES_CLUSTER_IDENTIFIER);
    }

    auroraUtil.ec2DeauthorizesIP(runnerIP);
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-postgres");
  }

  @Test
  public void runPerformanceTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-postgres-performance");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora-postgres");
  }

  @Test
  public void debugPerformanceTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora-postgres-performance");
  }

  protected static GenericContainer<?> initializeTestContainer(
      final Network network, List<String> postgresInstances) {

    final GenericContainer<?> container =
        containerHelper
            .createTestContainer("aws/rds-test-container")
            .withNetworkAliases(AURORA_POSTGRES_TEST_HOST_NAME)
            .withNetwork(network)
            .withEnv("TEST_USERNAME", AURORA_POSTGRES_USERNAME)
            .withEnv("TEST_PASSWORD", AURORA_POSTGRES_PASSWORD)
            .withEnv("TEST_DB", AURORA_POSTGRES_DB)
            .withEnv("DB_REGION", AURORA_POSTGRES_DB_REGION)
            .withEnv("DB_CLUSTER_CONN", dbHostCluster)
            .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
            .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
            .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
            .withEnv(
                "PROXIED_CLUSTER_TEMPLATE", "?." + dbConnStrSuffix + PROXIED_DOMAIN_NAME_SUFFIX)
            .withEnv("DB_CONN_STR_SUFFIX", "." + dbConnStrSuffix)
            .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
            .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
            .withEnv("AWS_SESSION_TOKEN", AWS_SESSION_TOKEN);

    // Add postgres instances & proxies to container env
    for (int i = 0; i < postgresInstances.size(); i++) {
      // Add instance
      container.addEnv("POSTGRES_INSTANCE_" + (i + 1) + "_URL", postgresInstances.get(i));

      // Add proxies
      container.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS", "toxiproxy-instance-" + (i + 1));
    }
    container.addEnv("POSTGRES_PORT", Integer.toString(AURORA_POSTGRES_PORT));
    container.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    container.addEnv("POSTGRES_PROXY_PORT", Integer.toString(postgresProxyPort));

    System.out.println("Toxiproxy Instances port: " + postgresProxyPort);
    System.out.println("Instances Proxied: " + postgresInstances.size());

    container.start();

    return container;
  }
}
