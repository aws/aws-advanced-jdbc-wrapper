/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.host;

import com.mysql.cj.util.StringUtils;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

public class StandardMysqlContainerTest {

  private static final String STANDARD_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_MYSQL_HOST = "standard-mysql-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final int STANDARD_MYSQL_PORT = 3306;

  private static final String STANDARD_MYSQL_DB =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MYSQL_DB"))
          ? System.getenv("STANDARD_MYSQL_DB") : "test";
  private static final String STANDARD_MYSQL_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MYSQL_USERNAME"))
          ? System.getenv("STANDARD_MYSQL_USERNAME") : "test";
  private static final String STANDARD_MYSQL_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MYSQL_PASSWORD"))
          ? System.getenv("STANDARD_MYSQL_PASSWORD") : "test";

  private static MySQLContainer<?> mysqlContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static ToxiproxyContainer proxyContainer;
  private static int mysqlProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    network = Network.newNetwork();

    mysqlContainer = containerHelper.createMysqlContainer(network, STANDARD_MYSQL_HOST,
        STANDARD_MYSQL_DB, STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);
    mysqlContainer.start();

    proxyContainer =
        containerHelper.createProxyContainer(network, STANDARD_MYSQL_HOST, PROXIED_DOMAIN_NAME_SUFFIX);
    proxyContainer.start();
    mysqlProxyPort = containerHelper.createInstanceProxy(STANDARD_MYSQL_HOST, proxyContainer,
        STANDARD_MYSQL_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    proxyContainer.stop();
    mysqlContainer.stop();
    integrationTestContainer.stop();
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-standard-mysql");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-standard-mysql");
  }

  protected static GenericContainer<?> createTestContainer() {
    return containerHelper.createTestContainer("aws/rds-test-container")
        .withNetworkAliases(STANDARD_TEST_RUNNER_NAME)
        .withNetwork(network)
        .withEnv("STANDARD_MYSQL_HOST", STANDARD_MYSQL_HOST)
        .withEnv("STANDARD_MYSQL_PORT", String.valueOf(STANDARD_MYSQL_PORT))
        .withEnv("STANDARD_MYSQL_DB", STANDARD_MYSQL_DB)
        .withEnv("STANDARD_MYSQL_USERNAME", STANDARD_MYSQL_USERNAME)
        .withEnv("STANDARD_MYSQL_PASSWORD", STANDARD_MYSQL_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(mysqlProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance");
  }
}
