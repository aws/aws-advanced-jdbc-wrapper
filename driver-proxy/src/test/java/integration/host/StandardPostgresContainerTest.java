/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.host;

import com.mysql.cj.util.StringUtils;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import integration.util.ContainerHelper;

public class StandardPostgresContainerTest {

  private static final String STANDARD_POSTGRES_TEST_HOST_NAME = "standard-postgres-test-host";
  private static final String STANDARD_POSTGRES_CONTAINER_NAME = "standard-postgres-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final int STANDARD_POSTGRES_PORT = 5432;

  private static final String STANDARD_POSTGRES_DB =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_DB"))
          ? System.getenv("STANDARD_POSTGRES_DB") : "test";
  private static final String STANDARD_POSTGRES_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_USERNAME"))
          ? System.getenv("STANDARD_POSTGRES_USERNAME") : "test";
  private static final String STANDARD_POSTGRES_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_PASSWORD"))
          ? System.getenv("STANDARD_POSTGRES_PASSWORD") : "test";

  private static PostgreSQLContainer<?> postgresContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static ToxiproxyContainer proxyContainer;
  private static int postgresProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException {
    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    network = Network.newNetwork();

    postgresContainer = containerHelper.createPostgresContainer(network, STANDARD_POSTGRES_CONTAINER_NAME,
        STANDARD_POSTGRES_DB, STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);
    postgresContainer.start();

    proxyContainer =
        containerHelper.createProxyContainer(network, STANDARD_POSTGRES_CONTAINER_NAME, PROXIED_DOMAIN_NAME_SUFFIX);
    proxyContainer.start();
    postgresProxyPort = containerHelper.createInstanceProxy(STANDARD_POSTGRES_CONTAINER_NAME, proxyContainer,
        STANDARD_POSTGRES_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    proxyContainer.stop();
    postgresContainer.stop();
    integrationTestContainer.stop();
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-standard-postgres");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-standard-postgres");
  }

  protected static GenericContainer<?> createTestContainer() {
    return containerHelper.createTestContainer("aws/rds-test-container")
        .withNetworkAliases(STANDARD_POSTGRES_TEST_HOST_NAME)
        .withNetwork(network)
        .withEnv("TEST_HOST", STANDARD_POSTGRES_CONTAINER_NAME)
        .withEnv("TEST_PORT", String.valueOf(STANDARD_POSTGRES_PORT))
        .withEnv("TEST_DB", STANDARD_POSTGRES_DB)
        .withEnv("TEST_USERNAME", STANDARD_POSTGRES_USERNAME)
        .withEnv("TEST_PASSWORD", STANDARD_POSTGRES_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(postgresProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance");
  }
}
