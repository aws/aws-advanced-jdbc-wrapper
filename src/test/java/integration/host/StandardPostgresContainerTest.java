/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.host;

import static org.junit.jupiter.api.Assertions.fail;

import integration.util.ContainerHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

public class StandardPostgresContainerTest {

  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final int POSTGRES_PORT = 5432;
  private static final String TEST_DB = "test";
  private static final String POSTGRES_USERNAME = "postgres";
  private static final String POSTGRES_PASSWORD = "";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static final List<String> postgresInstances = List.of(TEST_CONTAINER_NAME);
  private static PostgreSQLContainer<?> postgresContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static int postgresProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() {
    network = Network.newNetwork();

    postgresContainer = containerHelper.createPostgresContainer(network, TEST_CONTAINER_NAME, TEST_DB);
    postgresContainer.start();

    proxyContainers = containerHelper.createProxyContainers(network, postgresInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    postgresProxyPort = containerHelper.createInstanceProxies(postgresInstances, proxyContainers, POSTGRES_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();

    try {
      integrationTestContainer.execInContainer("dos2unix", "gradlew");
    } catch (InterruptedException | UnsupportedOperationException | IOException e) {
      fail("Standard Postgres integration test container initialised incorrectly");
    }
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }

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
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(network)
        .withEnv("TEST_HOST", TEST_CONTAINER_NAME)
        .withEnv("TEST_PORT", String.valueOf(POSTGRES_PORT))
        .withEnv("TEST_DB", TEST_DB)
        .withEnv("TEST_USERNAME", POSTGRES_USERNAME)
        .withEnv("TEST_PASSWORD", POSTGRES_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(postgresProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance-1");
  }
}
