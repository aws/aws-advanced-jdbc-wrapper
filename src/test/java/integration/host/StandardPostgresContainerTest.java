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
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

public class StandardPostgresContainerTest {

  private static final String STANDARD_POSTGRES_TEST_CONTAINER_NAME = "standard-postgres-test-container";
  private static final int STANDARD_POSTGRES_PORT = 5432;
  private static final String STANDARD_POSTGRES_DB = "test";
  private static final String STANDARD_POSTGRES_USERNAME = "test";
  private static final String STANDARD_POSTGRES_PASSWORD = "test";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static final List<String> postgresInstances = Arrays.asList(STANDARD_POSTGRES_TEST_CONTAINER_NAME);
  private static PostgreSQLContainer<?> postgresContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static int postgresProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() {
    network = Network.newNetwork();

    postgresContainer = containerHelper.createPostgresContainer(network, STANDARD_POSTGRES_TEST_CONTAINER_NAME,
        STANDARD_POSTGRES_DB, STANDARD_POSTGRES_PASSWORD);
    postgresContainer.start();

    proxyContainers = containerHelper.createProxyContainers(network, postgresInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    postgresProxyPort = containerHelper.createInstanceProxies(postgresInstances, proxyContainers,
        STANDARD_POSTGRES_PORT);

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
        .withNetworkAliases(STANDARD_POSTGRES_TEST_CONTAINER_NAME)
        .withNetwork(network)
        .withEnv("TEST_HOST", STANDARD_POSTGRES_TEST_CONTAINER_NAME)
        .withEnv("TEST_PORT", String.valueOf(STANDARD_POSTGRES_PORT))
        .withEnv("TEST_DB", STANDARD_POSTGRES_DB)
        .withEnv("TEST_USERNAME", STANDARD_POSTGRES_USERNAME)
        .withEnv("TEST_PASSWORD", STANDARD_POSTGRES_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(postgresProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance-1");
  }
}
