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

import com.mysql.cj.util.StringUtils;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import software.aws.jdbc.Driver;

public class StandardPostgresContainerTest {

  private static final String STANDARD_POSTGRES_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_POSTGRES_HOST = "standard-postgres-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final int STANDARD_POSTGRES_PORT = 5432;

  private static final String STANDARD_POSTGRES_DB =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_DB"))
          ? System.getenv("STANDARD_POSTGRES_DB") : "test-db";
  private static final String STANDARD_POSTGRES_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_USERNAME"))
          ? System.getenv("STANDARD_POSTGRES_USERNAME") : "test-user";
  private static final String STANDARD_POSTGRES_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_POSTGRES_PASSWORD"))
          ? System.getenv("STANDARD_POSTGRES_PASSWORD") : "test-password";

  // "openjdk" or "graalvm". Default is "openjdk"
  private static final String TEST_CONTAINER_TYPE = System.getenv("TEST_CONTAINER_TYPE");

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

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    network = Network.newNetwork();

    postgresContainer = containerHelper.createPostgresContainer(network, STANDARD_POSTGRES_HOST,
        STANDARD_POSTGRES_DB, STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);
    postgresContainer.start();

    proxyContainer =
        containerHelper.createProxyContainer(network, STANDARD_POSTGRES_HOST, PROXIED_DOMAIN_NAME_SUFFIX);
    proxyContainer.start();
    postgresProxyPort = containerHelper.createInstanceProxy(STANDARD_POSTGRES_HOST, proxyContainer,
        STANDARD_POSTGRES_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    if (proxyContainer != null) {
      proxyContainer.stop();
    }
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
    if (integrationTestContainer != null) {
      integrationTestContainer.stop();
    }
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
    return containerHelper.createTestContainerByType(TEST_CONTAINER_TYPE, "aws/rds-test-container")
        .withNetworkAliases(STANDARD_POSTGRES_TEST_RUNNER_NAME)
        .withNetwork(network)
        .withEnv("STANDARD_POSTGRES_HOST", STANDARD_POSTGRES_HOST)
        .withEnv("STANDARD_POSTGRES_PORT", String.valueOf(STANDARD_POSTGRES_PORT))
        .withEnv("STANDARD_POSTGRES_DB", STANDARD_POSTGRES_DB)
        .withEnv("STANDARD_POSTGRES_USERNAME", STANDARD_POSTGRES_USERNAME)
        .withEnv("STANDARD_POSTGRES_PASSWORD", STANDARD_POSTGRES_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(postgresProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance");
  }
}
