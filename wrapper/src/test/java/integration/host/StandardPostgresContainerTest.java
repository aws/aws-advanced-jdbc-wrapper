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
import software.amazon.jdbc.Driver;

public class StandardPostgresContainerTest {

  private static final String STANDARD_POSTGRES_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_POSTGRES_WRITER = "standard-postgres-writer";
  private static final String STANDARD_POSTGRES_READER = "standard-postgres-reader";
  private static final List<String>
      postgresInstances = Arrays.asList(STANDARD_POSTGRES_WRITER, STANDARD_POSTGRES_READER);
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

  private static PostgreSQLContainer<?> postgresWriterContainer;
  private static PostgreSQLContainer<?> postgresReaderContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
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

    postgresWriterContainer = containerHelper.createPostgresContainer(network, STANDARD_POSTGRES_WRITER,
        STANDARD_POSTGRES_DB, STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);
    postgresWriterContainer.start();

    postgresReaderContainer = containerHelper.createPostgresContainer(network, STANDARD_POSTGRES_READER,
        STANDARD_POSTGRES_DB, STANDARD_POSTGRES_USERNAME, STANDARD_POSTGRES_PASSWORD);
    postgresReaderContainer.start();

    proxyContainers =
        containerHelper.createProxyContainers(network, postgresInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }

    postgresProxyPort = containerHelper.createInstanceProxies(postgresInstances, proxyContainers,
        STANDARD_POSTGRES_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }

    if (postgresWriterContainer != null) {
      postgresWriterContainer.stop();
    }

    if (postgresReaderContainer != null) {
      postgresReaderContainer.stop();
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
        .withEnv("STANDARD_POSTGRES_WRITER", STANDARD_POSTGRES_WRITER)
        .withEnv("STANDARD_POSTGRES_READER", STANDARD_POSTGRES_READER)
        .withEnv("STANDARD_POSTGRES_PORT", String.valueOf(STANDARD_POSTGRES_PORT))
        .withEnv("STANDARD_POSTGRES_DB", STANDARD_POSTGRES_DB)
        .withEnv("STANDARD_POSTGRES_USERNAME", STANDARD_POSTGRES_USERNAME)
        .withEnv("STANDARD_POSTGRES_PASSWORD", STANDARD_POSTGRES_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(postgresProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_WRITER", "toxiproxy-instance-1")
        .withEnv("TOXIPROXY_READER", "toxiproxy-instance-2");
  }
}
