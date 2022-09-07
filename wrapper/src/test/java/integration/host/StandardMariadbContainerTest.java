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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import software.amazon.jdbc.Driver;

public class StandardMariadbContainerTest {

  private static final String STANDARD_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_MARIADB_HOST = "standard-mariadb-container";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final int STANDARD_MARIADB_PORT = 3306;

  private static final String STANDARD_MARIADB_DB =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MARIADB_DB"))
          ? System.getenv("STANDARD_MARIADB_DB") : "test";
  private static final String STANDARD_MARIADB_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MARIADB_USERNAME"))
          ? System.getenv("STANDARD_MARIADB_USERNAME") : "test";
  private static final String STANDARD_MARIADB_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("STANDARD_MARIADB_PASSWORD"))
          ? System.getenv("STANDARD_MARIADB_PASSWORD") : "test";

  private static final String TEST_CONTAINER_TYPE = System.getenv("TEST_CONTAINER_TYPE");

  private static MySQLContainer<?> mariadbContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static ToxiproxyContainer proxyContainer;
  private static int mariadbProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException, ClassNotFoundException {
    Class.forName("com.mariadb.cj.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    network = Network.newNetwork();

    mariadbContainer = containerHelper.createMysqlContainer(network, STANDARD_MARIADB_HOST,
        STANDARD_MARIADB_DB, STANDARD_MARIADB_USERNAME, STANDARD_MARIADB_PASSWORD);
    mariadbContainer.start();

    proxyContainer =
        containerHelper.createProxyContainer(network, STANDARD_MARIADB_HOST, PROXIED_DOMAIN_NAME_SUFFIX);
    proxyContainer.start();
    mariadbProxyPort = containerHelper.createInstanceProxy(STANDARD_MARIADB_HOST, proxyContainer,
        STANDARD_MARIADB_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    if (proxyContainer != null) {
      proxyContainer.stop();
    }
    if (mariadbContainer != null) {
      mariadbContainer.stop();
    }
    if (integrationTestContainer != null) {
      integrationTestContainer.stop();
    }
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-standard-mariadb");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-standard-mariadb");
  }

  protected static GenericContainer<?> createTestContainer() {
    return containerHelper.createTestContainerByType(TEST_CONTAINER_TYPE, "aws/rds-test-container")
        .withNetworkAliases(STANDARD_TEST_RUNNER_NAME)
        .withNetwork(network)
        .withEnv("STANDARD_MARIADB_HOST", STANDARD_MARIADB_HOST)
        .withEnv("STANDARD_MARIADB_PORT", String.valueOf(STANDARD_MARIADB_PORT))
        .withEnv("STANDARD_MARIADB_DB", STANDARD_MARIADB_DB)
        .withEnv("STANDARD_MARIADB_USERNAME", STANDARD_MARIADB_USERNAME)
        .withEnv("STANDARD_MARIADB_PASSWORD", STANDARD_MARIADB_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(mariadbProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_HOST", "toxiproxy-instance");
  }
}
