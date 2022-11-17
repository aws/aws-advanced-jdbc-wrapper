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
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import software.amazon.jdbc.Driver;

public class StandardMariadbContainerTest {

  private static final String STANDARD_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_MARIADB_WRITER = "standard-mariadb-writer";
  private static final String STANDARD_MARIADB_READER = "standard-mariadb-reader";
  private static final List<String> mariadbInstances = Arrays.asList(STANDARD_MARIADB_WRITER, STANDARD_MARIADB_READER);
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

  private static MariaDBContainer<?> mariadbWriter;
  private static MariaDBContainer<?> mariadbReader;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers;
  private static int mariadbProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException, ClassNotFoundException {
    Class.forName("org.mariadb.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    network = Network.newNetwork();

    mariadbWriter = containerHelper.createMariadbContainer(network, STANDARD_MARIADB_WRITER,
        STANDARD_MARIADB_DB, STANDARD_MARIADB_USERNAME, STANDARD_MARIADB_PASSWORD);
    mariadbWriter.start();

    mariadbReader = containerHelper.createMariadbContainer(network, STANDARD_MARIADB_READER,
        STANDARD_MARIADB_DB, STANDARD_MARIADB_USERNAME, STANDARD_MARIADB_PASSWORD);
    mariadbReader.start();

    proxyContainers = containerHelper.createProxyContainers(network, mariadbInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }

    mariadbProxyPort = containerHelper.createInstanceProxies(mariadbInstances, proxyContainers, STANDARD_MARIADB_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }

    if (mariadbWriter != null) {
      mariadbWriter.stop();
    }

    if (mariadbReader != null) {
      mariadbReader.stop();
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
        .withEnv("STANDARD_MARIADB_WRITER", STANDARD_MARIADB_WRITER)
        .withEnv("STANDARD_MARIADB_READER", STANDARD_MARIADB_READER)
        .withEnv("STANDARD_MARIADB_PORT", String.valueOf(STANDARD_MARIADB_PORT))
        .withEnv("STANDARD_MARIADB_DB", STANDARD_MARIADB_DB)
        .withEnv("STANDARD_MARIADB_USERNAME", STANDARD_MARIADB_USERNAME)
        .withEnv("STANDARD_MARIADB_PASSWORD", STANDARD_MARIADB_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(mariadbProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_WRITER", "toxiproxy-instance-1")
        .withEnv("TOXIPROXY_READER", "toxiproxy-instance-2");
  }
}
