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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import software.amazon.jdbc.Driver;

public class StandardMysqlContainerTest {

  private static final String STANDARD_TEST_RUNNER_NAME = "test-container";
  private static final String STANDARD_MYSQL_WRITER = "standard-mysql-writer";
  private static final String STANDARD_MYSQL_READER = "standard-mysql-reader";
  private static final List<String>
      mySqlInstances = Arrays.asList(STANDARD_MYSQL_WRITER, STANDARD_MYSQL_READER);
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

  private static final String TEST_CONTAINER_TYPE = System.getenv("TEST_CONTAINER_TYPE");

  private static MySQLContainer<?> mysqlWriterContainer;
  private static MySQLContainer<?> mysqlReaderContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static int mysqlProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    network = Network.newNetwork();

    mysqlWriterContainer = containerHelper.createMysqlContainer(network, STANDARD_MYSQL_WRITER,
        STANDARD_MYSQL_DB, STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);
    mysqlWriterContainer.start();

    mysqlReaderContainer = containerHelper.createMysqlContainer(network, STANDARD_MYSQL_READER,
        STANDARD_MYSQL_DB, STANDARD_MYSQL_USERNAME, STANDARD_MYSQL_PASSWORD);
    mysqlReaderContainer.start();

    proxyContainers =
        containerHelper.createProxyContainers(network, mySqlInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }

    mysqlProxyPort = containerHelper.createInstanceProxies(mySqlInstances, proxyContainers,
        STANDARD_MYSQL_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }

    if (mysqlWriterContainer != null) {
      mysqlWriterContainer.stop();
    }

    if (mysqlReaderContainer != null) {
      mysqlReaderContainer.stop();
    }

    if (integrationTestContainer != null) {
      integrationTestContainer.stop();
    }
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
    return containerHelper.createTestContainerByType(TEST_CONTAINER_TYPE, "aws/rds-test-container")
        .withNetworkAliases(STANDARD_TEST_RUNNER_NAME)
        .withNetwork(network)
        .withEnv("STANDARD_MYSQL_WRITER", STANDARD_MYSQL_WRITER)
        .withEnv("STANDARD_MYSQL_READER", STANDARD_MYSQL_READER)
        .withEnv("STANDARD_MYSQL_PORT", String.valueOf(STANDARD_MYSQL_PORT))
        .withEnv("STANDARD_MYSQL_DB", STANDARD_MYSQL_DB)
        .withEnv("STANDARD_MYSQL_USERNAME", STANDARD_MYSQL_USERNAME)
        .withEnv("STANDARD_MYSQL_PASSWORD", STANDARD_MYSQL_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(mysqlProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_WRITER", "toxiproxy-instance-1")
        .withEnv("TOXIPROXY_READER", "toxiproxy-instance-2");
  }
}
