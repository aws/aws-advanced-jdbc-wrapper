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

package integration.hibernate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.util.ContainerHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

public class HibernateContainerTest {

  protected static String DATABASE = System.getenv("DB_DATABASE_NAME") != null
      ? System.getenv("DB_DATABASE_NAME")
      : "hibernate_orm_test";
  protected static String USERNAME = System.getenv("DB_USERNAME") != null
      ? System.getenv("DB_USERNAME")
      : "hibernate_orm_test";
  protected static String PASSWORD = System.getenv("DB_PASSWORD") != null
      ? System.getenv("DB_PASSWORD")
      : "hibernate_orm_test";

  private static PostgreSQLContainer<?> postgresContainer;
  private static MySQLContainer<?> mySQLContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static final ContainerHelper containerHelper = new ContainerHelper();
  private static final Map<String, String> hibernateExistingConfigFiles = new HashMap<>();
  private static final Map<String, String> hibernateNewConfigFiles = new HashMap<>();
  private static final String HIBERNATE_CONFIG_DIRECTORY = "src/test/resources/hibernate_files/";
  private static final String HIBERNATE_REPO_DIRECTORY = "../hibernate-orm/";
  private static final String POSTGRES_NETWORK_ALIAS = "postgres-container";
  private static final String MYSQL_NETWORK_ALIAS = "mysql-container";

  static {
    // Existing config files to replace
    hibernateExistingConfigFiles.put("databases.gradle", "gradle/databases.gradle");
    hibernateExistingConfigFiles.put(
        "hibernate-core.gradle",
        "hibernate-core/hibernate-core.gradle");
    hibernateExistingConfigFiles.put(
        "java-module.gradle",
        "gradle/java-module.gradle");

    // New files to create
    hibernateNewConfigFiles.put(
        "hibernate.properties",
        "databases/amazon/resources/hibernate.properties");
    hibernateNewConfigFiles.put(
        "matrix.gradle",
        "databases/amazon/matrix.gradle");
  }

  @BeforeEach
  public void setup() throws SQLException, IOException, InterruptedException {
    final Network network = Network.newNetwork();

    postgresContainer = containerHelper.createPostgresContainer(
        network,
        POSTGRES_NETWORK_ALIAS,
        DATABASE,
        USERNAME,
        PASSWORD);
    postgresContainer.start();

    mySQLContainer = containerHelper.createMysqlContainer(
        network,
        MYSQL_NETWORK_ALIAS,
        DATABASE,
        USERNAME,
        PASSWORD);
    mySQLContainer.start();

    copyAndReplaceHibernateConfigFiles();
    createNewHibernateConfigFiles();

    final String containerImageName = containerHelper.getContainerImageName("openjdk11");
    integrationTestContainer = containerHelper.createTestContainer("aws/rds-test-container", containerImageName)
        .withNetwork(network)
        .withNetworkAliases("test-container")
        .withFileSystemBind("../hibernate-orm", "/app/hibernate-orm")
        .withEnv("DB_DATABASE_NAME", DATABASE)
        .withEnv("DB_USERNAME", USERNAME)
        .withEnv("DB_PASSWORD", PASSWORD);

    integrationTestContainer.start();
  }

  @AfterEach
  public void tearDown() {
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
    if (mySQLContainer != null) {
      mySQLContainer.stop();
    }
    if (integrationTestContainer != null) {
      integrationTestContainer.stop();
    }
  }

  @Test
  public void testPGHibernate() throws IOException, InterruptedException {
    assertEquals(
        0,
        containerHelper.runCmdInDirectory(
            integrationTestContainer,
            "/app/hibernate-orm",
            "./gradlew", "test", "-Pdb=amazon_ci", "-DdbHost=" + POSTGRES_NETWORK_ALIAS,
            "-PexcludeTests=PostgreSQLSkipAutoCommitTest"),
        "failed to run Hibernate ORM tests");
  }

  @Test
  public void testMySQLHibernate() throws IOException, InterruptedException {
    assertEquals(
        0,
        containerHelper.runCmdInDirectory(
            integrationTestContainer,
            "/app/hibernate-orm",
            "./gradlew", "test", "-Pdb=amazon_mysql_ci", "-DdbHost=" + MYSQL_NETWORK_ALIAS,
            "-PexcludeTests=MySQLSkipAutoCommitTest"),
        "failed to run Hibernate ORM tests");
  }

  @Test
  public void debugPGHibernate() throws IOException, InterruptedException {
    assertEquals(0,
        containerHelper.runCmdInDirectory(
            integrationTestContainer,
            "/app/hibernate-orm",
            "./gradlew", "test", "-Pdb=amazon_ci", "-DdbHost=" + POSTGRES_NETWORK_ALIAS,
            "-PexcludeTests=PostgreSQLSkipAutoCommitTest", "--debug-jvm", "--no-parallel", "--no-daemon"),
        "failed to debug Hibernate ORM tests"
    );
  }

  @Test
  public void debugMySQLHibernate() throws IOException, InterruptedException {
    assertEquals(0,
        containerHelper.runCmdInDirectory(
            integrationTestContainer,
            "/app/hibernate-orm",
            "./gradlew", "test", "-Pdb=amazon_mysql_ci", "-DdbHost=" + MYSQL_NETWORK_ALIAS,
            "-PexcludeTests=MySQLSkipAutoCommitTest", "--debug-jvm", "--no-parallel", "--no-daemon"),
        "failed to debug Hibernate ORM tests"
    );
  }

  private void copyAndReplaceHibernateConfigFiles() {
    hibernateExistingConfigFiles.forEach((src, dest) -> {
      final File sourceFile = new File(HIBERNATE_CONFIG_DIRECTORY, src);
      final File destinationFile = new File(HIBERNATE_REPO_DIRECTORY, dest);

      assertTrue(sourceFile.exists(), sourceFile.getAbsolutePath() + " doesn't exists.");
      assertTrue(destinationFile.exists(), destinationFile.getAbsolutePath() + " doesn't exists.");

      try {
        containerHelper.replaceFiles(sourceFile, destinationFile);
      } catch (IOException e) {
        fail("Unable to set up Hibernate config files.");
      }
    });
  }

  private void createNewHibernateConfigFiles() {
    hibernateNewConfigFiles.forEach((src, dest) -> {
      final File sourceFile = new File(HIBERNATE_CONFIG_DIRECTORY, src);
      final File destinationFile = new File(HIBERNATE_REPO_DIRECTORY, dest);

      try {
        Files.createDirectories(Paths.get(destinationFile.getParent()));
        containerHelper.replaceFiles(sourceFile, destinationFile);
      } catch (IOException e) {
        fail("Unable to set up Hibernate config files.");
      }
    });
  }
}
