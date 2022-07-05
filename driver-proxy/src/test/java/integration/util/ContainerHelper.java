/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.TestEnvironment;

public class ContainerHelper {
  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";
  private static final String MYSQL_CONTAINER_IMAGE_NAME = "mysql:8.0.28";
  private static final String POSTGRES_CONTAINER_IMAGE_NAME = "postgres:latest";
  private static final DockerImageName TOXIPROXY_IMAGE =
      DockerImageName.parse("shopify/toxiproxy:2.1.4");

  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
          + "ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END";
  private static final String SERVER_ID = "SERVER_ID";

  public void runTest(GenericContainer<?> container, String task)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, consumer, "./gradlew", task);
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  public void debugTest(GenericContainer<?> container, String task)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, consumer, "./gradlew", task, "--debug-jvm");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  public GenericContainer<?> createTestContainer(String dockerImageName) {
    return createTestContainer(dockerImageName, TEST_CONTAINER_IMAGE_NAME);
  }

  public GenericContainer<?> createTestContainer(
      String dockerImageName, String testContainerImageName) {
    class FixedExposedPortContainer<T extends GenericContainer<T>>
        extends GenericContainer<T> {

      public FixedExposedPortContainer(ImageFromDockerfile withDockerfileFromBuilder) {
        super(withDockerfileFromBuilder);
      }

      public T withFixedExposedPort(int hostPort, int containerPort) {
        super.addFixedExposedPort(hostPort, containerPort, InternetProtocol.TCP);

        return self();
      }
    }

    return new FixedExposedPortContainer<>(
        new ImageFromDockerfile(dockerImageName, true)
            .withDockerfileFromBuilder(
                builder ->
                    builder
                        .from(testContainerImageName)
                        .run("mkdir", "app")
                        .workDir("/app")
                        .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                        .expose(5005) // Exposing ports for debugger to be attached
                        .build()))
        .withFixedExposedPort(5005, 5005) // Mapping container port to host
        .withFileSystemBind(
            "./build/reports/tests",
            "/app/build/reports/tests",
            BindMode.READ_WRITE) // some tests may write some files here
        .withFileSystemBind("../gradle", "/app/gradle", BindMode.READ_WRITE)
        .withPrivilegedMode(true) // it's needed to control Linux core settings like TcpKeepAlive
        .withCopyFileToContainer(MountableFile.forHostPath("./build/classes/java/test"), "app/test")
        .withCopyFileToContainer(MountableFile.forHostPath("../gradlew"), "app/gradlew")
        .withCopyFileToContainer(MountableFile.forHostPath("./build/libs"), "app/libs")
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/build.gradle.kts"), "app/build.gradle.kts");
  }

  protected Long execInContainer(
      GenericContainer<?> container, Consumer<OutputFrame> consumer, String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container, consumer, StandardCharsets.UTF_8, command);
  }

  protected Long execInContainer(
      GenericContainer<?> container,
      Consumer<OutputFrame> consumer,
      Charset outputCharset,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, outputCharset, command);
  }

  protected Long execInContainer(
      InspectContainerResponse containerInfo,
      Consumer<OutputFrame> consumer,
      Charset outputCharset,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    if (!TestEnvironment.dockerExecutionDriverSupportsExec()) {
      // at time of writing, this is the expected result in CircleCI.
      throw new UnsupportedOperationException(
          "Your docker daemon is running the \"lxc\" driver, which doesn't support \"docker exec\".");
    }

    if (!isRunning(containerInfo)) {
      throw new IllegalStateException(
          "execInContainer can only be used while the Container is running");
    }

    final String containerId = containerInfo.getId();
    final DockerClient dockerClient = DockerClientFactory.instance().client();

    final ExecCreateCmdResponse execCreateCmdResponse =
        dockerClient
            .execCreateCmd(containerId)
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withCmd(command)
            .exec();

    try (final FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, consumer);
      callback.addConsumer(OutputFrame.OutputType.STDERR, consumer);
      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }

    return dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCodeLong();
  }

  protected boolean isRunning(InspectContainerResponse containerInfo) {
    try {
      return containerInfo != null && containerInfo.getState() != null && containerInfo.getState().getRunning();
    } catch (DockerException e) {
      return false;
    }
  }

  public MySQLContainer<?> createMysqlContainer(
      Network network, String networkAlias, String testDbName) {
    return createMysqlContainer(network, networkAlias, testDbName, "root");
  }

  public MySQLContainer<?> createMysqlContainer(
      Network network, String networkAlias, String testDbName, String password) {

    return new MySQLContainer<>(MYSQL_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withPassword(password)
        .withFileSystemBind("src/test/config/ssl-test-certs/", "/home/certdir/", BindMode.READ_WRITE)
        .withFileSystemBind("src/test/config/plugins/", "/home/plugin_dir/", BindMode.READ_WRITE)
        .withFileSystemBind(
            "src/test/config/docker-entrypoint-initdb.d",
            "/docker-entrypoint-initdb.d",
            BindMode.READ_WRITE)
        .withCommand(
            "--local_infile=1",
            "--max_allowed_packet=40M",
            "--max-connections=2048",
            "--secure-file-priv=/var/lib/mysql",
            "--ssl-key=/home/certdir/server-key.pem",
            "--ssl-cert=/home/certdir/server-cert.pem",
            "--ssl-ca=/home/certdir/ca-cert.pem",
            "--plugin_dir=/home/plugin_dir",
            "--log-error-verbosity=4",
            "--default-authentication-plugin=sha256_password",
            "--sha256_password_public_key_path=/home/certdir/mykey.pub",
            "--sha256_password_private_key_path=/home/certdir/mykey.pem",
            "--caching_sha2_password_public_key_path=/home/certdir/mykey.pub",
            "--caching_sha2_password_private_key_path=/home/certdir/mykey.pem");
  }

  public PostgreSQLContainer<?> createPostgresContainer(
      Network network, String networkAlias, String testDbName) {
    return createPostgresContainer(network, networkAlias, testDbName, "root");
  }

  public PostgreSQLContainer<?> createPostgresContainer(
      Network network, String networkAlias, String testDbName, String password) {

    return new PostgreSQLContainer<>(POSTGRES_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withPassword(password);
  }

  public ToxiproxyContainer createAndStartProxyContainer(
      final Network network,
      String networkAlias,
      String networkUrl,
      String hostname,
      int port,
      int expectedProxyPort) {
    final ToxiproxyContainer container =
        new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(networkAlias, networkUrl);
    container.start();
    ToxiproxyContainer.ContainerProxy proxy = container.getProxy(hostname, port);
    assertEquals(
        expectedProxyPort,
        proxy.getOriginalProxyPort(),
        "Proxy port for " + hostname + " should be " + expectedProxyPort);
    return container;
  }

  public List<String> getAuroraInstanceEndpoints(
      String connectionUrl, String userName, String password, String hostBase) throws SQLException {

    ArrayList<String> auroraInstances = new ArrayList<>();

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {
      // Get instances
      try (final ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
        while (resultSet.next()) {
          // Get Instance endpoints
          final String hostEndpoint = resultSet.getString(SERVER_ID) + "." + hostBase;
          auroraInstances.add(hostEndpoint);
        }
      }
    }
    return auroraInstances;
  }

  public List<String> getAuroraInstanceIds(String connectionUrl, String userName, String password)
      throws SQLException {

    ArrayList<String> auroraInstances = new ArrayList<>();

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {
      // Get instances
      try (final ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
        while (resultSet.next()) {
          // Get Instance endpoints
          final String hostEndpoint = resultSet.getString(SERVER_ID);
          auroraInstances.add(hostEndpoint);
        }
      }
    }
    return auroraInstances;
  }

  public void addAuroraAwsIamUser(
      String connectionUrl, String userName, String password, String dbUser)
      throws SQLException {

    final String dropAwsIamUserSQL = "DROP USER IF EXISTS " + dbUser + ";";
    final String createAwsIamUserSQL =
        "CREATE USER " + dbUser + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';";
    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {
      stmt.execute(dropAwsIamUserSQL);
      stmt.execute(createAwsIamUserSQL);
    }
  }

  public List<ToxiproxyContainer> createProxyContainers(
      final Network network, List<String> clusterInstances, String proxyDomainNameSuffix) {
    ArrayList<ToxiproxyContainer> containers = new ArrayList<>();
    int instanceCount = 0;
    for (String hostEndpoint : clusterInstances) {
      containers.add(
          new ToxiproxyContainer(TOXIPROXY_IMAGE)
              .withNetwork(network)
              .withNetworkAliases(
                  "toxiproxy-instance-" + (++instanceCount), hostEndpoint + proxyDomainNameSuffix));
    }
    return containers;
  }

  public ToxiproxyContainer createProxyContainer(
      final Network network, String hostEndpoint, String proxyDomainNameSuffix) {
    return new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "toxiproxy-instance", hostEndpoint + proxyDomainNameSuffix);
  }

  // return db cluster instance proxy port
  public int createInstanceProxies(
      List<String> clusterInstances, List<ToxiproxyContainer> containers, int port) {
    Set<Integer> proxyPorts = new HashSet<>();

    for (int i = 0; i < clusterInstances.size(); i++) {
      String instanceEndpoint = clusterInstances.get(i);
      ToxiproxyContainer container = containers.get(i);
      ToxiproxyContainer.ContainerProxy proxy = container.getProxy(instanceEndpoint, port);
      proxyPorts.add(proxy.getOriginalProxyPort());
    }
    assertEquals(1, proxyPorts.size(), "DB cluster proxies should be on the same port.");
    return proxyPorts.stream().findFirst().orElse(0);
  }

  public int createInstanceProxy(String hostEndpoint, ToxiproxyContainer container, int port) {
    ToxiproxyContainer.ContainerProxy proxy = container.getProxy(hostEndpoint, port);
    return proxy.getOriginalProxyPort();
  }

  // It works for Linux containers only!
  public int runInContainer(String cmd) {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command("sh", "-c", cmd);

    try {

      Process process = processBuilder.start();
      StringBuilder output = new StringBuilder();
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }

      int exitVal = process.waitFor();
      if (exitVal == 0) {
        // System.out.println(output);
      } else {
        // abnormal...
        System.err.println(output);
        System.err.println("Failed to execute: " + cmd);
      }
      return exitVal;

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    return 1;
  }

  /**
   * Stops all traffic to and from server.
   */
  public void disableConnectivity(Proxy proxy) throws IOException {
    proxy
        .toxics()
        .bandwidth(
            "DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from database server towards driver
    proxy
        .toxics()
        .bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from driver towards database server
  }

  /**
   * Allow traffic to and from server.
   */
  public void enableConnectivity(Proxy proxy) {
    try {
      proxy.toxics().get("DOWN-STREAM").remove();
    } catch (IOException ex) {
      // ignore
    }

    try {
      proxy.toxics().get("UP-STREAM").remove();
    } catch (IOException ex) {
      // ignore
    }
  }
}
