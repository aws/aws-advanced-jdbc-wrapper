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

package integration.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.DriverHelper;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestEnvironment;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterResponse;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.FailoverDbClusterResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.RebootDbClusterResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

/**
 * Creates and destroys AWS RDS Clusters and Instances. To use this functionality the following environment variables
 * must be defined: - AWS_ACCESS_KEY_ID - AWS_SECRET_ACCESS_KEY
 */
public class AuroraTestUtility {

  private static final Logger LOGGER = Logger.getLogger(AuroraTestUtility.class.getName());

  // Default values
  private String dbUsername = "my_test_username";
  private String dbPassword = "my_test_password";
  private String dbName = "test";
  private String dbIdentifier = "test-identifier";
  private DatabaseEngineDeployment dbEngineDeployment;
  private String dbEngine = "aurora-postgresql";
  private String dbEngineVersion = "13.9";
  private String dbInstanceClass = "db.r5.large";
  private String storageType = "io1";
  private int allocatedStorage = 100;
  private int iops = 1000;
  private final Region dbRegion;
  private final String dbSecGroup = "default";
  private int numOfInstances = 5;
  private ArrayList<TestInstanceInfo> instances = new ArrayList<>();

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;
  private static final Random rand = new Random();

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  public AuroraTestUtility(String region, String endpoint) throws URISyntaxException {
    this(getRegionInternal(region), endpoint, DefaultCredentialsProvider.create());
  }

  public AuroraTestUtility(
      String region, String rdsEndpoint, String awsAccessKeyId, String awsSecretAccessKey, String awsSessionToken)
      throws URISyntaxException {

    this(
        getRegionInternal(region),
        rdsEndpoint,
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region              define AWS Regions, refer to
   *                            <a
   *                            href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html">Regions,
   *                            Availability Zones, and Local Zones</a>
   * @param credentialsProvider Specific AWS credential provider
   */
  public AuroraTestUtility(Region region, String rdsEndpoint, AwsCredentialsProvider credentialsProvider)
      throws URISyntaxException {
    dbRegion = region;
    final RdsClientBuilder rdsClientBuilder = RdsClient.builder()
        .region(dbRegion)
        .credentialsProvider(credentialsProvider);

    if (!StringUtils.isNullOrEmpty(rdsEndpoint)) {
      rdsClientBuilder.endpointOverride(new URI(rdsEndpoint));
    }

    rdsClient = rdsClientBuilder.build();
    ec2Client = Ec2Client.builder()
        .region(dbRegion)
        .credentialsProvider(credentialsProvider)
        .build();
  }

  protected static Region getRegionInternal(String rdsRegion) {
    Optional<Region> regionOptional =
        Region.regions().stream().filter(r -> r.id().equalsIgnoreCase(rdsRegion)).findFirst();

    if (regionOptional.isPresent()) {
      return regionOptional.get();
    }
    throw new IllegalArgumentException(String.format("Unknown AWS region '%s'.", rdsRegion));
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @param username      Master username for access to database
   * @param password      Master password for access to database
   * @param dbName        Database name
   * @param identifier    Database cluster identifier
   * @param engine        Database engine to use, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html
   * @param instanceClass instance class, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
   * @param version       the database engine's version
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(
      String username,
      String password,
      String dbName,
      String identifier,
      DatabaseEngineDeployment deployment,
      String engine,
      String instanceClass,
      String version,
      int numOfInstances,
      ArrayList<TestInstanceInfo> instances)
      throws InterruptedException {
    this.dbUsername = username;
    this.dbPassword = password;
    this.dbName = dbName;
    this.dbIdentifier = identifier;
    this.dbEngineDeployment = deployment;
    this.dbEngine = engine;
    this.dbInstanceClass = instanceClass;
    this.dbEngineVersion = version;
    this.numOfInstances = numOfInstances;
    this.instances = instances;

    switch (this.dbEngineDeployment) {
      case AURORA:
        return createAuroraCluster();
      case RDS_MULTI_AZ:
        return createMultiAzCluster();
      default:
        throw new UnsupportedOperationException(this.dbEngineDeployment.toString());
    }
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createAuroraCluster() throws InterruptedException {
    // Create Cluster
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();

    final CreateDbClusterRequest dbClusterRequest =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .databaseName(dbName)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .sourceRegion(dbRegion.id())
            .enableIAMDatabaseAuthentication(true)
            .engine(dbEngine)
            .engineVersion(dbEngineVersion)
            .storageEncrypted(true)
            .tags(testRunnerTag)
            .build();

    rdsClient.createDBCluster(dbClusterRequest);

    // Create Instances
    for (int i = 1; i <= numOfInstances; i++) {
      final String instanceName = dbIdentifier + "-" + i;
      rdsClient.createDBInstance(
          CreateDbInstanceRequest.builder()
              .dbClusterIdentifier(dbIdentifier)
              .dbInstanceIdentifier(instanceName)
              .dbInstanceClass(dbInstanceClass)
              .engine(dbEngine)
              .engineVersion(dbEngineVersion)
              .publiclyAccessible(true)
              .tags(testRunnerTag)
              .build());
    }

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));
    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String clusterDomainPrefix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainPrefix;
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createMultiAzCluster() throws InterruptedException {
    // Create Cluster
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();
    CreateDbClusterRequest.Builder clusterBuilder =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .publiclyAccessible(true)
            .databaseName(dbName)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .sourceRegion(dbRegion.id())
            .engine(dbEngine)
            .engineVersion(dbEngineVersion)
            .enablePerformanceInsights(false)
            .backupRetentionPeriod(1)
            .storageEncrypted(true)
            .tags(testRunnerTag);

    clusterBuilder =
        clusterBuilder.allocatedStorage(allocatedStorage)
            .dbClusterInstanceClass(dbInstanceClass)
            .storageType(storageType)
            .iops(iops);

    rdsClient.createDBCluster(clusterBuilder.build());

    // For multi-AZ deployments, the cluster instances are created automatically.

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));
    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String clusterDomainPrefix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainPrefix;
  }

  /**
   * Creates an RDS instance under the current cluster and waits until it is up.
   *
   * @param instanceId the desired instance ID of the new instance
   * @return the instance info of the new instance
   * @throws InterruptedException if the new instance is not available within 5 minutes
   */
  public TestInstanceInfo createInstance(String instanceId) throws InterruptedException {
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();
    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();

    rdsClient.createDBInstance(
        CreateDbInstanceRequest.builder()
            .dbClusterIdentifier(info.getAuroraClusterName())
            .dbInstanceIdentifier(instanceId)
            .dbInstanceClass(dbInstanceClass)
            .engine(info.getDatabaseEngine())
            .engineVersion(info.getDatabaseEngineVersion())
            .publiclyAccessible(true)
            .tags(testRunnerTag)
            .build());

    // Wait for the instance to become available
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-instance-id").values(instanceId).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(15)));

    if (waiterResponse.matched().exception().isPresent()) {
      throw new InterruptedException(
          "Instance creation timeout for " + instanceId
              + ". The instance was not available within 5 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-instance-id").values(instanceId).build()));

    if (dbInstancesResult.dbInstances().size() != 1) {
      throw new RuntimeException(
          "The describeDBInstances request for newly created instance " + instanceId
              + " returned an unexpected number of instances: "
              + dbInstancesResult.dbInstances().size());
    }

    DBInstance instance = dbInstancesResult.dbInstances().get(0);
    TestInstanceInfo instanceInfo = new TestInstanceInfo(
        instance.dbInstanceIdentifier(),
        instance.endpoint().address(),
        instance.endpoint().port());
    this.instances.add(instanceInfo);
    return instanceInfo;
  }

  /**
   * Deletes an RDS instance.
   *
   * @param instanceToDelete the info for the instance to delete
   * @throws InterruptedException if the instance has not been deleted within 5 minutes
   */
  public void deleteInstance(TestInstanceInfo instanceToDelete) throws InterruptedException {
    rdsClient.deleteDBInstance(
        DeleteDbInstanceRequest.builder()
            .dbInstanceIdentifier(instanceToDelete.getInstanceId())
            .skipFinalSnapshot(true)
            .build());
    this.instances.remove(instanceToDelete);

    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse = waiter.waitUntilDBInstanceDeleted(
        (requestBuilder) -> requestBuilder.filters(
            Filter.builder().name("db-instance-id").values(instanceToDelete.getInstanceId())
                .build()),
        (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(15)));

    if (waiterResponse.matched().exception().isPresent()) {
      throw new InterruptedException(
          "Instance deletion timeout for " + instanceToDelete.getInstanceId()
              + ". The instance was not deleted within 5 minutes");
    }
  }

  /**
   * Gets public IP.
   *
   * @return public IP of user
   * @throws UnknownHostException when checkip host isn't available
   */
  public String getPublicIPAddress() throws UnknownHostException {
    String ip;
    try {
      URL ipChecker = new URL("http://checkip.amazonaws.com");
      BufferedReader reader = new BufferedReader(new InputStreamReader(ipChecker.openStream()));
      ip = reader.readLine();
    } catch (Exception e) {
      throw new UnknownHostException("Unable to get IP");
    }
    return ip;
  }

  /**
   * Authorizes IP to EC2 Security groups for RDS access.
   */
  public void ec2AuthorizeIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }

    if (ipExists(ipAddress)) {
      return;
    }

    try {
      ec2Client.authorizeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .ipPermissions((permissionBuilder) ->
                      permissionBuilder.ipRanges((ipRangeBuilder) ->
                          ipRangeBuilder
                              .cidrIp(ipAddress + "/32")
                              .description("Test run at " + Instant.now()))
                          .ipProtocol("-1") // All protocols
                          .fromPort(0) // For all ports
                          .toPort(65535)));
    } catch (Ec2Exception exception) {
      if (!DUPLICATE_IP_ERROR_CODE.equalsIgnoreCase(exception.awsErrorDetails().errorCode())) {
        throw exception;
      }
    }
  }

  private boolean ipExists(String ipAddress) {
    final DescribeSecurityGroupsResponse response =
        ec2Client.describeSecurityGroups(
            (builder) ->
                builder
                    .groupNames(dbSecGroup)
                    .filters(
                        software.amazon.awssdk.services.ec2.model.Filter.builder()
                            .name("ip-permission.cidr")
                            .values(ipAddress + "/32")
                            .build()));

    return response != null && !response.securityGroups().isEmpty();
  }

  /**
   * De-authorizes IP from EC2 Security groups.
   */
  public void ec2DeauthorizesIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }
    try {
      ec2Client.revokeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .cidrIp(ipAddress + "/32")
                  .ipProtocol("-1") // All protocols
                  .fromPort(0) // For all ports
                  .toPort(65535));
    } catch (Ec2Exception exception) {
      // Ignore
    }
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   *
   * @param identifier database identifier to delete
   */
  public void deleteCluster(String identifier) {
    dbIdentifier = identifier;
    deleteCluster();
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   *
   */
  public void deleteCluster() {

    switch (this.dbEngineDeployment) {
      case AURORA:
        this.deleteAuroraCluster();
        break;
      case RDS_MULTI_AZ:
        this.deleteMultiAzCluster();
        break;
      default:
        throw new UnsupportedOperationException(this.dbEngineDeployment.toString());
    }
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   */
  public void deleteAuroraCluster() {
    // Tear down instances
    for (int i = 1; i <= numOfInstances; i++) {
      try {
        rdsClient.deleteDBInstance(
            DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(dbIdentifier + "-" + i)
                .skipFinalSnapshot(true)
                .build());
      } catch (Exception ex) {
        LOGGER.finest("Error deleting instance " + dbIdentifier + "-" + i + ". " + ex.getMessage());
        // Ignore this error and continue with other instances
      }
    }

    // Tear down cluster
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbClusterResponse response = rdsClient.deleteDBCluster(
            (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (DbClusterNotFoundException ex) {
        // ignore
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db cluster " + dbIdentifier + ": " + ex);
      }
    }
  }

  /**
   * Destroys all instances and clusters.
   */
  public void deleteMultiAzCluster() {
    // deleteDBinstance requests are not necessary to delete a multi-az cluster.
    // Tear down cluster
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbClusterResponse response = rdsClient.deleteDBCluster(
            (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (DbClusterNotFoundException ex) {
        // ignore
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db cluster " + dbIdentifier + ": " + ex);
      }
    }
  }

  public boolean doesClusterExist(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    try {
      rdsClient.describeDBClusters(request);
    } catch (DbClusterNotFoundException ex) {
      return false;
    }
    return true;
  }

  public DBCluster getClusterInfo(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      throw new RuntimeException("Cluster " + clusterId + " not found.");
    }

    return response.dbClusters().get(0);
  }

  public DatabaseEngine getClusterEngine(final DBCluster cluster) {
    switch (cluster.engine()) {
      case "aurora-postgresql":
      case "postgres":
        return DatabaseEngine.PG;
      case "aurora-mysql":
      case "mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(cluster.engine());
    }
  }

  public List<TestInstanceInfo> getClusterInstanceIds(final String clusterId) {
    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(Filter.builder().name("db-cluster-id").values(clusterId).build()));

    List<TestInstanceInfo> result = new ArrayList<>();
    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      result.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }
    return result;
  }

  public void waitUntilClusterHasRightState(String clusterId) throws InterruptedException {
    waitUntilClusterHasRightState(clusterId, "available");
  }

  public void waitUntilClusterHasRightState(String clusterId, String allowedStatus) throws InterruptedException {
    String status = getDBCluster(clusterId).status();
    LOGGER.finest("Cluster status: " + status + ", waiting for status: " + allowedStatus);
    final Set<String> allowedStatuses = new HashSet<>(Arrays.asList(allowedStatus.toLowerCase()));
    final long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
    while (!allowedStatuses.contains(status.toLowerCase()) && waitTillNanoTime > System.nanoTime()) {
      TimeUnit.MILLISECONDS.sleep(1000);
      String tmpStatus = getDBCluster(clusterId).status();
      if (!tmpStatus.equalsIgnoreCase(status)) {
        LOGGER.finest("Cluster status (waiting): " + tmpStatus);
      }
      status = tmpStatus;
    }
    LOGGER.finest("Cluster status (after wait): " + status);
  }

  public DBCluster getDBCluster(String clusterId) {
    final DescribeDbClustersResponse dbClustersResult =
        rdsClient.describeDBClusters((builder) -> builder.dbClusterIdentifier(clusterId));
    final List<DBCluster> dbClusterList = dbClustersResult.dbClusters();
    return dbClusterList.get(0);
  }

  public List<String> getAuroraInstanceIds() throws SQLException {
    return getAuroraInstanceIds(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
        ConnectionStringHelper.getUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  // The first instance in topology should be a writer!
  public List<String> getAuroraInstanceIds(
      DatabaseEngine databaseEngine,
      DatabaseEngineDeployment deployment,
      String connectionUrl,
      String userName,
      String password)
      throws SQLException {

    String retrieveTopologySql;
    switch (deployment) {
      case AURORA:
        switch (databaseEngine) {
          case MYSQL:
            retrieveTopologySql =
                "SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status "
                    + "ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)";
            break;
          case PG:
            retrieveTopologySql =
                "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
                    + "ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END";
            break;
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
        break;
      case RDS_MULTI_AZ:
        switch (databaseEngine) {
          case MYSQL:

            final String replicaWriterId = getMultiAzMysqlReplicaWriterInstanceId(connectionUrl, userName, password);
            retrieveTopologySql =
                "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID FROM mysql.rds_topology"
                + " ORDER BY CASE WHEN id = "
                + (replicaWriterId == null ? "@@server_id" : String.format("'%s'", replicaWriterId))
                + " THEN 0 ELSE 1 END, SUBSTRING_INDEX(endpoint, '.', 1)";
            break;
          case PG:
            retrieveTopologySql =
                "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID"
                + " FROM rds_tools.show_topology()"
                + " ORDER BY CASE WHEN id ="
                + " (SELECT MAX(multi_az_db_cluster_source_dbi_resource_id) FROM"
                + " rds_tools.multi_az_db_cluster_source_dbi_resource_id())"
                  + " THEN 0 ELSE 1 END, endpoint";

            break;
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
        break;
      default:
        throw new UnsupportedOperationException(deployment.toString());
    }

    ArrayList<String> auroraInstances = new ArrayList<>();

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
        final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery(retrieveTopologySql)) {
      while (resultSet.next()) {
        // Get Instance endpoints
        final String hostEndpoint = resultSet.getString("SERVER_ID");
        auroraInstances.add(hostEndpoint);
      }
    }
    return auroraInstances;
  }

  private String getMultiAzMysqlReplicaWriterInstanceId(
      String connectionUrl,
      String userName,
      String password)
      throws SQLException {

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
        final Statement stmt = conn.createStatement();
        final ResultSet resultSet = stmt.executeQuery("SHOW REPLICA STATUS")) {
      if (resultSet.next()) {
        final String writerId = resultSet.getString("Source_Server_id"); // like '1034958454'
        return writerId;
      }
      return null;
    }

  }

  public Boolean isDBInstanceWriter(String instanceId) {
    return isDBInstanceWriter(
        TestEnvironment.getCurrent().getInfo().getAuroraClusterName(), instanceId);
  }

  public Boolean isDBInstanceWriter(String clusterId, String instanceId) {
    return getMatchedDBClusterMember(clusterId, instanceId).isClusterWriter();
  }

  public DBClusterMember getMatchedDBClusterMember(String clusterId, String instanceId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList(clusterId).stream()
            .filter(dbClusterMember -> dbClusterMember.dbInstanceIdentifier().equals(instanceId))
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(
          "Cannot find cluster member whose db instance identifier is " + instanceId);
    }
    return matchedMemberList.get(0);
  }

  public List<DBClusterMember> getDBClusterMemberList(String clusterId) {
    final DBCluster dbCluster = getDBCluster(clusterId);
    return dbCluster.dbClusterMembers();
  }

  public boolean makeSureInstancesUp(List<String> instances) throws InterruptedException {
    return makeSureInstancesUp(instances, true, TimeUnit.MINUTES.toSeconds(5));
  }

  public boolean makeSureInstancesUp(List<String> instances, long timeoutSec) throws InterruptedException {
    return makeSureInstancesUp(instances, true, TimeUnit.MINUTES.toSeconds(5));
  }

  public boolean makeSureInstancesUp(List<String> instances, boolean finalCheck, long timeoutSec)
      throws InterruptedException {
    final ConcurrentHashMap<String, Boolean> remainingInstances = new ConcurrentHashMap<>();
    instances.forEach((k) -> remainingInstances.put(k, true));

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setConnectTimeout(props, 30, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 30, TimeUnit.SECONDS);
    final ExecutorService executorService = Executors.newFixedThreadPool(instances.size());
    final CountDownLatch latch = new CountDownLatch(instances.size());
    final AtomicBoolean stop = new AtomicBoolean(false);

    for (final String id : instances) {
      executorService.submit(() -> {
        while (!stop.get()) {
          TestInstanceInfo instanceInfo =
              TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(id);
          String url = ConnectionStringHelper.getUrl(
              instanceInfo.getHost(),
              instanceInfo.getPort(),
              TestEnvironment.getCurrent()
                  .getInfo()
                  .getDatabaseInfo()
                  .getDefaultDbName());
          try (final Connection conn = DriverManager.getConnection(url, props)) {
            LOGGER.finest("Instance " + id + " is up.");
            remainingInstances.remove(id);
            latch.countDown();
            break;
          } catch (final SQLException ex) {
            // Continue waiting until instance is up.
            LOGGER.log(Level.FINEST, "Exception while trying to connect to instance " + id, ex);
          } catch (final Exception ex) {
            LOGGER.log(Level.SEVERE, "Exception:", ex);
            break;
          }
          try {
            TimeUnit.MILLISECONDS.sleep(5000);
          } catch (InterruptedException e) {
            break;
          }
        }
      });
    }

    latch.await(timeoutSec, TimeUnit.SECONDS);
    stop.set(true);
    executorService.shutdownNow();

    if (finalCheck) {
      assertTrue(
          remainingInstances.isEmpty(),
          "The following instances are still down: \n"
              + String.join("\n", remainingInstances.keySet()));
    } else {
      if (!remainingInstances.isEmpty()) {
        LOGGER.finest("The following instances are still down: \n"
            + String.join("\n", remainingInstances.keySet()));
      }
    }
    return remainingInstances.isEmpty();
  }

  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.
  public void assertFirstQueryThrows(Connection connection, Class expectedSQLExceptionClass) {
    assertThrows(
        expectedSQLExceptionClass,
        () -> {
          String instanceId = queryInstanceId(
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
              TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
              connection);
          LOGGER.finest(() -> "Instance ID: " + instanceId);
        });
  }

  public void failoverClusterAndWaitUntilWriterChanged() throws InterruptedException {
    String clusterId = TestEnvironment.getCurrent().getInfo().getAuroraClusterName();
    failoverClusterToATargetAndWaitUntilWriterChanged(
        clusterId,
        getDBClusterWriterInstanceId(clusterId),
        getRandomDBClusterReaderInstanceId(clusterId));
  }

  public void failoverClusterToATargetAndWaitUntilWriterChanged(
      String initialWriterId, String targetWriterId) throws InterruptedException {
    failoverClusterToATargetAndWaitUntilWriterChanged(
        TestEnvironment.getCurrent().getInfo().getAuroraClusterName(),
        initialWriterId,
        targetWriterId);
  }

  public void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterId, String initialWriterId, String targetWriterId)
      throws InterruptedException {

    DatabaseEngineDeployment deployment =
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment();
    if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ) {
      LOGGER.finest(String.format("failover from: %s", initialWriterId));
    } else {
      LOGGER.finest(String.format("failover from %s to target: %s", initialWriterId, targetWriterId));
    }
    final TestDatabaseInfo dbInfo = TestEnvironment.getCurrent().getInfo().getDatabaseInfo();
    final String clusterEndpoint = dbInfo.getClusterEndpoint();

    failoverClusterToTarget(
        clusterId,
        // TAZ cluster doesn't support target node
        deployment != DatabaseEngineDeployment.RDS_MULTI_AZ ? targetWriterId : null);

    String clusterIp = hostToIP(clusterEndpoint);

    // Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the correct writer instance.
    if (deployment == DatabaseEngineDeployment.AURORA) {
      LOGGER.finest("Cluster endpoint resolves to: " + clusterIp);
      String newClusterIp = hostToIP(clusterEndpoint);
      long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
      while (clusterIp.equals(newClusterIp) && waitTillNanoTime > System.nanoTime()) {
        TimeUnit.SECONDS.sleep(1);
        clusterIp = hostToIP(clusterEndpoint);
      }
      LOGGER.finest("Cluster endpoint resolves to (after wait): " + newClusterIp);

      // Wait for initial writer instance to be verified as not writer.
      waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
      while (isDBInstanceWriter(initialWriterId) && waitTillNanoTime > System.nanoTime()) {
        TimeUnit.SECONDS.sleep(1);
      }

    } else if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ) {
      // check cluster status from active -> failing over
      waitUntilClusterHasRightState(clusterId, "failing-over");
      // check cluster status from failing over -> active
      waitUntilClusterHasRightState(clusterId, "available");

      // We don't know what is the new writer node since targetWriterId is ignored by MultiAz cluster.
      // Waiting for clusterEndpoint changes IP address
      LOGGER.finest("Cluster endpoint resolves to: " + clusterIp);
      String newClusterEndpointIp = hostToIP(clusterEndpoint);
      final long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
      while (clusterIp.equals(newClusterEndpointIp) && waitTillNanoTime > System.nanoTime()) {
        TimeUnit.SECONDS.sleep(1);
        newClusterEndpointIp = hostToIP(clusterEndpoint);
      }
      LOGGER.finest("Cluster endpoint resolves to (after wait): " + newClusterEndpointIp);

      // wait until all instances except initial writer instance to be available
      makeSureInstancesUp(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances()
          .stream()
          .map(TestInstanceInfo::getInstanceId)
          .filter(x -> !x.equalsIgnoreCase(initialWriterId))
          .collect(Collectors.toList()));
    }
    LOGGER.finest(String.format("finished failover from %s to target: %s", initialWriterId, targetWriterId));
  }

  private boolean hasWriterChanged(String initialWriterId, long timeoutNanos)
      throws InterruptedException {
    final long waitUntil = System.nanoTime() + timeoutNanos;

    String currentWriterId = getDBClusterWriterInstanceId();
    while (initialWriterId.equals(currentWriterId)) {
      if (waitUntil < System.nanoTime()) {
        return false;
      }
      TimeUnit.MILLISECONDS.sleep(3000);
      // Calling the RDS API to get writer Id.
      currentWriterId = getDBClusterWriterInstanceId();
    }
    return true;
  }

  public void failoverClusterToTarget(String clusterId, @Nullable String targetInstanceId)
      throws InterruptedException {
    waitUntilClusterHasRightState(clusterId);

    int remainingAttempts = 10;
    while (--remainingAttempts > 0) {
      try {
        FailoverDbClusterResponse response = rdsClient.failoverDBCluster(
            (builder) -> {
              builder.dbClusterIdentifier(clusterId);
              if (!StringUtils.isNullOrEmpty(targetInstanceId)) {
                builder.targetDBInstanceIdentifier(targetInstanceId);
              }
            });
        if (!response.sdkHttpResponse().isSuccessful()) {
          LOGGER.finest(String.format("failoverDBCluster response: %d, %s",
              response.sdkHttpResponse().statusCode(),
              response.sdkHttpResponse().statusText()));
        } else {
          LOGGER.finest("failoverDBCluster request is sent");
          return;
        }
      } catch (final Exception e) {
        LOGGER.finest(String.format("failoverDBCluster request to %s failed: %s", targetInstanceId, e.getMessage()));
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
    throw new RuntimeException("Failed to request a cluster failover.");
  }

  public void rebootCluster(String clusterName) throws InterruptedException {
    int remainingAttempts = 10;
    while (--remainingAttempts > 0) {
      try {
        RebootDbClusterResponse response = rdsClient.rebootDBCluster(
            builder -> builder.dbClusterIdentifier(clusterName).build());
        if (!response.sdkHttpResponse().isSuccessful()) {
          LOGGER.finest(String.format("rebootDBCluster response: %d, %s",
              response.sdkHttpResponse().statusCode(),
              response.sdkHttpResponse().statusText()));
        } else {
          LOGGER.finest("rebootDBCluster request is sent");
          return;
        }
      } catch (final Exception e) {
        LOGGER.finest(String.format("rebootDBCluster '%s' cluster request failed: %s", clusterName, e.getMessage()));
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
    throw new RuntimeException("Failed to request a cluster reboot.");
  }

  public String hostToIP(String hostname) {
    return hostToIP(hostname, true);
  }

  public String hostToIP(String hostname, boolean fail) {
    try {
      final InetAddress inet = InetAddress.getByName(hostname);
      return inet.getHostAddress();
    } catch (UnknownHostException e) {
      if (fail) {
        fail("The IP address of host " + hostname + " could not be determined");
      }
      return null;
    }
  }

  public boolean waitDnsEqual(
      String hostToCheck, String expectedHostIpOrName, long timeoutSec, boolean fail)
      throws InterruptedException {

    RdsUtils rdsUtils = new RdsUtils();

    String hostIpAddress = this.hostToIP(hostToCheck, false);
    if (hostIpAddress == null) {

      long startTimeNano = System.nanoTime();
      while (hostIpAddress == null
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimeNano) < timeoutSec) {
        Thread.sleep(5000);
        hostIpAddress = this.hostToIP(hostToCheck, false);
      }
      if (hostIpAddress == null) {
        fail("Can't get IP address for " + hostToCheck);
      }
    }

    String expectedHostIpAddress = rdsUtils.isIPv4(expectedHostIpOrName) || rdsUtils.isIPv6(expectedHostIpOrName)
        ? expectedHostIpOrName
        : this.hostToIP(expectedHostIpOrName);

    LOGGER.finest(String.format("Wait for %s (current IP address %s) resolves to %s (IP address %s)",
        hostToCheck, hostIpAddress, expectedHostIpOrName, expectedHostIpAddress));

    long startTimeNano = System.nanoTime();
    while (!expectedHostIpAddress.equals(hostIpAddress)
        && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimeNano) < timeoutSec) {
      Thread.sleep(5000);
      hostIpAddress = this.hostToIP(hostToCheck, false);
      LOGGER.finest(String.format("%s resolves to %s", hostToCheck, hostIpAddress));
    }

    boolean result = expectedHostIpAddress.equals(hostIpAddress);
    if (fail) {
      assertTrue(result);
    }

    LOGGER.finest("Completed.");
    return result;
  }

  public boolean waitDnsNotEqual(
      String hostToCheck, String expectedNotToBeHostIpOrName, long timeoutSec, boolean fail)
      throws InterruptedException {

    RdsUtils rdsUtils = new RdsUtils();

    String hostIpAddress = this.hostToIP(hostToCheck, false);
    if (hostIpAddress == null) {

      long startTimeNano = System.nanoTime();
      while (hostIpAddress == null
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimeNano) < timeoutSec) {
        Thread.sleep(5000);
        hostIpAddress = this.hostToIP(hostToCheck, false);
      }
      if (hostIpAddress == null) {
        fail("Can't get IP address for " + hostToCheck);
      }
    }

    String expectedHostIpAddress =
        rdsUtils.isIPv4(expectedNotToBeHostIpOrName) || rdsUtils.isIPv6(expectedNotToBeHostIpOrName)
          ? expectedNotToBeHostIpOrName
          : this.hostToIP(expectedNotToBeHostIpOrName);

    LOGGER.finest(String.format("Wait for %s (current IP address %s) resolves to anything except %s (IP address %s)",
        hostToCheck, hostIpAddress, expectedNotToBeHostIpOrName, expectedHostIpAddress));

    long startTimeNano = System.nanoTime();
    while (expectedHostIpAddress.equals(hostIpAddress)
        && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimeNano) < timeoutSec) {
      Thread.sleep(5000);
      hostIpAddress = this.hostToIP(hostToCheck, false);
      LOGGER.finest(String.format("%s resolves to %s", hostToCheck, hostIpAddress));
    }

    boolean resultNotEqual = !expectedHostIpAddress.equals(hostIpAddress);
    if (fail) {
      assertTrue(resultNotEqual);
    }

    LOGGER.finest("Completed.");
    return resultNotEqual;
  }

  public String getRandomDBClusterReaderInstanceId(String clusterId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList(clusterId).stream()
            .filter(x -> !x.isClusterWriter())
            .collect(Collectors.toList());

    if (matchedMemberList.isEmpty()) {
      fail("Failed to failover cluster - a reader instance could not be found.");
    }

    return matchedMemberList.get(rand.nextInt(matchedMemberList.size())).dbInstanceIdentifier();
  }

  public String getDBClusterWriterInstanceId() {
    return getDBClusterWriterInstanceId(
        TestEnvironment.getCurrent().getInfo().getAuroraClusterName());
  }

  public String getDBClusterWriterInstanceId(String clusterId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList(clusterId).stream()
            .filter(DBClusterMember::isClusterWriter)
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(clusterId);
    }
    // Should be only one writer at index 0.
    return matchedMemberList.get(0).dbInstanceIdentifier();
  }

  protected String getInstanceIdSql(DatabaseEngine databaseEngine, DatabaseEngineDeployment deployment) {
    switch (deployment) {
      case AURORA:
        switch (databaseEngine) {
          case MYSQL:
            return "SELECT @@aurora_server_id as id";
          case PG:
            return "SELECT aurora_db_instance_identifier()";
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
      case RDS_MULTI_AZ:
        switch (databaseEngine) {
          case MYSQL:
            return "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as id FROM mysql.rds_topology WHERE id=@@server_id";
          case PG:
            return "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as id "
                    + "FROM rds_tools.show_topology() "
                    + "WHERE id IN (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())";
          default:
            throw new UnsupportedOperationException(databaseEngine.toString());
        }
      default:
        throw new UnsupportedOperationException(deployment.toString());
    }
  }

  public String queryInstanceId(Connection connection) throws SQLException {
    return queryInstanceId(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngineDeployment(),
        connection);
  }

  public String queryInstanceId(
      DatabaseEngine databaseEngine, DatabaseEngineDeployment deployment, Connection connection)
      throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      return executeInstanceIdQuery(databaseEngine, deployment, stmt);
    }
  }

  protected String executeInstanceIdQuery(
      DatabaseEngine databaseEngine, DatabaseEngineDeployment deployment, Statement stmt)
      throws SQLException {
    try (final ResultSet rs = stmt.executeQuery(getInstanceIdSql(databaseEngine, deployment))) {
      if (rs.next()) {
        return rs.getString(1);
      }
    }
    return null;
  }

  public void createUser(Connection conn, String username, String password) throws SQLException {
    DatabaseEngine engine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    String dropUserSql = getCreateUserSql(engine, username, password);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(dropUserSql);
    }
  }

  protected String getCreateUserSql(DatabaseEngine engine, String username, String password) {
    switch (engine) {
      case MYSQL:
        return "CREATE USER " + username + " identified by '" + password + "'";
      case PG:
        return "CREATE USER " + username + " with password '" + password + "'";
      default:
        throw new UnsupportedOperationException(engine.toString());
    }
  }

  public void addAuroraAwsIamUser(
      DatabaseEngine databaseEngine,
      String connectionUrl,
      String userName,
      String password,
      String dbUser,
      String databaseName)
      throws SQLException {

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
        final Statement stmt = conn.createStatement()) {

      switch (databaseEngine) {
        case MYSQL:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute(
              "CREATE USER " + dbUser + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';");
          stmt.execute("GRANT ALL PRIVILEGES ON " + databaseName + ".* TO '" + dbUser + "'@'%';");
          break;
        case PG:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute("CREATE USER " + dbUser + ";");
          stmt.execute("GRANT rds_iam TO " + dbUser + ";");
          stmt.execute("GRANT ALL PRIVILEGES ON DATABASE " + databaseName + " TO " + dbUser + ";");
          break;
        default:
          throw new UnsupportedOperationException(databaseEngine.toString());
      }
    }
  }

  public List<String> getEngineVersions(String engine) {
    final List<String> res = new ArrayList<>();
    final DescribeDbEngineVersionsResponse versions = rdsClient.describeDBEngineVersions(
        DescribeDbEngineVersionsRequest.builder().engine(engine).build()
    );
    for (DBEngineVersion version : versions.dbEngineVersions()) {
      res.add(version.engineVersion());
    }
    return res;
  }

  public String getLatestVersion(String engine) {
    return getEngineVersions(engine).stream()
        .sorted(Comparator.reverseOrder())
        .findFirst()
        .orElse(null);
  }

  public String getLTSVersion(String engine) {
    final DescribeDbEngineVersionsResponse versions = rdsClient.describeDBEngineVersions(
        DescribeDbEngineVersionsRequest.builder().defaultOnly(true).engine(engine).build()
    );
    if (!versions.dbEngineVersions().isEmpty()) {
      return versions.dbEngineVersions().get(0).engineVersion();
    }
    throw new RuntimeException("Failed to find LTS version");
  }

  public static AuroraTestUtility getUtility() {
    return getUtility(null);
  }

  public static AuroraTestUtility getUtility(TestEnvironmentInfo info) {
    if (info == null) {
      info = TestEnvironment.getCurrent().getInfo();
    }

    try {
      return new AuroraTestUtility(info.getRegion(), info.getRdsEndpoint());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
