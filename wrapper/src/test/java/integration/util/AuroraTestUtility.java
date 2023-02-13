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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import integration.refactored.DatabaseEngine;
import integration.refactored.DriverHelper;
import integration.refactored.TestInstanceInfo;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestEnvironment;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterMember;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;
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
  private String dbEngine = "aurora-postgresql";
  private String dbEngineVersion = "13.7";
  private String dbInstanceClass = "db.r5.large";
  private final Region dbRegion;
  private final String dbSecGroup = "default";
  private int numOfInstances = 5;
  private ArrayList<TestInstanceInfo> instances = new ArrayList<>();

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;
  private static final Random rand = new Random();

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  /**
   * Initializes an AmazonRDS & AmazonEC2 client. RDS client used to create/destroy clusters & instances. EC2 client
   * used to add/remove IP from security group.
   */
  public AuroraTestUtility() {
    this(Region.US_EAST_1, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *               https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public AuroraTestUtility(Region region) {
    this(region, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *               https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public AuroraTestUtility(String region) {
    this(getRegionInternal(region), DefaultCredentialsProvider.create());
  }

  public AuroraTestUtility(
      String region, String awsAccessKeyId, String awsSecretAccessKey, String awsSessionToken) {

    this(
        getRegionInternal(region),
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region              define AWS Regions, refer to
   *                            https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   * @param credentialsProvider Specific AWS credential provider
   */
  public AuroraTestUtility(Region region, AwsCredentialsProvider credentialsProvider) {
    dbRegion = region;

    rdsClient =
        RdsClient.builder().region(dbRegion).credentialsProvider(credentialsProvider).build();

    ec2Client =
        Ec2Client.builder().region(dbRegion).credentialsProvider(credentialsProvider).build();
  }

  public Region getRegion(String rdsRegion) {
    return getRegionInternal(rdsRegion);
  }

  protected static Region getRegionInternal(String rdsRegion) {
    Optional<Region> regionOptional =
        Region.regions().stream().filter(r -> r.id().equalsIgnoreCase(rdsRegion)).findFirst();

    if (regionOptional.isPresent()) {
      return regionOptional.get();
    }
    throw new IllegalArgumentException(String.format("Unknown AWS region '%s'.", rdsRegion));
  }

  public String createCluster(
      String username,
      String password,
      String name,
      String identifier,
      String engine,
      String instanceClass,
      String version)
      throws InterruptedException {
    return createCluster(
        username, password, name, identifier, engine, instanceClass, version, 5, new ArrayList<>());
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
      String engine,
      String instanceClass,
      String version,
      int numOfInstances,
      ArrayList<TestInstanceInfo> instances)
      throws InterruptedException {
    dbUsername = username;
    dbPassword = password;
    this.dbName = dbName;
    dbIdentifier = identifier;
    dbEngine = engine;
    dbInstanceClass = instanceClass;
    dbEngineVersion = version;
    this.numOfInstances = numOfInstances;
    this.instances = instances;
    return createCluster();
  }

  public String createCluster(String username, String password, String name, String identifier)
      throws InterruptedException {
    return createCluster(username, password, name, identifier, 5);
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @param username   Master username for access to database
   * @param password   Master password for access to database
   * @param name       Database name
   * @param identifier Database identifier
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(
      String username, String password, String name, String identifier, int numOfInstances)
      throws InterruptedException {
    dbUsername = username;
    dbPassword = password;
    dbName = name;
    dbIdentifier = identifier;
    this.numOfInstances = numOfInstances;
    this.instances = new ArrayList<>();
    return createCluster();
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster() throws InterruptedException {
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
              .dbClusterIdentifier(dbIdentifier)
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
      String zone = instance.availabilityZone();
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainPrefix;
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
                  .cidrIp(ipAddress + "/32")
                  .ipProtocol("-1") // All protocols
                  .fromPort(0) // For all ports
                  .toPort(65535));
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
   */
  public void deleteCluster() {
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
    rdsClient.deleteDBCluster(
        (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
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

  public DatabaseEngine getClusterDatabaseEngine(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      throw new RuntimeException("Cluster " + clusterId + " not found.");
    }

    switch (response.dbClusters().get(0).engine()) {
      case "aurora-postgresql":
        return DatabaseEngine.PG;
      case "aurora-mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(response.dbClusters().get(0).engine());
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
    String status = getDBCluster(clusterId).status();
    while (!"available".equalsIgnoreCase(status)) {
      TimeUnit.MILLISECONDS.sleep(1000);
      status = getDBCluster(clusterId).status();
    }
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
        ConnectionStringHelper.getUrl(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
  }

  public List<String> getAuroraInstanceIds(
      DatabaseEngine databaseEngine, String connectionUrl, String userName, String password)
      throws SQLException {

    String retrieveTopologySql;
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

  public Boolean isDBInstanceWriter(String instanceId) {
    return isDBInstanceWriter(
        TestEnvironment.getCurrent().getInfo().getAuroraClusterName(), instanceId);
  }

  public Boolean isDBInstanceWriter(String clusterId, String instanceId) {
    return getMatchedDBClusterMember(clusterId, instanceId).isClusterWriter();
  }

  public Boolean isDBInstanceReader(String clusterId, String instanceId) {
    return !getMatchedDBClusterMember(clusterId, instanceId).isClusterWriter();
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

  public void makeSureInstancesUp(List<String> instances) throws InterruptedException {
    makeSureInstancesUp(instances, true);
  }

  protected void makeSureInstancesUp(List<String> instances, boolean finalCheck)
      throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(instances.size());
    final ConcurrentHashMap<String, Boolean> remainingInstances = new ConcurrentHashMap<>();
    instances.forEach((k) -> remainingInstances.put(k, true));

    final Properties props = ConnectionStringHelper.getDefaultProperties();
    DriverHelper.setConnectTimeout(props, 5, TimeUnit.SECONDS);
    DriverHelper.setSocketTimeout(props, 5, TimeUnit.SECONDS);

    for (final String id : instances) {
      executorService.submit(
          () -> {
            while (true) {
              TestInstanceInfo instanceInfo =
                  TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstance(id);
              try (final Connection conn =
                  DriverManager.getConnection(
                      ConnectionStringHelper.getUrl(
                          instanceInfo.getEndpoint(),
                          instanceInfo.getEndpointPort(),
                          TestEnvironment.getCurrent()
                              .getInfo()
                              .getDatabaseInfo()
                              .getDefaultDbName()),
                      props)) {
                remainingInstances.remove(id);
                break;
              } catch (final SQLException ex) {
                ex.printStackTrace();
                // Continue waiting until instance is up.
              } catch (final Exception ex) {
                System.out.println("Exception: " + ex);
                break;
              }
              TimeUnit.MILLISECONDS.sleep(1000);
            }
            return null;
          });
    }
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.MINUTES);

    if (finalCheck) {
      assertTrue(
          remainingInstances.isEmpty(),
          "The following instances are still down: \n"
              + String.join("\n", remainingInstances.keySet()));
    }
  }

  public void failoverClusterAndWaitUntilWriterChanged() throws InterruptedException {
    LOGGER.finest("Inside failoverCluster method");
    String clusterId = TestEnvironment.getCurrent().getInfo().getAuroraClusterName();
    waitUntilClusterHasRightState(clusterId);
    failoverClusterToATargetAndWaitUntilWriterChanged(
        clusterId,
        getDBClusterWriterInstanceId(clusterId),
        getRandomDBClusterReaderInstanceId(clusterId));
  }

  public void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterWriterId, String targetInstanceId) throws InterruptedException {

    failoverClusterToATargetAndWaitUntilWriterChanged(
        TestEnvironment.getCurrent().getInfo().getAuroraClusterName(),
        clusterWriterId,
        targetInstanceId);
  }

  public void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterId, String clusterWriterId, String targetInstanceId)
      throws InterruptedException {
    LOGGER.finest(String.format("failover from %s to target: %s", clusterWriterId, targetInstanceId));
    final String clusterEndpoint = TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint();
    final String initialWriterIP = hostToIP(clusterEndpoint);
    String nextClusterWriterId = getDBClusterWriterInstanceId(clusterId);

    while (clusterWriterId.equals(nextClusterWriterId)) {
      failoverClusterWithATargetInstance(clusterId, targetInstanceId);
      TimeUnit.SECONDS.sleep(5);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId(clusterId);
    }

    // Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the correct writer instance.
    String nextWriterIP = hostToIP(clusterEndpoint);
    while (initialWriterIP.equals(nextWriterIP)) {
      TimeUnit.SECONDS.sleep(1);
      nextWriterIP = hostToIP(clusterEndpoint);
    }

    LOGGER.finest(String.format("finished failover from %s to target: %s", clusterWriterId, targetInstanceId));
  }

  public void failoverClusterWithATargetInstance(String clusterId, String targetInstanceId)
      throws InterruptedException {
    waitUntilClusterHasRightState(clusterId);

    while (true) {
      try {
        rdsClient.failoverDBCluster(
            (builder) ->
                builder
                    .dbClusterIdentifier(clusterId)
                    .targetDBInstanceIdentifier(targetInstanceId));
        break;
      } catch (final Exception e) {
        LOGGER.finest(String.format("failoverDBCluster request to %s failed: %s", targetInstanceId, e.getMessage()));
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
  }

  protected String hostToIP(String hostname) {
    try {
      final InetAddress inet = InetAddress.getByName(hostname);
      return inet.getHostAddress();
    } catch (UnknownHostException e) {
      fail("The IP address of host " + hostname + " could not be determined");
      return null;
    }
  }

  public String getRandomDBClusterReaderInstanceId(String clusterId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList(clusterId).stream()
            .filter(x -> !x.isClusterWriter())
            .collect(Collectors.toList());

    if (matchedMemberList.isEmpty()) {
      fail("Failed to failover cluster - a reader instance could not be found.");
    }

    return matchedMemberList.get(rand.nextInt(numOfInstances - 1)).dbInstanceIdentifier();
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

  protected String getInstanceIdSql(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return "SELECT @@aurora_server_id as id";
      case PG:
        return "SELECT aurora_db_instance_identifier()";
      default:
        throw new UnsupportedOperationException(databaseEngine.toString());
    }
  }

  public String queryInstanceId(Connection connection) throws SQLException {
    return queryInstanceId(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), connection);
  }

  public String queryInstanceId(DatabaseEngine databaseEngine, Connection connection)
      throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      return executeInstanceIdQuery(databaseEngine, stmt);
    }
  }

  public String executeInstanceIdQuery(DatabaseEngine databaseEngine, Statement stmt)
      throws SQLException {
    try (final ResultSet rs = stmt.executeQuery(getInstanceIdSql(databaseEngine))) {
      if (rs.next()) {
        return rs.getString(1);
      }
    }
    return null;
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
}
