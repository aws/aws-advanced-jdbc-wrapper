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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * Creates and destroys AWS RDS Clusters and Instances. To use this functionality the following
 * environment variables must be defined: - AWS_ACCESS_KEY_ID - AWS_SECRET_ACCESS_KEY
 */
public class AuroraTestUtility {
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

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  /**
   * Initializes an AmazonRDS & AmazonEC2 client. RDS client used to create/destroy clusters &
   * instances. EC2 client used to add/remove IP from security group.
   */
  public AuroraTestUtility() {
    this(Region.US_EAST_1, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *     https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public AuroraTestUtility(Region region) {
    this(region, DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *     https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   */
  public AuroraTestUtility(String region) {
    this(getRegionInternal(region), DefaultCredentialsProvider.create());
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region define AWS Regions, refer to
   *     https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
   * @param credentials Specific AWS credential provider
   */
  public AuroraTestUtility(Region region, DefaultCredentialsProvider credentials) {
    dbRegion = region;

    rdsClient = RdsClient.builder().region(dbRegion).credentialsProvider(credentials).build();

    ec2Client = Ec2Client.builder().region(dbRegion).credentialsProvider(credentials).build();
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
    throw new IllegalArgumentException(
        Messages.get(
            "IamAuthConnectionPlugin.unsupportedRegion",
            new String[] {rdsRegion}));
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for
   * databases.
   *
   * @param username Master username for access to database
   * @param password Master password for access to database
   * @param name Database name
   * @param identifier Database cluster identifier
   * @param engine Database engine to use, refer to
   *     https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html
   * @param instanceClass instance class, refer to
   *     https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
   * @param instances number of instances to spin up
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(
      String username,
      String password,
      String name,
      String identifier,
      String engine,
      String instanceClass,
      int instances)
      throws InterruptedException {
    dbUsername = username;
    dbPassword = password;
    dbName = name;
    dbIdentifier = identifier;
    dbEngine = engine;
    dbInstanceClass = instanceClass;
    numOfInstances = instances;
    return createCluster();
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for
   * databases.
   *
   * @param username Master username for access to database
   * @param password Master password for access to database
   * @param name Database name
   * @param identifier Database identifier
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(String username, String password, String name, String identifier)
      throws InterruptedException {
    dbUsername = username;
    dbPassword = password;
    dbName = name;
    dbIdentifier = identifier;
    return createCluster();
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for
   * databases.
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
      rdsClient.createDBInstance(
          CreateDbInstanceRequest.builder()
              .dbClusterIdentifier(dbIdentifier)
              .dbInstanceIdentifier(dbIdentifier + "-" + i)
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
    return endpoint.substring(endpoint.indexOf('.') + 1);
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

  /** Authorizes IP to EC2 Security groups for RDS access. */
  public void ec2AuthorizeIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
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

  /** De-authorizes IP from EC2 Security groups. */
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

  /** Destroys all instances and clusters. Removes IP from EC2 whitelist. */
  public void deleteCluster() {
    // Tear down instances
    for (int i = 1; i <= numOfInstances; i++) {
      rdsClient.deleteDBInstance(
          DeleteDbInstanceRequest.builder()
              .dbInstanceIdentifier(dbIdentifier + "-" + i)
              .skipFinalSnapshot(true)
              .build());
    }

    // Tear down cluster
    rdsClient.deleteDBCluster(
        (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
  }
}
