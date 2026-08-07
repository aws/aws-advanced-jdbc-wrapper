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

public class TestEnvironmentConfiguration {

  public final boolean noDocker =
      Boolean.parseBoolean(System.getProperty("test-no-docker", "false"));
  public final boolean noAurora =
      Boolean.parseBoolean(System.getProperty("test-no-aurora", "false"));
  public final boolean noMultiAzCluster =
      Boolean.parseBoolean(System.getProperty("test-no-multi-az-cluster", "false"));
  public final boolean noMultiAzInstance =
      Boolean.parseBoolean(System.getProperty("test-no-multi-az-instance", "false"));
  public final boolean noPerformance =
      Boolean.parseBoolean(System.getProperty("test-no-performance", "false"));
  public final boolean noMysqlEngine =
      Boolean.parseBoolean(System.getProperty("test-no-mysql-engine", "false"));
  public final boolean noMysqlDriver =
      Boolean.parseBoolean(System.getProperty("test-no-mysql-driver", "false"));
  public final boolean noPgEngine =
      Boolean.parseBoolean(System.getProperty("test-no-pg-engine", "false"));
  public final boolean noPgDriver =
      Boolean.parseBoolean(System.getProperty("test-no-pg-driver", "false"));
  public final boolean noMariadbEngine =
      Boolean.parseBoolean(System.getProperty("test-no-mariadb-engine", "false"));
  public final boolean noMariadbDriver =
      Boolean.parseBoolean(System.getProperty("test-no-mariadb-driver", "false"));
  public final boolean noFailover =
      Boolean.parseBoolean(System.getProperty("test-no-failover", "false"));
  public final boolean noIam =
      Boolean.parseBoolean(System.getProperty("test-no-iam", "false"));
  public final boolean noSecretsManager =
      Boolean.parseBoolean(System.getProperty("test-no-secrets-manager", "false"));
  public final boolean noHikari =
      Boolean.parseBoolean(System.getProperty("test-no-hikari", "false"));
  public final boolean noGraalVm =
      Boolean.parseBoolean(System.getProperty("test-no-graalvm", "false"));
  public final boolean noOpenJdk =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk", "false"));
  public final boolean noOpenJdk8 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk8", "false"));
  public final boolean noOpenJdk11 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk11", "false"));
  public final boolean noOpenJdk17 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk17", "false"));
  public final boolean noOpenJdk21 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk21", "false"));
  public final boolean noOpenJdk24 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk24", "false"));
  public final boolean testHibernateOnly =
      Boolean.parseBoolean(System.getProperty("test-hibernate-only", "false"));
  public final boolean testAutoscalingOnly =
      Boolean.parseBoolean(System.getProperty("test-autoscaling-only", "false"));
  public final boolean testEncryptionOnly =
      Boolean.parseBoolean(System.getProperty("test-encryption-only", "false"));
  public final boolean testMetricsOnly =
      Boolean.parseBoolean(System.getProperty("test-metrics-only", "false"));

  public final boolean noInstances1 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-1", "false"));
  public final boolean noInstances2 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-2", "false"));
  public final boolean noInstances3 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-3", "false"));
  public final boolean noInstances5 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-5", "false"));

  public final boolean noTracesTelemetry =
      Boolean.parseBoolean(System.getProperty("test-no-traces-telemetry", "false"));
  public final boolean noMetricsTelemetry =
      Boolean.parseBoolean(System.getProperty("test-no-metrics-telemetry", "false"));
  public final boolean noBlueGreen =
      Boolean.parseBoolean(System.getProperty("test-no-bg", "true"));
  public final boolean testBlueGreenOnly =
      Boolean.parseBoolean(System.getProperty("test-bg-only", "false"));
  public final boolean testValkeyCache =
      Boolean.parseBoolean(System.getProperty("test-valkey-cache", "false"));

  public final String includeTags = System.getProperty("test-include-tags");
  public final String excludeTags = System.getProperty("test-exclude-tags");

  /**
   * Splits the in-container integration test classes across several CI jobs so they can run in
   * parallel, each against its own database cluster. {@code shardIndex} is 1-based and must be in
   * the range {@code [1, shardCount]}. The default ({@code 1} of {@code 1}) runs every test class,
   * which is what a local run or any non-sharded workflow does.
   *
   * <p>Only test class selection is affected. Which environments (deployment, engine, instance
   * count) are exercised is still controlled by the {@code test-no-*} properties.
   */
  public final int shardIndex = Integer.parseInt(System.getProperty("test-shard-index", "1"));
  public final int shardCount = Integer.parseInt(System.getProperty("test-shard-count", "1"));

  public final String rdsDbRegion = System.getenv("RDS_DB_REGION");

  public final boolean reuseRdsDb = Boolean.parseBoolean(System.getenv("REUSE_RDS_DB"));

  // "cluster-mysql", "instance-name", "cluster-multi-az-name"
  public final String rdsDbName = System.getenv("RDS_DB_NAME");

  public final String rdsDbDomain =
      System.getenv("RDS_DB_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"

  public final String rdsEndpoint =
      System.getenv("RDS_ENDPOINT"); // "https://rds-int.amazon.com"

  // Expected values: "latest", "default", or engine version, for example, "15.4"
  // If left as empty, will use default version
  public final String mysqlVersion =
      System.getenv("MYSQL_VERSION");
  public final String pgVersion =
      System.getenv("PG_VERSION");

  public final String dbName = System.getenv("DB_DATABASE_NAME");
  public final String dbUsername = System.getenv("DB_USERNAME");
  public final String dbPassword = System.getenv("DB_PASSWORD");

  public final String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
  public final String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
  public final String awsSessionToken = System.getenv("AWS_SESSION_TOKEN");
  public final String kmsKeyId = System.getenv("KMS_KEY_ID");
  public final String iamUser = System.getenv("IAM_USER");

}
