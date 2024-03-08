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

  public boolean noDocker =
      Boolean.parseBoolean(System.getProperty("test-no-docker", "false"));
  public boolean noAurora =
      Boolean.parseBoolean(System.getProperty("test-no-aurora", "false"));
  public boolean noPerformance =
      Boolean.parseBoolean(System.getProperty("test-no-performance", "false"));
  public boolean noMysqlEngine =
      Boolean.parseBoolean(System.getProperty("test-no-mysql-engine", "false"));
  public boolean noMysqlDriver =
      Boolean.parseBoolean(System.getProperty("test-no-mysql-driver", "false"));
  public boolean noPgEngine =
      Boolean.parseBoolean(System.getProperty("test-no-pg-engine", "false"));
  public boolean noPgDriver =
      Boolean.parseBoolean(System.getProperty("test-no-pg-driver", "false"));
  public boolean noMariadbEngine =
      Boolean.parseBoolean(System.getProperty("test-no-mariadb-engine", "false"));
  public boolean noMariadbDriver =
      Boolean.parseBoolean(System.getProperty("test-no-mariadb-driver", "false"));
  public boolean noFailover =
      Boolean.parseBoolean(System.getProperty("test-no-failover", "false"));
  public boolean noIam =
      Boolean.parseBoolean(System.getProperty("test-no-iam", "false"));
  public boolean noSecretsManager =
      Boolean.parseBoolean(System.getProperty("test-no-secrets-manager", "false"));
  public boolean noHikari =
      Boolean.parseBoolean(System.getProperty("test-no-hikari", "false"));
  public boolean noGraalVm =
      Boolean.parseBoolean(System.getProperty("test-no-graalvm", "false"));
  public boolean noOpenJdk =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk", "false"));
  public boolean noOpenJdk8 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk8", "false"));
  public boolean noOpenJdk11 =
      Boolean.parseBoolean(System.getProperty("test-no-openjdk11", "false"));
  public boolean testHibernateOnly =
      Boolean.parseBoolean(System.getProperty("test-hibernate-only", "false"));
  public boolean testAutoscalingOnly =
      Boolean.parseBoolean(System.getProperty("test-autoscaling-only", "false"));

  public boolean noInstances1 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-1", "false"));
  public boolean noInstances2 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-2", "false"));
  public boolean noInstances5 =
      Boolean.parseBoolean(System.getProperty("test-no-instances-5", "false"));

  public boolean noTracesTelemetry =
      Boolean.parseBoolean(System.getProperty("test-no-traces-telemetry", "false"));
  public boolean noMetricsTelemetry =
      Boolean.parseBoolean(System.getProperty("test-no-metrics-telemetry", "false"));

  public String includeTags = System.getProperty("test-include-tags");
  public String excludeTags = System.getProperty("test-exclude-tags");

  public String auroraDbRegion = System.getenv("AURORA_DB_REGION");

  public String reuseAuroraCluster = System.getenv("REUSE_AURORA_CLUSTER");
  public String auroraClusterName = System.getenv("AURORA_CLUSTER_NAME"); // "cluster-mysql"
  public String auroraClusterDomain =
      System.getenv("AURORA_CLUSTER_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"

  // Expected values: "latest", "lts", or engine version, for example, "15.4"
  // If left as empty, will use LTS version
  public String auroraMySqlDbEngineVersion =
      System.getenv("AURORA_MYSQL_DB_ENGINE_VERSION");
  public String auroraPgDbEngineVersion =
      System.getenv("AURORA_PG_ENGINE_VERSION");

  public String dbName = System.getenv("DB_DATABASE_NAME");
  public String dbUsername = System.getenv("DB_USERNAME");
  public String dbPassword = System.getenv("DB_PASSWORD");

  public String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
  public String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
  public String awsSessionToken = System.getenv("AWS_SESSION_TOKEN");

  public String iamUser = System.getenv("IAM_USER");

}
