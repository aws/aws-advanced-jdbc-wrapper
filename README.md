# Amazon Web Services (AWS) JDBC Driver
[![build_status](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/software.amazon.jdbc/aws-advanced-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Qodana](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml)

The **Amazon Web Services (AWS) JDBC Driver** has been redesigned as an advanced JDBC wrapper.

The wrapper is complementary to an existing JDBC driver and aims to extend the functionality of the driver to enable applications to take full advantage of the features of clustered databases such as Amazon Aurora. In other words, the AWS JDBC Driver does not connect directly to any database, but enables support of AWS and Aurora functionalities on top of an underlying JDBC driver of the user's choice.

The AWS JDBC Driver is targeted to work with **any** existing JDBC driver. Currently, the AWS JDBC Driver has been validated to support the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc), [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j), and [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j).

In conjunction with the JDBC Drivers for PostgreSQL, MySQL, and MariaDB, the AWS JDBC Driver enables functionalities from Amazon Aurora such as fast failover for PostgreSQL and MySQL Aurora clusters. It also introduces integration with AWS authentication services such as [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) and [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/).

## About the Wrapper
Hosting a database cluster in the cloud via Aurora is able to provide users with sets of features and configurations to obtain maximum performance and availability, such as database failover. However, at the moment, most existing drivers do not currently support those functionalities or are not able to entirely take advantage of it.

The main idea behind the AWS JDBC Driver is to add a software layer on top of an existing JDBC driver that would enable all the enhancements brought by Aurora, without requiring users to change their workflow with their databases and existing JDBC drivers.

### What is Failover?
In an Amazon Aurora database cluster, **failover** is a mechanism by which Aurora automatically repairs the cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver is designed to understand the situation and coordinate with the cluster in order to provide minimal downtime and allow connections to be very quickly restored in the event of a DB instance failure.

### Benefits of the AWS JDBC Driver
Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver allows customers to continue using their existing community drivers in addition to having the AWS JDBC Driver fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible.

### Enhanced Failure Monitoring
Since a database failover is usually identified by reaching a network or a connection timeout, the AWS JDBC Driver introduces an enhanced and customizable manner to faster identify a database outage.

Enhanced Failure Monitoring (EFM) is a feature available from the [Host Monitoring Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md#enhanced-failure-monitoring) that periodically checks the connected database node's health and availability. If a database node is determined to be unhealthy, the connection is aborted (and potentially routed to another healthy node in the cluster).

### Using the AWS JDBC Driver with RDS Multi-AZ DB Clusters
The [AWS RDS Multi-AZ DB Clusters](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) are capable of switching over the current writer node to another node in the cluster within approximately 1 second or less, in case of minor engine version upgrade or OS maintenance operations.
The AWS JDBC Driver has been optimized for such fast failover when working with AWS RDS Multi-AZ DB Clusters.

With the `failover` plugin, the downtime during certain DB cluster operations, such as engine minor version upgrades, can be reduced to one second or even less with finely tuned parameters. It supports both MySQL and PostgreSQL clusters.

Visit [this page](./docs/using-the-jdbc-driver/SupportForRDSMultiAzDBCluster.md) for more details.

### Using the AWS JDBC Driver with plain RDS databases
The AWS JDBC Driver also works with RDS provided databases that are not Aurora.

Please visit [this page](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#using-the-aws-jdbc-driver-with-plain-rds-databases) for more information.

## Getting Started
For more information on how to download the AWS JDBC Driver, minimum requirements to use it, 
and how to integrate it within your project and with your JDBC driver of choice, please visit the 
[Getting Started page](./docs/GettingStarted.md).

### Maven Central
You can find our driver by searching in The Central Repository with GroupId and ArtifactId [software.amazon:aws-advanced-jdbc-wrapper][mvn-search].

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
```xml
<!-- Add the following dependency to your pom.xml, -->
<!-- replacing LATEST with the specific version as required -->

<dependency>
  <groupId>software.amazon.jdbc</groupId>
  <artifactId>aws-advanced-jdbc-wrapper</artifactId>
  <version>LATEST</version>
</dependency>
```

[mvn-search]: https://search.maven.org/search?q=g:software.amazon.jdbc "Search on Maven Central"

## Properties

| Parameter                              |                                                   Reference                                                    |                                                      Documentation Link                                                       |
|----------------------------------------|:--------------------------------------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------:|
| `wrapperDialect`                       |                                            `DialectManager.DIALECT`                                            |                [Dialects](/docs/using-the-jdbc-driver/DatabaseDialects.md), and whether you should include it.                |
| `wrapperPlugins`                       |                                          `PropertyDefinition.PLUGINS`                                          |                                                                                                                               |
| `secretsManagerSecretId`               |                             `AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY`                             |             [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)             |
| `secretsManagerRegion`                 |                              `AwsSecretsManagerConnectionPlugin.REGION_PROPERTY`                               |             [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)             |
| `wrapperDriverName`                    |                              `DriverMetaDataConnectionPlugin.WRAPPER_DRIVER_NAME`                              |    [DriverMetaDataConnectionPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheDriverMetadataConnectionPlugin.md)     |
| `failoverMode`                         |                                    `FailoverConnectionPlugin.FAILOVER_MODE`                                    |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `clusterInstanceHostPattern`           |                             `AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN`                             |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `enableClusterAwareFailover`           |                            `FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER`                            |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `failoverClusterTopologyRefreshRateMs` |                      `FailoverConnectionPlugin.FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS`                      |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `failoverReaderConnectTimeoutMs`       |                         `FailoverConnectionPlugin.FAILOVER_READER_CONNECT_TIMEOUT_MS`                          |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `failoverTimeoutMs`                    |                                 `FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS`                                 |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `failoverWriterReconnectIntervalMs`    |                        `FailoverConnectionPlugin.FAILOVER_WRITER_RECONNECT_INTERVAL_MS`                        |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `clusterTopologyHighRefreshRateMs`     |                     `MonitoringRdsHostListProvider.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS`                      |                  [FailoverPlugin v2](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailover2Plugin.md)                   |
| `failoverReaderHostSelectorStrategy`   | `software.amazon.jdbc.plugin.failover2.`<br/>`FailoverConnectionPlugin.FAILOVER_READER_HOST_SELECTOR_STRATEGY` |                  [FailoverPlugin v2](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailover2Plugin.md)                   |
| `failureDetectionCount`                |                            `HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT`                            |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
| `failureDetectionEnabled`              |                           `HostMonitoringConnectionPlugin.FAILURE_DETECTION_ENABLED`                           |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
| `failureDetectionInterval`             |                          `HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL`                           |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
| `failureDetectionTime`                 |                            `HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME`                             |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
| `monitorDisposalTime`                  |                                 `MonitorServiceImpl.MONITOR_DISPOSAL_TIME_MS`                                  |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
| `iamDefaultPort`                       |                                   `IamAuthConnectionPlugin.IAM_DEFAULT_PORT`                                   |           [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)            |
| `iamHost`                              |                                       `IamAuthConnectionPlugin.IAM_HOST`                                       |           [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)            |
| `iamRegion`                            |                                      `IamAuthConnectionPlugin.IAM_REGION`                                      |           [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)            |
| `iamExpiration`                        |                                    `IamAuthConnectionPlugin.IAM_EXPIRATION`                                    |           [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)            |
| `awsProfile`                           |                                        `PropertyDefinition.AWS_PROFILE`                                        | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `wrapperLogUnclosedConnections`        |                                 `PropertyDefinition.LOG_UNCLOSED_CONNECTIONS`                                  | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `wrapperLoggerLevel`                   |                                       `PropertyDefinition.LOGGER_LEVEL`                                        |                             [Logging](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging)                             |
| `wrapperProfileName`                   |                                       `PropertyDefinition.PROFILE_NAME`                                        |              [Configuration Profiles](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles)              |
| `autoSortWrapperPluginOrder`           |                                  `PropertyDefinition.AUTO_SORT_PLUGIN_ORDER`                                   |                             [Plugins](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#plugins)                             |
| `loginTimeout`                         |                                       `PropertyDefinition.LOGIN_TIMEOUT`                                       | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `connectTimeout`                       |                                      `PropertyDefinition.CONNECT_TIMEOUT`                                      | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `socketTimeout`                        |                                      `PropertyDefinition.SOCKET_TIMEOUT`                                       | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `tcpKeepAlive`                         |                                      `PropertyDefinition.TCP_KEEP_ALIVE`                                       | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |

**A Secret ARN** has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`

## Logging
Enabling logging is a very useful mechanism for troubleshooting any issue one might potentially experience while using the AWS JDBC Driver.

In order to learn how to enable and configure logging, check out the [Logging](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging) section.

## Documentation
Technical documentation regarding the functionality of the AWS JDBC Driver will be maintained in this GitHub repository. Since the AWS JDBC Driver requires an underlying JDBC driver, please refer to the individual driver's documentation for driver-specific information.

### Using the AWS JDBC Driver
To find all the documentation and concrete examples on how to use the AWS JDBC Driver, please refer to the [AWS JDBC Driver Documentation](./docs/Documentation.md) page.

### Known Limitations

#### Amazon RDS Blue/Green Deployments

Although the AWS Advanced JDBC Wrapper is not compatible with [AWS Blue/Green Deployments](https://docs.aws.amazon.com/whitepapers/latest/overview-deployment-options/bluegreen-deployments.html) and does not officially support them, the combination of the AWS Advanced JDBC Wrapper and the Failover Plugin has been validated for use with clusters that employ Blue/Green Deployments. While general basic connectivity to both Blue and Green clusters is always in place, some failover cases are not fully supported.

The current limitations are:
- After a Blue/Green switchover, the wrapper may not be able to properly detect the new topology and handle failover, as there are discrepancies between the metadata and the available endpoints.
- The specific version requirements for Aurora MySQL versus Aurora PostgreSQL may vary, as the internal systems used by the wrapper can differ[^1].

The development team is aware of these limitations and is working to improve the wrapper's awareness and handling of Blue/Green switchovers. In the meantime, users can consider utilizing the `enableGreenNodeReplacement` configuration parameter, which allows the driver to override incorrect topology metadata and try to connect to available new Blue endpoints.

[^1]: Aurora MySQL requires v3.07 or later.

#### Amazon Aurora Global Databases

This driver currently does not support `planned failover` or `switchover` of Amazon Aurora Global Databases. Failing over to a secondary cluster will result in errors and there may be additional unforeseen errors when working with global databases. Connecting to the primary cluster is fully supported. There is a limitation when connected to the secondary cluster; the [failover2 plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailover2Plugin.md) will not work on the secondary cluster, however the [failover plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md) will work. Full Support for Amazon Aurora Global Databases is in the backlog, but we cannot comment on a timeline right now.

## Examples

| Description                                                                                                                                                                                                              |                                                                                                                                                                    Examples                                                                                                                                                                    |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Using the AWS JDBC Driver to get a simple connection                                                                                                                                                                     |                                                                                                                         [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/PgConnectionSample.java)                                                                                                                          |
| Using the AWS JDBC Driver with failover handling                                                                                                                                                                         |                                                                                                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/PgFailoverSample.java)                                                                                                                           |
| Using the AWS IAM Authentication Plugin with `DriverManager`                                                                                                                                                             | [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationPostgresqlExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMysqlExample.java) <br/> [MariaDB](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMariadbExample.java) |
| Using the AWS Secrets Manager Plugin with `DriverManager`                                                                                                                                                                |                                            [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginPostgresqlExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginMySQLExample.java)                                             |
| Using the AWS Credentials Manager to configure an alternative AWS credentials provider.                                                                                                                                  |                                                                                                               [PostgreSQL and MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsCredentialsManagerExample.java)                                                                                                                |
| Using the AWS JDBC Driver with `AWSWrapperDatasource`                                                                                                                                                                    |                                                                                                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/DatasourceExample.java)                                                                                                                          |
| Using the Driver Metadata Plugin to override driver name, this plugin [enables specific database features that may only be available to target drivers](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/370) |                                                                                                                [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/DriverMetaDataConnectionPluginExample.java)                                                                                                                |
| Using the Read/Write Splitting Plugin with `DriverManager`                                                                                                                                                               |                                                            [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingMySQLExample.java)                                                             |
| Using the Read/Write Splitting Plugin with Spring                                                                                                                                                                        |                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingSpringJdbcTemplatePostgresExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingSpringJdbcTemplateMySQLExample.java)                                           |
| Using HikariCP with the `AWSWrapperDatasource`                                                                                                                                                                           |                                                                                                                             [PostgreSQL](examples/HikariExample/src/main/java/software/amazon/HikariExample.java)                                                                                                                              |
| Using HikariCP with the `AWSWrapperDatasource` with failover handling                                                                                                                                                    |                                                                                                                         [PostgreSQL](examples/HikariExample/src/main/java/software/amazon/HikariFailoverExample.java)                                                                                                                          |
| Using Spring and HikariCP with the AWS JDBC Driver                                                                                                                                                                       |                                                                                                                                            [PostgreSQL](examples/SpringBootHikariExample/README.md)                                                                                                                                            |
| Using Spring and HikariCP with the AWS JDBC Driver and failover handling                                                                                                                                                 |                                                                                                                                            [PostgreSQL](examples/SpringTxFailoverExample/README.md)                                                                                                                                            |
| Using Spring and Hibernate with the AWS JDBC Driver                                                                                                                                                                      |                                                                                                                                            [PostgreSQL](examples/SpringHibernateExample/README.md)                                                                                                                                             |
| Using Spring and Wildfly with the AWS JDBC Driver                                                                                                                                                                        |                                                                                                                                             [PostgreSQL](examples/SpringWildflyExample/README.md)                                                                                                                                              |
| Using Vert.x and c3p0 with the AWS JDBC Driver                                                                                                                                                                           |                                                                                                                                                 [PostgreSQL](examples/VertxExample/README.md)                                                                                                                                                  |
| Using the AWS JDBC Driver with Telemetry and using the AWS Distro for OpenTelemetry Collector                                                                                                                            |                                                                                                                     [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/TelemetryMetricsOTLPExample.java)                                                                                                                     |
| Using the AWS JDBC Driver with Telemetry and using the AWS X-Ray Daemon                                                                                                                                                  |                                                                                                                     [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/TelemetryMetricsXRayExample.java)                                                                                                                     |

## Getting Help and Opening Issues
If you encounter a bug with the AWS JDBC Driver, we would like to hear about it.
Please search the [existing issues](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues) to see if others are also experiencing the issue before reporting the problem in a new issue. GitHub issues are intended for bug reports and feature requests. 

When opening a new issue, please fill in all required fields in the issue template to help expedite the investigation process.

For all other questions, please use [GitHub discussions](https://github.com/awslabs/aws-advanced-jdbc-wrapper/discussions).

## How to Contribute
1. Set up your environment by following the directions in the [Development Guide](docs/development-guide/DevelopmentGuide.md).
2. To contribute, first make a fork of this project. 
3. Make any changes on your fork. Make sure you are aware of the requirements for the project (e.g. do not require Java 7 if we are supporting Java 8 and higher).
4. Create a pull request from your fork. 
5. Pull requests need to be approved and merged by maintainers into the main branch. <br />
**Note:** Before making a pull request, [run all tests](./docs/development-guide/DevelopmentGuide.md#running-the-tests) and verify everything is passing.

### Code Style
The project source code is written using the [Google checkstyle](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml), and the style is strictly enforced in our automation pipelines. Any contribution that does not respect/satisfy the style will automatically fail at build time.

## Releases
The `aws-advanced-jdbc-wrapper` has a regular monthly release cadence. A new release will occur during the last week of each month. However, if there are no changes since the latest release, then a release will not occur.

## Aurora Engine Version Testing
This `aws-advanced-jdbc-wrapper` is being tested against the following Community and Aurora database versions in our test suite:

| Database          | Versions                                                                                                                                                                                                                                                                                                           |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL             | 8.0.36                                                                                                                                                                                                                                                                                                             |
| PostgreSQL        | 16.2                                                                                                                                                                                                                                                                                                               |
| Aurora MySQL      | - Default version. To check the default version, open the RDS "Create database" page in the AWS console and check the pre-selected database version. <br><br> - Latest release, as shown on [this page](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.Updates.30Updates.html).  |
| Aurora PostgreSQL | - Default version. To check the default version, open the RDS "Create database" page in the AWS console and check the pre-selected database version. <br><br> - Latest release, as shown on [this page](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Updates.html).  |

The `aws-advanced-jdbc-wrapper` is compatible with MySQL 5.7 and MySQL 8.0 as per the Community MySQL Connector/J 8.0 Driver.

## License
This software is released under the Apache 2.0 license.
