# Amazon Web Services (AWS) JDBC Driver
[![build_status](https://github.com/aws/aws-advanced-jdbc-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/aws/aws-advanced-jdbc-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/software.amazon.jdbc/aws-advanced-jdbc-wrapper)](https://central.sonatype.com/artifact/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/software.amazon.jdbc/aws-advanced-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/software.amazon.jdbc/aws-advanced-jdbc-wrapper)

The **Amazon Web Services (AWS) JDBC Driver** has been redesigned as an advanced JDBC wrapper.

The wrapper is complementary to an existing JDBC driver and aims to extend the functionality of the driver to enable applications to take full advantage of the features of clustered databases such as Amazon Aurora. In other words, the AWS JDBC Driver does not connect directly to any database, but enables support of AWS and Aurora functionalities on top of an underlying JDBC driver of the user's choice. This approach enables service-specific enhancements, without requiring users to change their workflow and existing JDBC driver tooling.

The AWS JDBC Driver is targeted to work with **any** existing JDBC driver. Currently, the AWS JDBC Driver has been validated to support the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc), [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j), and [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j).

The AWS JDBC Driver provides modular functionality through feature plugins, with each plugin being relevant to specific database services based on their architecture and capabilities. For example, [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) authentication is supported across multiple services, while [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) applies to services that support password-based authentication. The fast failover plugin provides reduced recovery time during failover for Aurora PostgreSQL and Aurora MySQL clusters.

## Benefits of the AWS JDBC Driver for All Aurora and RDS Database Services

### Seamless AWS Authentication Service Integration

Built-in support for AWS Identity and Access Management (IAM) authentication eliminates the need to manage database passwords, while AWS Secrets Manager integration provides secure credential management for services that require password-based authentication.

### Preserve Existing Workflows

The wrapper design allows developers to continue using their preferred JDBC drivers and existing database code while gaining service-specific enhancements. No application rewrites are required.

### Modular Plugin Architecture

The plugin-based design ensures applications only load the functionality they need, reducing dependencies and overhead.

## Benefits of the AWS JDBC Driver for Aurora PostgreSQL, Aurora MySQL, and RDS

### Faster Failover and Reduced Downtime

For Aurora PostgreSQL, Aurora MySQL, and RDS Multi-AZ DB clusters, the driver significantly reduces connection recovery time during [database failovers](./docs/using-the-jdbc-driver/WhatIsFailover.md). By maintaining a real-time cache of cluster topology and bypassing DNS resolution delays, applications can reconnect to healthy database instances in seconds rather than minutes.

### Enhanced Failure Detection

The driver includes Enhanced Failure Monitoring (EFM) that proactively monitors database node health, detecting failures faster than traditional timeout-based approaches. This allows applications to respond to issues before they impact end users.

## Using the AWS JDBC Driver with...

### Amazon Aurora PostgreSQL and Aurora MySQL

The AWS JDBC Driver provides fast failover capabilities for Aurora PostgreSQL and Aurora MySQL clusters, significantly reducing connection recovery time during database failovers.

Visit [this page](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailover2Plugin.md) for more details.

### Amazon RDS Multi-AZ DB Clusters

The [AWS RDS Multi-AZ DB Clusters](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) are capable of switching over the current writer node to another node in the cluster within approximately 1 second or less, in case of minor engine version upgrade or OS maintenance operations. The AWS JDBC Driver has been optimized for such fast failover when working with AWS RDS Multi-AZ DB Clusters.

With the `failover` plugin, the downtime during certain DB cluster operations, such as engine minor version upgrades, can be reduced to one second or even less with finely tuned parameters. It supports both MySQL and PostgreSQL clusters.

Visit [this page](./docs/using-the-jdbc-driver/SupportForRDSMultiAzDBCluster.md) for more details.

### Plain Amazon RDS databases

The AWS JDBC Driver also works with RDS provided databases that are not Aurora.

### RDS Proxy

There are limitations with the AWS JDBC Driver and RDS Proxy. This is currently intended, by design, since the main reason is that RDS Proxy transparently re-routes requests to a single database instance. RDS Proxy decides which database instance is used based on many criteria (on a per-request basis). Due to this, functionality like Failover, Enhanced Host Monitoring, and Read/Write Splitting is not compatible since the driver relies on cluster topology and RDS Proxy handles this automatically.

However, the driver can still be used to handle authentication workflows. For more information regarding compatibility, please refer to the specific plugin documentation.

Visit [this page](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#using-the-aws-jdbc-driver-with-plain-rds-databases) for more details.

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
| `wrapperDialect`                       |                                            `DialectManager.DIALECT`                                            |               [Dialects](./docs/using-the-jdbc-driver/DatabaseDialects.md), and whether you should include it.                |
| `wrapperPlugins`                       |                                          `PropertyDefinition.PLUGINS`                                          |                                                                                                                               |
| `clusterId`                            |                                        `RdsHostListProvider.CLUSTER_ID`                                        | [AWS Advanced JDBC Driver Parameters](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters) |
| `clusterInstanceHostPattern`           |                              `RdsHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN`                               |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
| `secretsManagerSecretId`               |                             `AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY`                             |             [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)             |
| `secretsManagerRegion`                 |                              `AwsSecretsManagerConnectionPlugin.REGION_PROPERTY`                               |             [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)             |
| `wrapperDriverName`                    |                              `DriverMetaDataConnectionPlugin.WRAPPER_DRIVER_NAME`                              |    [DriverMetaDataConnectionPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheDriverMetadataConnectionPlugin.md)     |
| `failoverMode`                         |                                    `FailoverConnectionPlugin.FAILOVER_MODE`                                    |                    [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                     |
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
| `monitorDisposalTime`                  |                               `HostMonitorServiceImpl.MONITOR_DISPOSAL_TIME_MS`                                |              [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)               |
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

Support for Blue/Green deployments using the AWS Advanced JDBC Driver requires specific metadata tables. The following service versions provide support for Blue/Green Deployments:

- Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.
- Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.
- Supported Aurora MySQL Versions: Engine Release `3.07` and above.

Please note that Aurora Global Database and RDS Multi-AZ clusters with Blue/Green deployments is currently not supported. For detailed information on supported database versions, refer to the [Blue/Green Deployment Plugin Documentation](./docs/using-the-jdbc-driver/using-plugins/UsingTheBlueGreenPlugin.md).

#### Amazon Aurora Global Databases

This driver currently does not support `planned failover` or `switchover` of Amazon Aurora Global Databases. Failing over to a secondary cluster will result in errors and there may be additional unforeseen errors when working with global databases. Connecting to the primary cluster is fully supported. There is a limitation when connected to the secondary cluster; the [failover2 plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailover2Plugin.md) will not work on the secondary cluster, however the [failover plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md) will work. When working with Global Databases and accepting the driver's limited support, ensure you explicitly provide a list of plugins. This is crucial because the `failover2` plugin, which is enabled by default, does not support Global Databases as previously mentioned. By specifying `failover` plugin along with your desired plugins, you can ensure (still limited) functionality with Global Databases. Full Support for Amazon Aurora Global Databases is in the backlog, but we cannot comment on a timeline right now.

## Examples

| Description                                                                                                                                                                                                          |                                                                                                                                                                    Examples                                                                                                                                                                    |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| Using the AWS JDBC Driver to get a simple connection                                                                                                                                                                 |                                                                                                                         [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/PgConnectionSample.java)                                                                                                                          |
| Using the AWS JDBC Driver with failover handling                                                                                                                                                                     |                                                                                                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/PgFailoverSample.java)                                                                                                                           |
| Using the AWS IAM Authentication Plugin with `DriverManager`                                                                                                                                                         | [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationPostgresqlExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMysqlExample.java) <br/> [MariaDB](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMariadbExample.java) |
| Using the AWS IAM Authentication Plugin with `DataSource`                                                                                                                                                            |                                                                                                           [PostgreSQL and MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationDatasourceExample.java)                                                                                                           |
| Using the AWS Secrets Manager Plugin with `DriverManager`                                                                                                                                                            |                                            [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginPostgresqlExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginMySQLExample.java)                                             |
| Using the AWS Credentials Manager to configure an alternative AWS credentials provider.                                                                                                                              |                                                                                                               [PostgreSQL and MySQL](examples/AWSDriverExample/src/main/java/software/amazon/AwsCredentialsManagerExample.java)                                                                                                                |
| Using the AWS JDBC Driver with `AWSWrapperDatasource`                                                                                                                                                                |                                                                                                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/DatasourceExample.java)                                                                                                                          |
| Using the Driver Metadata Plugin to override driver name, this plugin [enables specific database features that may only be available to target drivers](https://github.com/aws/aws-advanced-jdbc-wrapper/issues/370) |                                                                                                                [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/DriverMetaDataConnectionPluginExample.java)                                                                                                                |
| Using the Read/Write Splitting Plugin with `DriverManager`                                                                                                                                                           |                                                            [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingMySQLExample.java)                                                             |
| Using the Read/Write Splitting Plugin with Spring                                                                                                                                                                    |                                          [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingSpringJdbcTemplatePostgresExample.java) <br/> [MySQL](examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingSpringJdbcTemplateMySQLExample.java)                                           |
| Using HikariCP with the `AWSWrapperDatasource`                                                                                                                                                                       |                                                                                                                             [PostgreSQL](examples/HikariExample/src/main/java/software/amazon/HikariExample.java)                                                                                                                              |
| Using HikariCP with the `AWSWrapperDatasource` with failover handling                                                                                                                                                |                                                                                                                         [PostgreSQL](examples/HikariExample/src/main/java/software/amazon/HikariFailoverExample.java)                                                                                                                          |
| Using Spring and HikariCP with the AWS JDBC Driver                                                                                                                                                                   |                                                                                                                                            [PostgreSQL](examples/SpringBootHikariExample/README.md)                                                                                                                                            |
| Using Spring and HikariCP with the AWS JDBC Driver and failover handling                                                                                                                                             |                                                                                                                                            [PostgreSQL](examples/SpringTxFailoverExample/README.md)                                                                                                                                            |
| Using Spring and Hibernate with the AWS JDBC Driver                                                                                                                                                                  |                                                                                                                                            [PostgreSQL](examples/SpringHibernateExample/README.md)                                                                                                                                             |
| Using Spring and Wildfly with the AWS JDBC Driver                                                                                                                                                                    |                                                                                                                                             [PostgreSQL](examples/SpringWildflyExample/README.md)                                                                                                                                              |
| Using Vert.x and c3p0 with the AWS JDBC Driver                                                                                                                                                                       |                                                                                                                                                 [PostgreSQL](examples/VertxExample/README.md)                                                                                                                                                  |
| Using the AWS JDBC Driver with Telemetry and using the AWS Distro for OpenTelemetry Collector                                                                                                                        |                                                                                                                     [PostgreSQL](examples/AWSDriverExample/src/main/java/software/amazon/TelemetryMetricsOTLPExample.java)                                                                                                                     |
| Using the AWS JDBC Driver with Telemetry and using the AWS X-Ray Daemon                                                                                                                                              |                                                                                                                    [PostgreSQL](./examples/AWSDriverExample/src/main/java/software/amazon/TelemetryTracingXRayExample.java)                                                                                                                    |

## Getting Help and Opening Issues
If you encounter a bug with the AWS JDBC Driver, we would like to hear about it.
Please search the [existing issues](https://github.com/aws/aws-advanced-jdbc-wrapper/issues) to see if others are also experiencing the issue before reporting the problem in a new issue. GitHub issues are intended for bug reports and feature requests.

When opening a new issue, please fill in all required fields in the issue template to help expedite the investigation process.

For all other questions, please use [GitHub discussions](https://github.com/aws/aws-advanced-jdbc-wrapper/discussions).

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
