# Amazon Web Services (AWS) JDBC Driver

[![build_status](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/software.amazon.jdbc/aws-advanced-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Qodana](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml)

The **Amazon Web Services JDBC Driver** has been redesigned as an advanced JDBC wrapper. This wrapper is complementary to and extends the functionality of an existing JDBC driver to help an application take advantage of the features of clustered databases such as Amazon Aurora. The AWS JDBC Driver does not implement connectivity on its own to any database, but will enable support of AWS and Aurora functionalities on top of an underlying JDBC driver of the user's choice.

The AWS JDBC Driver is meant to work with any JDBC driver. Currently, the AWS JDBC Driver has been validated to support the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc), [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j), and [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j).

In conjunction with the PostgreSQL JDBC Driver, MySQL JDBC Driver, and MariaDB JDBC Driver, the AWS JDBC Driver enables fast failover for Amazon Aurora with PostgreSQL and MySQL compatibility. It also supports integration with [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) and [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/).

## About the Driver

### What is Failover?
In an Amazon Aurora database (DB) cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

### Benefits of the AWS JDBC Driver
Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver allows customers to continue using their existing community drivers in addition to having the AWS JDBC Driver fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to augment existing JDBC community drivers and be a unified connector to JDBC workflows for Aurora.

#### Enhanced Failure Monitoring
Enhanced Failure Monitoring (EFM) is a feature available from the [Host Monitoring Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md#enhanced-failure-monitoring) that periodically checks the connected database node's health and availability. If a database node is determined to be unhealthy, the connection is aborted.

#### Using the AWS JDBC Driver with plain RDS databases
Please visit [this page](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#using-the-aws-jdbc-driver-with-plain-rds-databases) for more information.

## Getting Started
For more information on how to obtain the AWS JDBC Driver, minimum requirements to use it, 
and how to integrate the AWS JDBC Driver into your project, please visit the 
[Getting Started page](./docs/GettingStarted.md).
### Maven Central
You can find our driver by searching in The Central Repository with GroupId and ArtifactId [software.amazon:aws-advanced-jdbc-wrapper][mvn-search].

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
```xml
<!-- Add the following dependency to your pom.xml, -->
<!-- replacing LATEST with specific version as required -->

<dependency>
  <groupId>software.amazon</groupId>
  <artifactId>aws-advanced-jdbc-wrapper</artifactId>
  <version>LATEST</version>
</dependency>
```

[mvn-search]: https://search.maven.org/search?q=g:software.amazon.jdbc "Search on Maven Central"

## Properties

| Parameter                              |                              Reference                               |                                                   Documentation Link                                                   |
|----------------------------------------|:--------------------------------------------------------------------:|:----------------------------------------------------------------------------------------------------------------------:|
| `wrapperDialect`                       |                       `DialectManager.DIALECT`                       |            [Dialects](/docs/using-the-jdbc-driver/DatabaseDialects.md), and whether you should include it.             |
| `wrapperPlugins`                       |                     `PropertyDefinition.PLUGINS`                     |                                                                                                                        |
| `secretsManagerSecretId`               |        `AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY`        |         [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)          |
| `secretsManagerRegion`                 |         `AwsSecretsManagerConnectionPlugin.REGION_PROPERTY`          |         [SecretsManagerPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)          |
| `wrapperDriverName`                    |         `DriverMetaDataConnectionPlugin.WRAPPER_DRIVER_NAME`         | [DriverMetaDataConnectionPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheDriverMetadataConnectionPlugin.md) |
| `failoverMode`                         |               `FailoverConnectionPlugin.FAILOVER_MODE`               |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `clusterInstanceHostPattern`           |        `AuroraHostListProvider.CLUSTER_INSTANCE_HOST_PATTERN`        |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `enableClusterAwareFailover`           |       `FailoverConnectionPlugin.ENABLE_CLUSTER_AWARE_FAILOVER`       |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `failoverClusterTopologyRefreshRateMs` | `FailoverConnectionPlugin.FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS` |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `failoverReaderConnectTimeoutMs`       |    `FailoverConnectionPlugin.FAILOVER_READER_CONNECT_TIMEOUT_MS`     |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `failoverTimeoutMs`                    |            `FailoverConnectionPlugin.FAILOVER_TIMEOUT_MS`            |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `failoverWriterReconnectIntervalMs`    |   `FailoverConnectionPlugin.FAILOVER_WRITER_RECONNECT_INTERVAL_MS`   |                 [FailoverPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)                 |
| `failureDetectionCount`                |       `HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT`       |           [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)           |
| `failureDetectionEnabled`              |      `HostMonitoringConnectionPlugin.FAILURE_DETECTION_ENABLED`      |           [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)           |
| `failureDetectionInterval`             |     `HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL`      |           [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)           |
| `failureDetectionTime`                 |       `HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME`        |           [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)           |
| `monitorDisposalTime`                  |            `MonitorServiceImpl.MONITOR_DISPOSAL_TIME_MS`             |           [HostMonitoringPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)           |
| `iamDefaultPort`                       |              `IamAuthConnectionPlugin.IAM_DEFAULT_PORT`              |        [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)        |
| `iamHost`                              |                  `IamAuthConnectionPlugin.IAM_HOST`                  |        [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)        |
| `iamRegion`                            |                 `IamAuthConnectionPlugin.IAM_REGION`                 |        [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)        |
| `iamExpiration`                        |               `IamAuthConnectionPlugin.IAM_EXPIRATION`               |        [IamAuthenticationPlugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)        |
| `wrapperLogUnclosedConnections`        |            `PropertyDefinition.LOG_UNCLOSED_CONNECTIONS`             |                  [LogUnclosedConnections](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging)                  |
| `wrapperLoggerLevel`                   |                  `PropertyDefinition.LOGGER_LEVEL`                   |                       [LoggingLevel](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging)                       |
| `wrapperProfileName`                   |                  `PropertyDefinition.PROFILE_NAME`                   |           [ConfigurationProfiles](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles)           |

**A Secret ARN** has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`
## Using the AWS JDBC Driver
Please refer to the AWS JDBC Driver's [Documentation page](./docs/Documentation.md) for details about using the AWS JDBC Driver. 

## Logging
To configure logging, check out the [Logging](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging) section.

## Documentation
Technical documentation regarding the functionality of the AWS JDBC Driver will be maintained in this GitHub repository. Since the AWS JDBC Driver requires an underlying JDBC driver, please refer to the individual driver's documentation for driver-specific information.

## Examples

| Description                                                                                                                                                                                                              |                                                                                                                                                                    Examples                                                                                                                                                                    |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
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
| Using Spring and Hibernate with the AWS JDBC Driver                                                                                                                                                                      |                                                                                                                                            [PostgreSQL](examples/SpringHibernateExample/README.md)                                                                                                                                             |
| Using Spring and Wildfly with the AWS JDBC Driver                                                                                                                                                                        |                                                                                                                                             [PostgreSQL](examples/SpringWildflyExample/README.md)                                                                                                                                              |
| Using Vert.x and c3p0 with the AWS JDBC Driver                                                                                                                                                                           |                                                                                                                                                 [PostgreSQL](examples/VertxExample/README.md)                                                                                                                                                  |

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
We enforce a style using [Google checkstyle](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml).
This can be verified in the tests; tests will not pass with checkstyle errors.

## Releases
The `aws-advanced-jdbc-wrapper` has a regular monthly release cadence. A new release will occur during the last week of each month. However, if there are no changes since the latest release, then a release will not occur.

## Aurora Engine Version Testing
This `aws-advanced-jdbc-wrapper` is being tested against the following Community and Aurora database versions in our test suite:

| Database          | Versions                                                                                                                                                                                                   |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL             | 8.0.32                                                                                                                                                                                                     |
| PostgreSQL        | 15.2                                                                                                                                                                                                       |
| Aurora MySQL      | MySQL	8.0.mysql_aurora.3.02.2 (Wire-compatible with MySQL 8.0.23 onwards. For more details see [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraMySQLReleaseNotes/AuroraMySQL.Updates.3022.html)) |
| Aurora PostgreSQL | 14.7 and 15.2 (Compatible with PostgreSQL 14.7 and 15.2, see release notes [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Updates.html))                |

The `aws-advanced-jdbc-wrapper` is compatible with MySQL 5.7 and MySQL 8.0 as per the Community MySQL Connector/J 8.0 Driver.

## License
This software is released under the Apache 2.0 license.
