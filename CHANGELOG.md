# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [2.2.0] - 2023-6-14

### :magic_wand: Added
- Autoscaling and the least connections strategy ([PR #451](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/451)).
- [Target driver dialects](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-the-jdbc-driver/TargetDriverDialects.md) ([PR #452](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/452)).
- Elastic Load Balancer URL support ([PR #476](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/476)).
- Documentation:
  - Using the Driver with plain RDS Databases. See [Using the Driver](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#using-the-aws-jdbc-driver-with-plain-rds-databases).
  - Internal connection pool behaviour only verifying password on initial connection. See [Using the Read Write Splitting Plugin Internal Connection Pooling document](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) and [code example](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/examples/AWSDriverExample/src/main/java/software/amazon/InternalConnectionPoolPasswordWarning.java).
  - Link performance test in table of contents. See [Documentation Table of Contents](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/Documentation.md).
  - Cluster URLs are not internally pooled. See [Using Read Write Splitting Plugin Internal Connection Pooling](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling).
  - The `leastConnections` strategy in the [Using Read Write Splitting Plugin Internal Connection Pooling](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) at point #3.
- Sample code and tutorial for using the driver with:
  - Spring and Hibernate
  - Spring and Wildfly

### :bug: Fixed
- Pruned null connections in connection tracker plugins ([PR #461](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/461))
- HikariCP integration tests and reworked AwsWrapperDataSource by removing property names, introduced a set of simple properties to set database, server name and server port ([PR #468](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/468)).
- Checkstyle failure due to modified license and driver connection provider passing original properties resulting properties being overridden for subsequent connections ([PR #471](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/471)).
- IamAuthConnectionPlugin to properly prioritize the override property IAM_DEFAULT_PORT then Hosts port, and then Dialect port ([Issue #473](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/473)).
- Values for the `wrapperLoggerLevel` parameter are no longer case-sensitive ([#PR #481](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/481)).

### :crab: Changed
- Extended test logs with thread names ([PR #465](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/465)).
- Updated logging format so it is easier to distinguish the logger name from the log message ([PR #469](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/)).

## [2.1.2] - 2023-5-21
### :crab: Changed
- Explicitly check for 28000 and 28P01 SQLStates in the IAM authentication plugin after SQLExceptions are thrown ([PR #456](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/456) and [PR #457](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/457)).

## [2.1.1] - 2023-5-15
### :bug: Fixed
- MySQL reference in code that could impact workflows with other drivers ([PR #446](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/446)).

## [2.1.0] - 2023-5-11
### :magic_wand: Added
- Checks for stale writer records in `AuroraHostListProvider` obtained after writer-failover so that they are not used ([PR #435](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/435)).

### :bug: Fixed
- Potential security concern by ensuring that user-specific connections in the connection pool are returned to the correct user ([PR #432](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/432)).
- Connection state transfer bug where switching from read-only connection to writer connection incorrectly triggers the failover process ([Issue #426](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/426)).
- Incorrect invalidation of the newly promoted writer and random readers after failover. EFM plugin to use instance endpoint as the monitoring endpoint in case the initial connection is established using cluster endpoint ([PR #431](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/431)).
- Running Hibernate tests no longer runs unrelated tests ([PR #417](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/417)).
- Reader failover using a shared Properties object resulting in race conditions ([PR #436](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/436) & [PR #438](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/438)).
- Temporarily setting the socket timeout for the topology query if it is not set, to avoid topology query from executing indefinitely ([PR #416](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/416)).

### :crab: Changed
- Removed logic from the failover plugin that changes connection to a writer instance when `setReadOnly(false)` is called on a reader connection ([Issue #426](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/426)). This functionality already exists in the read write splitting plugin.
- Removed Multi-writer cluster related code as they are no longer supported ([PR #435](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/435)).
- Clarified documentation on the failover process to account for Aurora PostgreSQL clusters being offline during failover ([PR #437](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/437)).
- :warning: Breaking changes were introduced with the new `failoverMode` configuration parameter ([PR #434](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/434)):
    - The `failoverMode` parameter replaces the `enableFailoverStrictReader` configuration parameter.
    - If you were previously using `enableFailoverStrictReader=true`, please update it to `failoverMode=strict-reader`.
    - For more information, please check the ([documentation](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#failover-parameters))

## [2.0.0] - 2023-04-28
### :magic_wand: Added
- [Read / Write Splitting (Official Release)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md).
- [Internal connection pools for the R/W splitting plugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) ([PR #359](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/359))
- [Database dialects](/docs/using-the-jdbc-driver/DatabaseDialects.md) ([PR #372](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/372) addresses [Issue #341](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/341)), which allow users to specify the database type to connect to.

### :bug: Fixed
- Fetched the instance endpoint and added it to the host aliases for connections established using custom domains ([Issue #386](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/386)).

### :crab: Changed
- Parsed region from ARN ([PR #392](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/392) addresses [Issue #391](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/391)).
- Updated documentation on:
  1. how to run integration tests ([PR #396](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/396)).
  2. required DataSource properties ([PR #398](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/398))
- :warning: Breaking changes were introduced with the internal connection pool changes ([PR #359](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/359)):
  - The ConnectionPlugin interface has introduced three new methods: forceConnect, acceptsStrategy, and getHostSpecByStrategy. Although the AbstractConnectionPlugin implements default behavior for these methods, you should consider adding your own implementations if you have implemented a custom ConnectionPlugin. More details on these methods can be found in the ConnectionPlugin Javadocs and the [pipelines documentation](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/development-guide/Pipelines.md).
  - The HostListProvider interface has introduced a new method: getHostRole. If you have implemented your own HostListProvider, you will need to implement this method. More details on this method can be found in the HostListProvider Javadocs.

## [1.0.2] - 2023-03-31
### :magic_wand: Added
- Default list of plugins added to the parameter `wrapperPlugins` ([PR #332](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/332)).
- Read-write splitting plugin example using Spring JDBC template. See [Read-Write Splitting Spring JDBC Template Example](./examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingSpringJdbcTemplateExample.java).
- Read-write splitting plugin benchmark and performance results ([PR #340](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/340) & [PR #316](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/316)).
- Aurora Connection Tracker plugin tracks all opened connections and closes all impacted connections after a failover ([PR #298](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/298)).
- Driver Metadata plugin allows users to override the driver name ([PR #371](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/371) addresses [Issue #370](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/370)).
- Documentation for the Driver Metadata plugin and Aurora Connection Tracker plugin. See [Using The Driver Metadata Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheDriverMetadataConnectionPlugin.md) & [Using The Aurora Connection Tracker Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAuroraConnectionTrackerPlugin.md).

### :bug: Fixed
- Unwrapped Savepoint objects when passing them in as parameters ([Issue #328](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/328)).
- Added null checks to `WrapperUtils#getConnectionFromSqlObject()` ([Issue #348](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/348)).
- Extra question mark in `clusterInstanceHostPattern` parameters is no longer filtered out when setting the connection string ([PR #383](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/383)).

### :crab: Changed
- Lock initialization of `AuroraHostListProvider` ([PR #347](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/347)).
- Optimized thread locks and expiring cache for the Enhanced Monitoring Plugin. ([PR #365](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/365)).
- Updated Hibernate sample code to reflect changes in the wrapper source code ([PR #368](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/368)).
- Updated KnownLimitations.md to reflect that Amazon RDS Blue/Green Deployments are not supported. See [Amazon RDS Blue/Green Deployments](./docs/KnownLimitations.md#amazon-rds-blue-green-deployments).

## [1.0.1] - 2023-01-30
### :magic_wand: Added
- [Read / Write Splitting and Load Balancing (Experimental)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md). Ongoing changes are being implemented to mirror behaviour of the community MySQL JDBC driver. We do not recommend using this in production until an official release.
- The Aurora Stale DNS Plugin to prevent the user application from incorrectly opening a new connection to an old writer node when DNS records have not yet updated after a recent failover event. For more details, see [Aurora Stale DNS Plugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#:~:text=Aurora%20Stale%20DNS%20Plugin).
- FailoverSQLException classes for easier error handling. See [Read-Write Splitting Postgres Example](./examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java).
- OSGi compatibility ([PR #270](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/270)).
- AwsCredentialsManager to customize credentials providers used in `IamAuthenticationPlugin` and `AwsSecretsManagerPlugin`. For more information, see [AWS Credentials Configuration](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/custom-configuration/AwsCredentialsConfiguration.md).

### :bug: Fixed
- `DataSourceConnectionProvider` no longer removes user/password properties on connect ([Issue #288](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/288) and [Issue #305](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/305)).
- Runtime exceptions thrown as reported in [issue #284](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/284).
- Incorrect log message in `PluginServiceImpl#setAvailability` that says host alias not found instead of empty hosts change list.
- FailoverTimeoutMS not being obeyed during failover, causing failover to take twice as long ([PR #244](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/244)).
- Reader failover sometimes reconnect to writer instances as reported by [Issue #223](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/223). Applications can now set the `failoverStrictReader` parameter to only allow failover to reader nodes during the reader failover process. See more details [here](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#failover-parameters).
- AWS Secrets Manager Plugin leaking PoolingHttpClientConnectionManager ([PR #321](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/321)).
- Internal `inTransaction` flag not being updated when `autocommit` status changes ([PR #282](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/282)).
- Incorrect wrapper version returned from `getDriverVersion` calls ([PR #319](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/319)).
- Incorrect `setReadOnly` behaviour when the method is called on a closed connection ([Issue #311](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/311)).
- `isCurrentHostWriter` incorrectly return false during writer failover ([PR #323](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/323)).

### :crab: Changed
- Use default connection check intervals/timeouts in the EFM plugin when a user-supplied setting is not available ([PR #274](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/274)).
- Thread-safety improvements within in the EFM plugin ([PR #332](https://github.com/awslabs/aws-mysql-jdbc/pull/332)).

## [1.0.0] - 2022-10-06
The Amazon Web Services (AWS) Advanced JDBC Driver allows an application to take advantage of the features of clustered Aurora databases.

### :magic_wand: Added
* Support for PostgreSQL
* The [Failover Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)
* The [Host Monitoring Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)
* The [AWS IAM Authentication Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)
* The [AWS Secrets Manager Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)

[2.2.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.2...2.2.0
[2.1.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.1...2.1.2
[2.1.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.0.0...2.1.0
[2.0.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.2...2.0.0
[1.0.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.0
