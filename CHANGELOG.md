# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

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

[1.0.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.0
