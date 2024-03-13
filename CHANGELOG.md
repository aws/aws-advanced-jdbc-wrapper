# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [2.3.5] - 2024-03-14

### :magic_wand: Added
- Sample code configuring the AWS JDBC Driver with DBCP ([PR #930](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/930)).

### :crab: Changed
- Fix issue with deadlock while using prepared transactions and PostgreSQL Explicit Locking ([PR #918](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/918)).
- Removed `ConnectionStringHostListProvider#identifyConnection` since it is not used ([PR #920](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/920)).

## [2.3.4] - 2024-03-01
### :magic_wand: Added
- Documentation:
  - Bundled Uber Jar for Federated Authentication. See [UsingTheFederatedAuthPlugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFederatedAuthPlugin.md#bundled-uber-jar).
  - Using the Read Write Splitting Plugin's internal connection pool with Spring applications. See [UsingTheReadWriteSplittingPlugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pools).
- Spring Framework application code examples with load balanced access to database cluster reader instances ([PR #852](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/852)).
- New configuration preset `SF_` optimized for Spring Framework applications ([PR #852](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/852)).
- Lightweight alternative for IAM token generator that requires fewer dependencies ([PR #867](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/867)).

### :bug: Fixed
- Fixes to session state transfer ([PR #852](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/852)).
- Enhanced Host Monitoring Plugin (EFM) v2 plugin to use `ConcurrentHashMap` instead of `HashMap` to avoid `ConcurrentModificationException` ([Issue #855](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/855)).
- Move lock location and skip executing `Statement.getConnection` when running `Statement.cancel` to fix `Statement.cancel` for MySQL ([PR #851](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/851))
- Remove Telemetry trace associated with a Monitor thread because traces for long-running tasks is an anti-pattern ([PR #875](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/875)). 

### :crab: Changed
- HostSelector implementations to take into account HostAvailability ([PR #856](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/856)).
- Reduced the number of Regular Expression checks with `Matcher.find` to improve performance ([PR #854](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/854)).
- HostSpec class to not use a default lastUpdateTime and instead use null ([PR 877](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/877)).
- Moved Reader Selection Strategies out of the `UsingTheReadWriteSplittingPlugin` doc and into its own page. See [ReaderSelectionStrategies](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/ReaderSelectionStrategies.md). 

## [2.3.3] - 2024-01-23
### :magic_wand: Added
- Documentation:
  - [Read Write Splitting Plugin Limitations with Spring Boot/Framework](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#limitations-when-using-spring-bootframework).
  - AWS Profile configuration parameter. See [README](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/README.md#properties), [UsingTheJDBCDriver](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#aws-advanced-jdbc-driver-parameters), and [AwsCredentialsConfiguration](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/custom-configuration/AwsCredentialsConfiguration.md).
- Example code for ReadWriteSplitting Plugin ([PR #765](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/765)).
- Enabling AWS Profile for IAM and AWS Secrets Manager authentication plugins ([PR #786](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/786)).

### :bug: Fixed
- SqlMethodAnalyzer to handle empty SQL query and not throw IndexOutOfBoundsException ([PR #798](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/798)).
- Restructure try blocks in dialects for exception handling ([PR #799](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/799)).
- Log message to communicate that an RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting ([PR 801](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/801)).
- Make a variable volatile in RdsHostListProvider ([Issue #486](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/486)).
- Transfer session state during failover ([Issue #812](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/812)).
- Release all stopped monitors so that they are not reused ([PR #831](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/831)).
- Added update candidates for the MariaDB dialect in order to swap to MySQL dialects in the case of using a MySQL database with the protocol `jdbc:aws-wrapper:mariadb://`, and fixed the RDS MySQL dialect from incorrectly returning false in `isDialect` method ([Issue #789](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/789)).

### :crab: Changed
- Session state tracking and transfer redesign ([PR #821](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/821)).
- Improve Multi-AZ cluster detection ([PR #824](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/824)).
- Enhanced Host Monitoring Plugin (EFM) v2 plugin is now a default plugin. The original EFM plugin can still be used by specifying `efm` in the `wrapperPlugins` parameter ([PR #825](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/825)). 
- Update China endpoint patterns ([PR #832](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/832)).

## [2.3.2] - 2023-12-18
### :magic_wand: Added
- [Federated Authentication Plugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFederatedAuthPlugin.md), which supports SAML authentication through ADFS ([PR #741](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/741)).
- [**Experimental** Enhanced Host Monitoring Plugin v2](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md#experimental-host-monitoring-plugin-v2), which is a redesign of the original Enhanced Host Monitoring Plugin that addresses memory leaks and high CPU usage during monitoring sessions ([PR #764](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/764)).
- Fastest Response Strategy Plugin, which implements a new autoscaling strategy ([PR #755](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/755)).
- Plugin code for Aurora Initial Connection Strategy Plugin. This plugin returns an instance endpoint when connected using a cluster endpoint ([PR #784](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/784)).

### :bug: Fixed
- Use existing entries to update the round-robin cache ([PR #739](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/739)).

### :crab: Changed
- Updated HikariCP example to include configuring the datasource with a JDBC URL ([PR #749](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/749)).
- Replaced the `sychronized` keyword with reentrant locks in AwsCredentialsManager ([PR #785](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/785)).
- Set HostId in HostSpec when connecting using Aurora instance endpoints ([PR #782](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/782)).

## [2.3.1] - 2023-11-29
### :magic_wand: Added
- User defined session state transfer functions ([PR #729](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/729)).
- Documentation for using the driver with RDS Multi-AZ database clusters ([PR #740](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/740)).
- [Configuration profiles](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles) and [configuration presets](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/ConfigurationPresets.md) ([PR #711](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/711) and [PR #738](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/738)).

### :bug: Fixed
- Stopped monitoring threads causing out of memory errors ([PR #718](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/718)).
- Automatically register a target driver in the class path to prevent `No suitable driver` SQL exceptions ([PR #748](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/748)).

### :crab: Changed
- Session state tracking to include additional state information ([PR #729](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/729)).
- Log level for intentionally ignored exceptions to reduce the number of warnings ([PR #751](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/751)).

## [2.3.0] - 2023-11-23
### :magic_wand: Added
- Fast switchover support for Amazon RDS Multi-AZ DB Clusters ([PR #690](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/690)).
- Endpoint override for the AWS Secrets Manager plugin ([PR #707](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/707)).
- Allow users to set up a lambda to initialize new connections ([PR #705](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/705)).
- Introduced `Dialect.prepareConnectProperties` to allow dialect classes to modify connection properties when opening a new connection ([PR #704](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/704)).
- Native telemetry support ([PR #617](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/617)).
- Documentation on known limitations with global databases ([PR #695](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/695)).

### :bug: Fixed
- Continue monitoring if unhandled Exception is thrown ([PR #676](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/676)).
- Password properties are now masked in logs ([PR #701](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/701) and [PR #723](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/723)).
- Issue when getting a connection for a closed statement ([PR #682](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/682)).
- Maven coordinates in README ([PR #681](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/681)).
- Update topology for specific methods ([PR #683](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/683)).

### :crab: Changed
- Added buffer to IAM token expiry and moved token expiry time creation ([PR #706](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/706)).
- Documentation on known limitations with Blue/Green deployments ([PR #680](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/680)).

## [2.2.5] - 2023-10-03
### :magic_wand: Added
- Optional preservation of partial session state post failover ([PR #632](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/632)).
- Round Robin host selection strategy ([PR #603](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/603)).
- Sample application failover retry with Spring Boot ([PR #638](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/638)).

### :crab: Changed
- Renamed the `messages.properties` file to `aws_advanced_jdbc_wrapper_messages.properties` ([Issue #633](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/633)).

## [2.2.4] - 2023-08-29
### :magic_wand: Added
- Host Availability Strategy to help keep host health status up to date ([PR #530](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/530)).
- Implement `setLoginTimeout` from a suggested enhancement ([Discussion #509](https://github.com/awslabs/aws-advanced-jdbc-wrapper/discussions/509)).

### :bug: Fixed
- Allow connecting with reader cluster endpoints for Aurora PostgreSQL versions 13.9 and greater by changing the `AuroraPgDialect` topology query ([Issue #593](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/593)).
- Race condition issues between `MonitorThreadContainer#getInstance()` and `MonitorThreadContainer#releaseInstance()` ([PR #601](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/601)).

### :crab: Changed
- Dynamically sets the default host list provider based on the dialect used. User applications no longer need to manually set the AuroraHostListProvider when connecting to Aurora Postgres or Aurora MySQL databases.
- Deprecated AuroraHostListConnectionPlugin.
  - As an enhancement, the wrapper is now able to automatically set the Aurora host list provider for connections to Aurora MySQL and Aurora PostgreSQL databases.
    Aurora Host List Connection Plugin is deprecated. If you were using the `AuroraHostListConnectionPlugin`, you can simply remove the plugin from the `wrapperPlugins` parameter.
    However, if you choose to, you can ensure the provider is used by specifying a topology-aware dialect, for more information, see [Database Dialects](docs/using-the-jdbc-driver/DatabaseDialects.md).
- Propagate `Connection.clearWarnings()` to underlying connections in the Read Write Splitting Plugin so that the connection object does not accumulate warning messages  ([Issue #547](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/547)).
- Close underlying connections in the Read Write Splitting Plugin after switching to read-write or read-only depending on whether internal connection pooling is used ([PR #583](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/583)).
- Sort plugins by default to prevent plugin misconfiguration. This can be disabled by setting the property `autoSortWrapperPluginOrder` to false ([PR #542](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/542)).
- Documentation:
  - Clarified AWS JDBC Driver limitations with Blue/Green deployments. See [Known Limitations](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/KnownLimitations.md#amazon-rds-bluegreen-deployments).
  - Updated and reworded main [README.md](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/README.md) page.

## [2.2.3] - 2023-07-28
### :magic_wand: Added
- Developer plugin to help test various scenarios including events like network outages and database cluster failover. This plugin is NOT intended to be used in production environments and is only for testing ([PR #531](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/531)).
- Documentation:
  - Developer plugin. See [UsingTheJdbcDriver](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#list-of-available-plugins) and [UsingTheDeveloperPlugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheDeveloperPlugin.md).
  - MySQL code samples ([PR #532](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/532)).
  - Add a Table of Contents section for the sample codes on README.md. See [README.md](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/README.md#examples).
  - Sample tutorial and code example for Vert.x. See the [tutorial](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/examples/VertxExample/README.md) and [code example](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/examples/VertxExample/src/main/java/com/example/starter/MainVerticle.java).
  - Added Properties section on the README listing all the driver properties and where they are used. See the [README.md](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/README.md#properties).

## [2.2.2] - 2023-07-05
### :magic_wand: Added
- Official support for Amazon Aurora with MySQL compatibility. The AWS JDBC Driver has been validated to support [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j) and [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j).
- Documentation:
  - Maintenance and release policy ([PR #442](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/442) and [PR #507](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/507)).
  - Migration guide for moving from the AWS JDBC Driver for MySQL to the AWS JDBC Driver ([PR #510](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/510)).

### :crab: Changed
- Improved integration test suite performance by creating required test database clusters in advance ([PR #411](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/411)).
- Documentation:
  - Correct `portNumber` to `serverPort` in Hikari example ([PR #504](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/504)).
  - Updated Maven Central links and references to third party framework examples ([PR #499](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/499)).

## [2.2.1] - 2023-6-16

### :bug: Fixed
- Move the Spring Wildfly example `gradle.properties` file to `examples/SpringWildflyExample/spring` ([Issue #491](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues/491)).

## [2.2.0] - 2023-6-14
### :magic_wand: Added
- Autoscaling and the least connections strategy ([PR #451](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/451)).
- [Target driver dialects](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/TargetDriverDialects.md) ([PR #452](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/452)).
- Elastic Load Balancer URL support ([PR #476](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/476)).
- Documentation:
  - Using the Driver with plain RDS Databases. See [Using the Driver](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#using-the-aws-jdbc-driver-with-plain-rds-databases).
  - Internal connection pool behaviour only verifying password on initial connection. See [Using the Read Write Splitting Plugin Internal Connection Pooling document](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) and [code example](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/examples/AWSDriverExample/src/main/java/software/amazon/InternalConnectionPoolPasswordWarning.java).
  - Link performance test in table of contents. See [Documentation Table of Contents](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/Documentation.md).
  - Cluster URLs are not internally pooled. See [Using Read Write Splitting Plugin Internal Connection Pooling](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling).
  - The `leastConnections` strategy in the [Using Read Write Splitting Plugin Internal Connection Pooling](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) at point 3.
- Sample code and tutorial for using the driver with:
  - [Spring and Hibernate](./examples/SpringHibernateExample/README.md)
  - [Spring and Wildfly](./examples/SpringWildflyExample/README.md)
  - [Spring and Hikari](./examples/SpringBootHikariExample/README.md)

### :bug: Fixed
- Pruned null connections in connection tracker plugins ([PR #461](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/461)).
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
- Optimized thread locks and expiring cache for the Enhanced Monitoring Plugin ([PR #365](https://github.com/awslabs/aws-advanced-jdbc-wrapper/pull/365)).
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
- Support for PostgreSQL
- The [Failover Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md)
- The [Host Monitoring Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md)
- The [AWS IAM Authentication Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md)
- The [AWS Secrets Manager Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheAwsSecretsManagerPlugin.md)

[2.3.5]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.3.4...2.3.5
[2.3.4]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.3.3...2.3.4
[2.3.3]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.3.2...2.3.3
[2.3.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.3.1...2.3.2
[2.3.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.3.0...2.3.1
[2.3.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.5...2.3.0
[2.2.5]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.4...2.2.5
[2.2.4]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.3...2.2.4
[2.2.3]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.2...2.2.3
[2.2.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.1...2.2.2
[2.2.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.2.0...2.2.1
[2.2.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.2...2.2.0
[2.1.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.1...2.1.2
[2.1.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/2.0.0...2.1.0
[2.0.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.2...2.0.0
[1.0.2]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.0
