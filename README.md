# Amazon Web Services (AWS) JDBC Driver

[![build_status](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/software.amazon.jdbc/aws-advanced-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/software.amazon.jdbc/aws-advanced-jdbc-wrapper)
[![Qodana](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/code_quality.yml)

The **Amazon Web Services JDBC Driver** has been redesigned as an advanced JDBC wrapper. This wrapper is complementary to and extends the functionality of an existing JDBC driver to help an application take advantage of the features of clustered databases such as Amazon Aurora. The AWS JDBC Driver does not implement connectivity on its own to any database, but will enable support of AWS and Aurora functionalities on top of an underlying JDBC driver of the user's choice.

The AWS JDBC Driver is meant to work with any JDBC driver. Currently, the AWS JDBC Driver has been validated to support the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc). Support for the [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j) and the [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j) is being evaluated.

In conjunction with the PostgreSQL JDBC Driver, the AWS JDBC Driver enables fast failover for Amazon Aurora with PostgreSQL compatibility. It also supports integration with [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) and [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/). We are investigating Read/Write splitting for a future release.

## About the Driver

### What is Failover?
In an Amazon Aurora database (DB) cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

### Benefits of the AWS JDBC Driver
Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver allows customers to continue using their existing community drivers in addition to having the AWS JDBC Driver fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to augment existing JDBC community drivers and be a unified connector to JDBC workflows for Aurora.

#### Enhanced Failure Monitoring
Enhanced Failure Monitoring (EFM) is a feature available from the [Host Monitoring Connection Plugin](./docs/using-the-jdbc-driver/using-plugins/UsingTheHostMonitoringPlugin.md#enhanced-failure-monitoring) that periodically checks the connected database node's health and availability. If a database node is determined to be unhealthy, the connection is aborted.

## Getting Started
For more information on how to obtain the AWS JDBC Driver, minimum requirements to use it, and how to integrate the AWS JDBC Driver into your project, please visit the [Getting Started page](./docs/GettingStarted.md).

## Using the AWS JDBC Driver
Please refer to the AWS JDBC Driver's [Documentation page](./docs/Documentation.md) for details about using the AWS JDBC Driver. 

## Logging
To configure logging, check out the [Logging](./docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#logging) section.

## Documentation
Technical documentation regarding the functionality of the AWS JDBC Driver will be maintained in this GitHub repository. Since the AWS JDBC Driver requires an underlying JDBC driver, please refer to the individual driver's documentation for driver specific information.

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

## License
This software is released under the Apache 2.0 license.
