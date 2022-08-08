# Amazon Web Services (AWS) Advanced JDBC Wrapper

[![build_status](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/aws-advanced-jdbc-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The **Amazon Web Services (AWS) Advanced JDBC Wrapper** allows an application to take advantage of the features of clustered Aurora databases. The AWS Advanced JDBC Wrapper does not implement connectivity to any database but will enable support of AWS and Aurora functionalities on top of an underlying JDBC driver of the user's choice.

The AWS Advanced JDBC Wrapper is meant to work with any JDBC driver. Currently, the JDBC Wrapper has been validated to support the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc). Support for the [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j) and the [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j) will be released in the future.

In conjunction with the PostgreSQL JDBC Driver, the AWS Advanced JDBC Wrapper enables fast failover for Amazon Aurora with PostgreSQL compatibility. Support is planned for additional features such as read/write splitting and AWS Secrets Manager usage.

## About the Wrapper

### What is Failover?
In an Amazon Aurora database (DB) cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS Advanced JDBC Wrapper is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

### Benefits of the AWS Advanced JDBC Wrapper
Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS Advanced JDBC Wrapper allows customers to continue using their existing community drivers in addition to having the JDBC Wrapper fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora DB, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS Advanced JDBC Wrapper can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS Advanced JDBC Wrapper is designed to augment existing JDBC community drivers and be a unified connector to JDBC workflows for Aurora.

## Getting Started
For more information on how to obtain the JDBC Wrapper, minimum requirements to use it, and how to integrate the JDBC Wrapper into your project, please visit the [Getting Started page](./docs/GettingStarted.md).

## Using the Wrapper
Please refer to the JDBC Wrapper's [Documentation page](./docs/Documentation.md) for details about using the JDBC Wrapper. 

## Logging
To configure logging, check out the [Logging](./docs/using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#logging) section.

## Documentation
Technical documentation regarding the functionality of the AWS Advanced JDBC Wrapper will be maintained in this GitHub repository. Since the JDBC Wrapper requires an underlying JDBC Driver, please refer to the individual driver's documentation for driver specific information.

## Getting Help and Opening Issues
If you encounter a bug with the AWS Advanced JDBC Wrapper, we would like to hear about it.
Please search the [existing issues](https://github.com/awslabs/aws-advanced-jdbc-wrapper/issues) to see if others are also experiencing the issue before reporting the problem in a new issue.
GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

When opening a new issue, please include a reproduction case and logs for the issue to help expedite the investigation process.

## How to Contribute
1. Set up your environment by following the directions in the [Development Guide](docs/development-guide/DevelopmentGuide.md)
2. To contribute, first make a fork of this project. 
3. Make any changes on your fork. Make sure you are aware of the requirements for the project (e.g. do not require Java 7 if we are supporting Java 8 and higher).
4. Create a pull request from your fork. 
5. Pull requests need to be approved and merged by maintainers into the master branch. <br />
**Note:** Before making a pull request, [run all tests](./docs/development-guide/DevelopmentGuide.md#running-the-tests) and verify everything is passing.

### Code Style
We enforce a style using [Google checkstyle](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml).
This can be verified in the tests; tests will not pass with checkstyle errors.

## License
This software is released under the Apache 2.0 license.
