# RDS Proxy FAQ

## What is Amazon RDS Proxy
Amazon RDS Proxy is a fully managed, highly available database proxy for Amazon Relational Database Service (RDS). Similar to the AWS Advanced JDBC Driver, Amazon RDS Proxy also supports failover and is able to route requests to new database instances to reduce downtime.

For more information, see the [[Amazon RDS Proxy FAQs](https://aws.amazon.com/rds/proxy/faqs/)].

## What is the difference between Amazon RDS Proxy and AWS Advanced JDBC Driver
* The RDS Proxy handles the infrastructure to perform connection pooling for the writer instance of the associated RDS or Aurora database. The AWS Advanced JDBC Driver does not implement connection pooling, therefore, user application needs to use a third party connection pool, such as HikariCP.
* The AWS Advanced JDBC Wrapper only supports failover for Aurora database engines, whereas the Amazon RDS Proxy supports failover for all RDS database engines as well as the Aurora database engines.
* The Amazon RDS Proxy detects transaction boundaries differently. The AWS Advanced JDBC Wrapper uses keywords such as COMMIT or ROLLBACK in the SQL statements, while Amazon RDS Proxy uses the network protocol to determine the transaction boundaries.

## When to use an Amazon RDS Proxy vs AWS Advanced JDBC Driver
Below are some scenarios where the AWS Advanced JDBC Driver would be more preferable:
- If you already have a community JDBC driver fully integrated in your workflow, such as the PostgreSQL JDBC driver or the MySQL Connector/J. The AWS Advanced JDBC Driver allows customers to continue using their existing community drivers in addition to having the AWS JDBC Driver fully exploit failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role.
- Using RDS Proxy requires a common VPC between the Aurora DB cluster or RDS DB instance and RDS proxy. The RDS Proxy also requires AWS Secrets Manager to manage the database credentials. While the AWS Advanced JDBC Driver and the Amazon RDS Proxy both support the same sets of authentication methods, the AWS Advanced JDBC Driver does not require AWS Secrets Manager for managing database credentials.

Below are some scenarios where the RDS Proxy would be more preferable:
- The AWS Advanced JDBC Driver currently only supports the PostgreSQL JDBC driver and provides experimental support for MySQL Connector/J and MariaDB Connector/J, whereas the Amazon RDS Proxy enables failover for other RDS engines.
- The Amazon RDS Proxy also maintains a connection pool offering faster connection rates and more open connections, so your application does not need to maintain a connection pool.
- The Amazon RDS Proxy also serves as an extra point of failure to your servers.

## Can the AWS RDS Proxy be used with Host Monitoring Connection Plugin
We recommend you either disable the Host Monitoring Connection Plugin or avoid using RDS Proxy endpoints when the Host Monitoring Connection Plugin is active. For more information, see the [Host Monitoring Connection Plugin documentation](https://github.com/awslabs/aws-advanced-jdbc-wrapper/wiki/UsingTheHostMonitoringPlugin#warning-warnings-about-usage-of-the-aws-advanced-jdbc-driver-with-rds-proxy).