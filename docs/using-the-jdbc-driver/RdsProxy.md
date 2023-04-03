# RDS Proxy FAQ

## What is Amazon RDS Proxy

Amazon RDS Proxy is a fully managed, highly available database proxy for Amazon Relational Database Service (RDS).
Similar to the AWS Advanced JDBC Driver, Amazon RDS Proxy also supports failover and is able to route requests to new
database instances to reduce downtime.

For more information, see the [[Amazon RDS Proxy FAQs](https://aws.amazon.com/rds/proxy/faqs/)].

## What is the difference between Amazon RDS Proxy and AWS Advanced JDBC Driver

| Feature                        | AWS Advanced JDBC Driver                                                                                | RDS Proxy                                                                                                                                                                                                                                                                                                                                              |
|--------------------------------|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Authentication                 | Supports native authentication, IAM authentication, and AWS Secrets Manager                             | Supports native authentication, IAM authentication, and AWS Secrets Manager.                                                                                                                                                                                                                                                                           |
| Connection Pooling             | Requires third-party connection pools, such as HikariCP                                                 | Supports third-party connection pooling but is also capable of handling the infrastructure to perform connection pooling for the writer instance of the associated RDS or Aurora database.                                                                                                                                                             |
| Failover Support               | Supports Aurora MySQL and PostgreSQL database engines                                                   | Supports all Aurora database engines as well as the RDS database engines, for more information see the supported list of engines [here](https://aws.amazon.com/rds/proxy/faqs/#:~:text=Q%3A%20Which%20database%20engines%20does%20RDS%20Proxy%20support%3F).                                                                                           |
| Transaction Boundary Detection | The AWS Advanced JDBC Wrapper uses keywords such as COMMIT or ROLLBACK in the SQL statements            | RDS Proxy detects when a transaction ends through the network protocol used by the database client application. For more information, see RDS Proxy's [Transactions page](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.howitworks.html#rds-proxy-transactions).                                                                    |
| Security                       | The AWS Advanced JDBC Wrapper follows the same security mechanisms supported by the underlying drivers. | RDS Proxy supports TLS protocol version 1.0, 1.1, and 1.2 for sessions between your client and the RDS Proxy endpoint. To use SSL connections from the RDS Proxy to the database, [you need to set the SSL session variables on the client side](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.howitworks.html#rds-proxy-security). |

## Can the AWS RDS Proxy be used with Host Monitoring Connection Plugin

We recommend you either disable the Host Monitoring Connection Plugin or avoid using RDS Proxy endpoints when the Host
Monitoring Connection Plugin is active. For more information, see
the [Host Monitoring Connection Plugin documentation](https://github.com/awslabs/aws-advanced-jdbc-wrapper/wiki/UsingTheHostMonitoringPlugin#warning-warnings-about-usage-of-the-aws-advanced-jdbc-driver-with-rds-proxy).