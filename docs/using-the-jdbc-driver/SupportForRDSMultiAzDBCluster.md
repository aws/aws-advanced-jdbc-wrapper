# Enhanced Support for Amazon RDS Multi-AZ DB Cluster

As of [v2.3.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.0), the AWS JDBC Driver has expanded its support for Amazon RDS Multi-AZ DB Cluster Deployment. By leveraging the topology information within the RDS Multi-AZ DB Cluster, the driver is capable of switching over the connection to a new writer node in approximately 1 second or less, given there is no replica lag during minor version upgrades or OS maintenance upgrades.

## General Usage

The process of using the AWS JDBC Driver with RDS Multi-AZ DB Cluster is the same as using it with an RDS Aurora cluster. All properties, configurations, functions, etc., remain consistent. Instead of connecting to a generic database endpoint, simply replace the endpoint with the Cluster Writer Endpoint provided by the RDS Multi-AZ DB Cluster.

### MySQL

To prepare the connection:

```java
Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:mysql://cluster-writer-endpoint[:port]/database", props);
```

### PostgreSQL

The topology information is populated in Amazon RDS for PostgreSQL versions 13.12, 14.9, 15.4, or higher, starting from revision R2. Ensure you have a supported PostgreSQL version deployed.

Per AWS documentation and [this blog post](https://aws.amazon.com/blogs/database/achieve-one-second-or-less-downtime-with-the-advanced-jdbc-wrapper-driver-when-upgrading-amazon-rds-multi-az-db-clusters/), the `rds_tools` extension must be manually installed using the following DDL before the topology information becomes available:

```sql
CREATE EXTENSION rds_tools;
```

Then, prepare the connection with:

```java
Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://cluster-writer-endpoint[:port]/database", props);
```


## Optimizing Switchover Time

During minor version upgrades of RDS Multi-AZ DB clusters, the `failover` plugin switches the connection from the current writer to a newly upgraded reader. If minimizing downtime during switchover is critical to your application, consider adjusting the `failoverClusterTopologyRefreshRateMs` to a lower value such as 100ms, from the default 2000ms. However, be aware that this can potentially increase the workload on the database during the switchover.

For more details on the `failover` plugin configuration, refer to the [Failover Configuration Guide](/docs/using-the-jdbc-driver/FailoverConfigurationGuide.md).

## Examples

We have created many examples in the [examples](/examples) folder demonstrating how to use the driver.

For additional information, you may also refer to [this AWS blog post](https://aws.amazon.com/blogs/database/achieve-one-second-or-less-downtime-with-the-advanced-jdbc-wrapper-driver-when-upgrading-amazon-rds-multi-az-db-clusters/).

## Limitations

The following plugins have been tested and confirmed to work with Amazon RDS Multi-AZ DB Clusters:

* auroraConnectionTracker
* failover
* efm

The compatibility of other plugins has not been tested at this time. They may function as expected or potentially result in unhandled behavior.
Use at your own discretion.
