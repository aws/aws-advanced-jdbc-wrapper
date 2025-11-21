# Enhanced Support for Amazon RDS Multi-AZ DB Cluster

As of [v2.3.0](https://github.com/aws/aws-advanced-jdbc-wrapper/releases/tag/2.3.0), the AWS JDBC Driver has expanded its support for Amazon RDS Multi-AZ DB Cluster Deployment. By leveraging the topology information within the RDS Multi-AZ DB Cluster, the driver is capable of switching over the connection to a new writer node in approximately 1 second or less, given there is no replica lag during minor version upgrades or OS maintenance upgrades.

## General Usage

The process of using the AWS JDBC Driver with RDS Multi-AZ DB Cluster is the same as using it with an RDS Aurora cluster. All properties, configurations, functions, etc., remain consistent. Instead of connecting to a generic database endpoint, simply replace the endpoint with the Cluster Writer Endpoint provided by the RDS Multi-AZ DB Cluster.

### MySQL

There are permissions that must be granted to all non-administrative users who need database access. Without proper access, these users cannot utilize many of the driver's advanced features, including failover support. To grant the necessary permissions to non-administrative users, execute the following statement:

```sql
GRANT SELECT ON mysql.rds_topology TO 'non-admin-username'@'%'
```

Preparing a connection with MySQL in a Multi-AZ Cluster remains the same as before:

```java
Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:mysql://cluster-writer-endpoint[:port]/database", props);
```

### PostgreSQL

The topology information is populated in Amazon RDS for PostgreSQL versions 13.12, 14.9, 15.4, or higher, starting from revision R3. Ensure you have a supported PostgreSQL version deployed.

Per AWS documentation and [this blog post](https://aws.amazon.com/blogs/database/achieve-one-second-or-less-downtime-with-the-advanced-jdbc-wrapper-driver-when-upgrading-amazon-rds-multi-az-db-clusters/), the `rds_tools` extension must be manually installed using the following DDL before the topology information becomes available on target cluster:

```sql
CREATE EXTENSION rds_tools;
```

The extension must be granted to all non-administrative users who need database access. Without access to `rds_tools`, non-admin users cannot utilize many of the driver's advanced features, including failover support. To grant the necessary permissions to non-administrative users, execute the following statement:

```sql
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO non-admin-username;
```

Then, prepare the connection with:

```java
Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://cluster-writer-endpoint[:port]/database", props);
```

## Optimizing Switchover Time

Amazon RDS Multi-AZ with two readable standbys now supports minor version upgrades with 1 second of downtime.

See feature announcement [here](https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-rds-multi-az-two-stanbys-upgrades-downtime/).

During minor version upgrades of RDS Multi-AZ DB clusters, the `failover2` plugin switches the connection from the current writer to a newly upgraded reader. If minimizing downtime during switchover is critical to your application, consider adjusting the `failoverClusterTopologyRefreshRateMs` to a lower value such as 100ms, from the default 2000ms. However, be aware that this can potentially increase the workload on the database during the switchover.

For more details on the `failover2` plugin configuration, refer to the [Failover Configuration Guide](./FailoverConfigurationGuide.md).

## Examples

We have created many examples in the 'examples' folder demonstrating how to use the driver.

For additional information, you may also refer to [this AWS blog post](https://aws.amazon.com/blogs/database/achieve-one-second-or-less-downtime-with-the-advanced-jdbc-wrapper-driver-when-upgrading-amazon-rds-multi-az-db-clusters/).

## Limitations

The following plugins have been tested and confirmed to work with Amazon RDS Multi-AZ DB Clusters:

* [Aurora Connection Tracker Plugin](./using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)
* [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)
* [Failover Connection Plugin v2](./using-plugins/UsingTheFailover2Plugin.md)
* [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)

The compatibility of other plugins has not been tested at this time. They may function as expected or potentially result in unhandled behavior.
Use at your own discretion.
