# What is Failover?

In AWS database clusters, **failover** is a mechanism that automatically repairs cluster availability when a primary
database instance becomes unavailable. This process ensures minimal downtime by promoting a replica instance to become
the new primary.

## Aurora Clusters

In an Amazon Aurora database cluster, failover works by electing an Aurora Replica to become the new primary DB
instance, ensuring the cluster maintains maximum availability for read-write operations.

## RDS Multi-AZ DB Clusters

In Amazon RDS Multi-AZ DB clusters, failover automatically promotes one of the two readable standby instances to become
the new primary when the original primary fails.

# Additional Resources

- [Failing over an Amazon Aurora DB cluster](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-failover.html)
- [RDS Multi-AZ DB clusters](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)
