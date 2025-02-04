# Database Dialects

## What are database dialects?
The AWS Advanced JDBC Driver is a wrapper that requires an underlying driver, and it is meant to be compatible with any JDBC driver. Database dialects help the AWS JDBC Driver determine what kind of underlying database is being used. To function correctly, the AWS JDBC Driver requires details unique to specific databases such as the default port number or the method to get the current host from the database. These details can be defined and provided to the AWS JDBC Driver by using database dialects. 

## Configuration Parameters
| Name             | Required             | Description                                                                        | Example                                       |
|------------------|----------------------|------------------------------------------------------------------------------------|-----------------------------------------------|
| `wrapperDialect` | No (see notes below) | The [dialect code](#list-of-available-dialect-codes) of the desired database type. | `DialectCodes.AURORA_MYSQL` or `aurora-mysql` |

> **NOTES:** 
> 
> The `wrapperDialect` parameter is not required. When it is not provided by the user, the AWS JDBC Driver will attempt to determine which of the existing dialects to use based on other connection details. However, if the dialect is known by the user, it is preferable to set the `wrapperDialect` parameter because it will take time to resolve the dialect.

### List of Available Dialect Codes
Dialect codes specify what kind of database any connections will be made to.

| Dialect Code Reference       | Value                        | Database                                                                                                                                           |
| ---------------------------- | ---------------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------|
| `AURORA_MYSQL`               | `aurora-mysql`               | [Aurora MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                        |
| `GLOBAL_AURORA_MYSQL`               | `global-aurora-mysql`               | [Aurora Global Database MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)           |
| `RDS_MULTI_AZ_MYSQL_CLUSTER` | `rds-multi-az-mysql-cluster` | [Amazon RDS MySQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)      |
| `RDS_MYSQL`                  | `rds-mysql`                  | Amazon RDS MySQL                                                                                                                                   |
| `MYSQL`                      | `mysql`                      | MySQL                                                                                                                                              |
| `AURORA_PG`                  | `aurora-pg`                  | [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                                                                                                              |
| `GLOBAL_AURORA_PG`                  | `global-aurora-pg`                  | [Aurora Global Database PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)      |
| `RDS_MULTI_AZ_PG_CLUSTER`    | `rds-multi-az-pg-cluster`    | [Amazon RDS PostgreSQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) |
| `RDS_PG`                     | `rds-pg`                     | Amazon RDS PostgreSQL                                                                                                                              |
| `PG`                         | `pg`                         | PostgreSQL                                                                                                                                         |
| `MARIADB`                    | `mariadb`                    | MariaDB                                                                                                                                            |
| `CUSTOM`                     | `custom`                     | See [custom dialects](#custom-dialects). This code is not required when using custom dialects.                                                     |
| `UNKNOWN`                    | `unknown`                    | Unknown. Although this code is available, do not use it as it will result in errors.                                                               |

## Custom Dialects
If you are interested in using the AWS JDBC Driver but your desired database type is not currently supported, it is possible to create a custom dialect.

To create a custom dialect, implement the [`Dialect`](/wrapper/src/main/java/software/amazon/jdbc/dialect/Dialect.java) interface. For databases clusters that are aware of their topology, the [`TopologyAwareDatabaseCluster`](/wrapper/src/main/java/software/amazon/jdbc/dialect/TopologyAwareDatabaseCluster.java) interface should also be implemented. See the following classes for examples:

- [PgDialect](/wrapper/src/main/java/software/amazon/jdbc/dialect/PgDialect.java)
  - This is a generic dialect that should work with any PostgreSQL database.
- [AuroraPgDialect](/wrapper/src/main/java/software/amazon/jdbc/dialect/AuroraPgDialect.java)
  - This dialect is an extension of PgDialect, but also implements the `TopologyAwareDatabaseCluster` interface.

Once the custom dialect class has been created, tell the AWS JDBC Driver to use it with the `setCustomDialect` method in the `DialectManager` class. It is not necessary to set the `wrawpperDialect` parameter. See below for an example:

```java
Dialect myDialect = new CustomDialect();
DialectManager.setCustomDialect(myDialect);
```
