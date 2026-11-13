# Database Dialects

## What are database dialects?
The AWS Advanced JDBC Wrapper is a wrapper that requires an underlying driver, and it is meant to be compatible with any JDBC driver. Database dialects help the AWS Advanced JDBC Wrapper determine what kind of underlying database is being used. To function correctly, the AWS Advanced JDBC Wrapper requires details unique to specific databases such as the default port number or the method to get the current host from the database. These details can be defined and provided to the AWS Advanced JDBC Wrapper by using database dialects. 

## Configuration Parameters
| Name             | Required             | Description                                                                        | Example                                       |
|------------------|----------------------|------------------------------------------------------------------------------------|-----------------------------------------------|
| `wrapperDialect` | No (see notes below) | The [dialect code](#list-of-available-dialect-codes) of the desired database type. | `DialectCodes.AURORA_MYSQL` or `aurora-mysql` |

> **NOTES:** 
> 
> The `wrapperDialect` parameter is not required. When it is not provided by the user, the AWS Advanced JDBC Wrapper will attempt to determine which of the existing dialects to use based on other connection details. However, if the dialect is known by the user, it is preferable to set the `wrapperDialect` parameter because it will take time to resolve the dialect.

### How the dialect is detected

When `wrapperDialect` is not set, detection happens in two steps.

1. Before connecting, the wrapper picks a provisional dialect from the driver protocol and the shape of the hostname. Aurora Global Database writer endpoints (`.global-`) and Aurora Limitless endpoints (`.shardgrp-`) are definitive; every other hostname only narrows the choice to an engine family. The provisional dialect supplies the default port and connection properties.
2. On the first connection, the wrapper collects a fingerprint of the server with one catalog statement per engine family (`pg_catalog` for PostgreSQL; `SHOW VARIABLES` and `information_schema` for MySQL and MariaDB), plus at most one follow-up for Aurora Global Database (region count) or an RDS Multi-AZ DB cluster. The fingerprint, not the hostname or the protocol, decides the dialect, so an Aurora cluster behind a custom domain, an RDS Proxy, or an instance endpoint resolves to the same dialect as through its cluster endpoint. Every statement is valid on any server of its family, so detection does not raise errors on the server.

The resolved dialect is cached per host for 24 hours, so later connections to the same host skip step 2. The fingerprint and the resolved dialect are logged at `FINEST` by `software.amazon.jdbc.dialect.DialectManager`.

### List of Available Dialect Codes
Dialect codes specify what kind of database any connections will be made to.

| Dialect Code Reference       | Value                        | Database                                                                                                                                           |
|------------------------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `AURORA_MYSQL`               | `aurora-mysql`               | [Aurora MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                        |
| `GLOBAL_AURORA_MYSQL`        | `global-aurora-mysql`        | [Aurora Global Database MySQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)           |
| `RDS_MULTI_AZ_MYSQL_CLUSTER` | `rds-multi-az-mysql-cluster` | [Amazon RDS MySQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)      |
| `RDS_MYSQL`                  | `rds-mysql`                  | Amazon RDS MySQL                                                                                                                                   |
| `MYSQL`                      | `mysql`                      | MySQL                                                                                                                                              |
| `AURORA_PG`                  | `aurora-pg`                  | [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_GettingStartedAurora.html)                                   |
| `GLOBAL_AURORA_PG`           | `global-aurora-pg`           | [Aurora Global Database PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-getting-started.html)      |
| `RDS_MULTI_AZ_PG_CLUSTER`    | `rds-multi-az-pg-cluster`    | [Amazon RDS PostgreSQL Multi-AZ DB Cluster Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) |
| `RDS_PG`                     | `rds-pg`                     | Amazon RDS PostgreSQL                                                                                                                              |
| `PG`                         | `pg`                         | PostgreSQL                                                                                                                                         |
| `MARIADB`                    | `mariadb`                    | MariaDB                                                                                                                                            |
| `CUSTOM`                     | `custom`                     | See [custom dialects](#custom-dialects). This code is not required when using custom dialects.                                                     |
| `UNKNOWN`                    | `unknown`                    | Unknown. Although this code is available, do not use it as it will result in errors.                                                               |

## Custom Dialects
If you are interested in using the AWS Advanced JDBC Wrapper but your desired database type is not currently supported, it is possible to create a custom dialect.

To create a custom dialect, implement the [`Dialect`](../../wrapper/src/main/java/software/amazon/jdbc/dialect/Dialect.java) interface. For Aurora Limitless database engines, the [`AuroraLimitlessDialect`](../../wrapper/src/main/java/software/amazon/jdbc/dialect/AuroraLimitlessDialect.java) interface should also be implemented. See the following classes for examples:

- [PgDialect](../../wrapper/src/main/java/software/amazon/jdbc/dialect/PgDialect.java)
  - This is a generic dialect that should work with any PostgreSQL database.
- [AuroraPgDialect](../../wrapper/src/main/java/software/amazon/jdbc/dialect/AuroraPgDialect.java)
  - This dialect is an extension of PgDialect, but also implements the `AuroraLimitlessDialect` interface.

Once the custom dialect class has been created, tell the AWS Advanced JDBC Wrapper to use it with the `setCustomDialect` method in the `DialectManager` class. It is not necessary to set the `wrawpperDialect` parameter. See below for an example:

```java
Dialect myDialect = new CustomDialect();
DialectManager.setCustomDialect(myDialect);
```
