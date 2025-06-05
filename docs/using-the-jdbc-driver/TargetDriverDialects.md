# Target Driver Dialects

## What are target driver dialects?
The AWS Advanced JDBC Driver is a wrapper that requires an underlying driver, and it is meant to be compatible with any JDBC driver. Target driver dialects help the AWS JDBC Driver to properly pass JDBC call to a target driver. To function correctly, the AWS JDBC Driver requires details unique to specific target driver such as DataSource method to set the current host or connection url, whether to include some specific configuration parameter to a list of properties or to include it in connection url, etc. These details can be defined and provided to the AWS JDBC Driver by using target driver dialects.

By default, target driver dialect is determined based on used target driver class name or a DataSource class name. 

## Configuration Parameters
| Name             | Required             | Description                                                                                                 | Example                                       |
|------------------|----------------------|-------------------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `wrapperTargetDriverDialect` | No (see notes below) | The [target driver dialect code](#list-of-available-target-driver-codes) of the desired target driver. | `TargetDriverDialectCodes.PG_JDBC` or `pgjdbc` |

> **NOTES:**
>
> The `wrapperTargetDriverDialect` parameter is not required. When it is not provided by the user, the AWS JDBC Driver will attempt to determine which of the existing target driver dialects to use based on target driver class name or a DataSource class name. If target driver specific implementation is not found, the AWS JDBC Driver will use a generic target driver dialect.
> 
### List of Available Target Driver Codes
Target Driver Dialect codes specify what target driver dialect class to use. 

| Dialect Code Reference      | Value                   | Target driver or DataSource class names                                                                                                                   |
|-----------------------------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `PG_JDBC`                   | `pgjdbc`                | org.postgresql.Driver,<br>org.postgresql.ds.PGSimpleDataSource,<br>org.postgresql.ds.PGPoolingDataSource,<br>org.postgresql.ds.PGConnectionPoolDataSource |
| `MYSQL_CONNECTOR_J`         | `mysql-connector-j`     | com.mysql.cj.jdbc.Driver,<br>com.mysql.cj.jdbc.MysqlDataSource,<br>com.mysql.cj.jdbc.MysqlConnectionPoolDataSource                                        |
| `MARIADB_CONNECTOR_J_VER_3` | `mariadb-connector-j-3` | org.mariadb.jdbc.Driver (ver. 3+),<br>org.mariadb.jdbc.MariaDbDataSource,<br>org.mariadb.jdbc.MariaDbPoolDataSource                                       |
| `GENERIC`                   | `generic`               | Any other JDBC driver                                                                                                                                     |

## Custom Target Driver Dialects
If you are interested in using the AWS JDBC Driver but your desired target driver has unique features so the existing generic dialect doesn't work well with it, it is possible to create a custom target driver dialect.

To create a custom target driver dialect, implement the [`TargetDriverDialect`](../../wrapper/src/main/java/software/amazon/jdbc/targetdriverdialect/TargetDriverDialect.java) interface. See the following classes for examples:

- [PgTargetDriverDialect](../../wrapper/src/main/java/software/amazon/jdbc/targetdriverdialect/PgTargetDriverDialect.java)
    - This is a dialect that should work with [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc).
- [MysqlConnectorJTargetDriverDialect](../../wrapper/src/main/java/software/amazon/jdbc/targetdriverdialect/MysqlConnectorJTargetDriverDialect.java)
    - This is a dialect that should work with [MySQL Connector/J Driver](https://github.com/mysql/mysql-connector-j).

Once the custom dialect class has been created, tell the AWS JDBC Driver to use it with the `setCustomDialect` method in the `TargetDriverDialectManager` class. It is not necessary to set the `wrapperTargetDriverDialect` parameter. See below for an example:

```java
TargetDriverDialect myTargetDriverDialect = new CustomTargetDriverDialect();
TargetDriverDialectManager.setCustomDialect(myTargetDriverDialect);
```
