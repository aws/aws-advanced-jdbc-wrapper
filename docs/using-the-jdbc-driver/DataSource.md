# Connecting with a DataSource
You can use the `DriverManager` class or a datasource to establish a new connection when using the AWS JDBC Driver. The AWS JDBC Driver has a built-in datasource class named [AwsWrapperDataSource](../../wrapper/src/main/java/software/amazon/jdbc/ds/AwsWrapperDataSource.java) that allows the AWS JDBC Driver to work with various driver-specific datasources.

## Using the AwsWrapperDataSource

To establish a connection with the AwsWrapperDataSource, you must:

1. Select a driver-specific datasource depending on what underlying driver is being used (for example: `org.postgresql.ds.PGSimpleDataSource` or `com.mysql.cj.jdbc.MysqlDataSource`).
2. Set up basic connection information in the AwsWrapperDataSource. See [this table](#configurable-datasource-properties) for the available options.
3. Configure any needed driver-specific datasource in the AwsWrapperDataSource using the [target dataSource properties](#configurable-datasource-properties).
4. Configure any needed AWS JDBC Driver properties in the AwsWrapperDataSource using the [target dataSource properties](#configurable-datasource-properties).

### Configurable DataSource Properties

See the table below for a list of configurable properties.

> **:warning: Note:** If the same connection property is provided both explicitly in the connection URL and in the datasource properties, the value set in the datasource properties will take precedence. 

| Property                     | Configuration Method            | Description                                                                                                                                                | Type         | Required                                                                                                                                                                                                                          | Example                                                                                                   |
|------------------------------|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| Server name                  | `setServerName`                 | The name of the server.                                                                                                                                    | `String`     | Yes, if no URL is provided.                                                                                                                                                                                                       | `db-server.mydomain.com`                                                                                  |
| Server port                  | `setServerPort`                 | The server port.                                                                                                                                           | `String`     | No                                                                                                                                                                                                                                | `5432`                                                                                                    |
| Database name                | `setDatabase`                   | The name of the database.                                                                                                                                  | `String`     | No                                                                                                                                                                                                                                | `testDatabase`                                                                                            |
| JDBC URL                     | `setJdbcUrl`                    | The URL to connect with.                                                                                                                                   | `String`     | No. Either URL or server name should be set. If both URL and server name have been set, URL will take precedence. Please note that some drivers, such as MariaDb, require some parameters to be included particularly in the URL. | `jdbc:postgresql://localhost/postgres`                                                                    |
| JDBC protocol                | `setJdbcProtocol`               | The JDBC protocol that will be used.                                                                                                                       | `String`     | Yes, if the JDBC URL has not been set.                                                                                                                                                                                            | `jdbc:postgresql:`                                                                                        |
| Underlying DataSource class  | `setTargetDataSourceClassName`  | The fully qualified class name of the underlying DataSource class the AWS JDBC Driver should use.                                                          | `String`     | Yes, if the JDBC URL has not been set.                                                                                                                                                                                            | `org.postgresql.ds.PGSimpleDataSource`                                                                    |
| Target DataSource Properties | `setTargetDataSourceProperties` | Any additional properties that are required. This includes properties specific to the current underlying driver as well as any AWS JDBC Driver properties. | `Properties` | No                                                                                                                                                                                                                                | See this [example](../../examples/AWSDriverExample/src/main/java/software/amazon/DatasourceExample.java). | 

## Using the AwsWrapperDataSource with Connection Pooling Frameworks

The JDBC Wrapper also supports establishing a connection with a connection pooling framework.

To use the AWS JDBC Driver with a connection pool, you must:

1. Configure the connection pool.
2. Set the datasource class name to `software.amazon.jdbc.ds.AwsWrapperDataSource` for the connection pool.
3. Configure the `AwsWrapperDataSource`.
4. Configure the driver-specific datasource.

### HikariCP Pooling Example

[HikariCP](https://github.com/brettwooldridge/HikariCP) is a popular connection pool; the steps that follow configure a simple connection pool with HikariCP and PostgreSQL:

1. Configure the HikariCP datasource:
   ```java
   HikariDataSource ds = new HikariDataSource();
   
   // Configure the connection pool:
   ds.setMaximumPoolSize(5);
   ds.setIdleTimeout(60000);
   ds.setUsername(USER);
   ds.setPassword(PASSWORD);
   ```

2. Set the datasource class name to `software.amazon.jdbc.ds.AwsWrapperDataSource`:
   ```java
   ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());
   ```

3. Configure the `AwsWrapperDataSource`:
   ```java
   // Note: jdbcProtocol is required when connecting via server name
   ds.addDataSourceProperty("jdbcProtocol", "jdbc:postgresql:");
   ds.addDataSourceProperty("serverName", "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
   ds.addDataSourceProperty("serverPort", "5432");
   ds.addDataSourceProperty("database", "postgres");
   
   // Alternatively, the AwsWrapperDataSource can be configured with a JDBC URL instead of individual properties as seen above.
   ds.addDataSourceProperty("jdbcUrl", "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres");
   ```

4. Set the driver-specific datasource:
   ```java
   ds.addDataSourceProperty("targetDataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
    ```

5. Configure the driver-specific datasource and any AWS JDBC Driver properties, if needed. This step is optional:
   ```java
   Properties targetDataSourceProps = new Properties();
   targetDataSourceProps.setProperty("socketTimeout", "10");
   targetDataSourceProps.setProperty("wrapperLoggerLevel", "ALL");
   ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);
   ```

> [!WARNING]\
> HikariCP supports either DataSource-based configuration or DriverManager-based configuration by specifying the `dataSourceClassName` or the `jdbcUrl`. When using the `AwsWrapperDataSource` you must specify the `dataSourceClassName`, and the  `HikariDataSource.setJdbcUrl` method should not be used. For more information see HikariCP's [documentation](https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby).

> [!NOTE]\
> When using HikariCP with the wrapper, you may see log messages from HikariCP about unable to instantiate their `PropertyElf` class:
> 
> `com.zaxxer.hikari.util.PropertyElf - Class "{wrapperLogLevel=ALL, defaultRowFetchSize=10000, wrapperPlugins=failover,efm2}" not found or could not instantiate it`
> 
> The wrapper does not rely on Hikari's PropertyElf to set properties on the target DataSource object, so these messages can be ignored.

### Examples
See [here](../../examples/AWSDriverExample/src/main/java/software/amazon/DatasourceExample.java) for a simple AWS Driver Datasource example.

See [here](../../examples/HikariExample/src/main/java/software/amazon/HikariExample.java) for a complete Hikari example.
