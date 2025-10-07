# ShardingSphere HikariCP Example with AWS Advanced JDBC Wrapper

[Apache ShardingSphere is an ecosystem created to enhance existing databases with features like data sharding, elastic scaling and many more](https://shardingsphere.apache.org/document/current/en/overview/#introduction).

This example demonstrates how to configure Apache ShardingSphere with HikariCP and the AWS Advanced JDBC Wrapper by going over the following sections:
1. Custom Database Type
2. ShardingSphere and Hikari Configuration
3. Failover Handling

## Custom Database Type

The ShardingSphere JDBC framework currently does not support the AWS Advanced JDBC Wrapper's JDBC URL prefix `jdbc:aws-wrapper:mysql`. To use the Wrapper with the framework, user applications must [register a new DatabaseType](https://github.com/apache/shardingsphere/issues/31618#issuecomment-2154732320), following the examples shown in these steps:

1. Create a custom `DatabaseType` in your application, in this example, we named it `WrapperType`.
    ```java
    import java.util.Collection;
    import java.util.Collections;
    import java.util.Optional;
    import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
    import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
    
    public final class WrapperType implements DatabaseType {
    
      @Override
      public Collection getJdbcUrlPrefixes() {
        return Collections.singleton("jdbc:aws-wrapper:mysql");
      }
    
      @Override
      public Optional getTrunkDatabaseType() {
        return Optional.of(TypedSPILoader.getService(DatabaseType.class, "MySQL"));
      }
    
      @Override
      public String getType() {
        return "Aurora MySQL";
      }
    }
    ```
2. Register the new `DatabaseType` with SPI by creating a file called `org.apache.shardingsphere.infra.database.core.type.DatabaseType` under `src/main/resources/META-INF/services`.
3. The content of the file should be the fully qualified classpath of the custom `DatabaseType`. In this example, it will be `software.amazon.WrapperType`

## ShardingSphere and Hikari Configuration

This example uses [HikariCP](https://github.com/brettwooldridge/HikariCP) to help manage database connections. The AWS Advanced JDBC Wrapper's failover plugin throws failover-related exceptions that need to be handled explicitly by HikariCP, otherwise connections will be closed immediately after failover.
To handle these exceptions, user application needs to configure a custom exception override class in the ShardingSphere datasource configuration:
```yaml
dataSources:
  ds0:
    dataSourceClassName: com.zaxxer.hikari.HikariDataSource
    driverClassName: software.amazon.jdbc.Driver
    jdbcUrl: jdbc:aws-wrapper:mysql://database-mysql.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/shard0
    username: user
    password: pass
    exceptionOverrideClassName: software.amazon.jdbc.util.HikariCPSQLException
    dataSourceProperties:
      wrapperPlugins: failover2
```

## Running the Example

```bash
./gradlew :ShardingSphereHikariExample:run
```
