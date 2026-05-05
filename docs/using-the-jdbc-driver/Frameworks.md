# The AWS Advanced JDBC Wrapper Integration with 3rd Party Frameworks

The AWS Advanced JDBC Wrapper works with multiple 3rd party frameworks. 
Here are some examples of configuring the driver to work with these frameworks:
- [Spring and Hibernate](../../examples/SpringHibernateExample/README.md)
- [Spring and Wildfly](../../examples/SpringWildflyExample/README.md)
- [Spring and Hikari](../../examples/SpringBootHikariExample/README.md)

## Open Liberty

[Open Liberty](https://openliberty.io/) is an open-source Jakarta EE and MicroProfile application server from IBM. The AWS Advanced JDBC Wrapper can be used as the JDBC driver within Open Liberty's `dataSource` configuration to provide failover support, authentication, and other AWS database features to applications deployed on Liberty.

### Configuring Liberty with Failover

The AWS Advanced JDBC Wrapper uses the following SQL states for failover exceptions:
- `08S02` — Failover succeeded, the active connection has changed.
- `08007` — Failover occurred during an active transaction, transaction outcome is unknown.
- `08001` — Failover was attempted but failed, no valid connection is available.
- `08003` — Operation attempted on a connection that is no longer open.

Open Liberty natively recognizes `08001`, `08003`, `08006`, and `08S01` as [stale connection codes](https://github.com/OpenLiberty/open-liberty/blob/f37fee4243462adae53338af311c1a234ac3ee39/dev/com.ibm.ws.jdbc/src/com/ibm/ws/rsadapter/impl/DatabaseHelper.java#L166-L174). The SQL states `08S02` and `08007` are not recognized by default and require further configuration.

To ensure the AWS Advanced JDBC Wrapper's failover behavior is handled correctly by the Connection Manager, configure `identifyException` to map the wrapper's failover SQL states as stale connections. This allows Liberty to discard pooled connections and cached statements after a failover event, enabling the application to recover automatically.

Add the following to your `dataSource` configuration in `server.xml` when using the `failover`, `failover2`, or `gdbFailover` plugins:

```xml
<dataSource id="default" jndiName="jdbc/myDS" type="javax.sql.DataSource">
  <identifyException as="StaleConnection" sqlState="08S02"/>
  <identifyException as="StaleConnection" sqlState="08007"/>
  ...
</dataSource>
```

As an alternative, to discard cached statements but not pooled connections, setting `statementCacheSize="0"` on the `dataSource` element disables statement caching entirely and avoids the need for `identifyException` configuration at the cost of re-preparing statements on every use.

### Example DataSource Configuration

Configure the AWS Advanced JDBC Wrapper as a `javax.sql.DataSource` using `AwsWrapperDataSource`. Wrapper connection properties such as `wrapperPlugins`, `wrapperDialect`, and [`clusterId`](ClusterId.md) are passed through `targetDataSourceProperties`. See [Connecting with a DataSource](DataSource.md) for more details.

```xml
<library id="aws-advanced-jdbc-wrapper">
    <fileset dir="/config/aws-advanced-jdbc-wrapper" includes="*.jar"/>
</library>

<dataSource id="default" jndiName="jdbc/myDS" type="javax.sql.DataSource">
  <identifyException as="StaleConnection" sqlState="08S02"/>
  <identifyException as="StaleConnection" sqlState="08007"/>
  <jdbcDriver libraryRef="aws-advanced-jdbc-wrapper"
              javax.sql.DataSource="software.amazon.jdbc.ds.AwsWrapperDataSource"/>
  <properties
    serverName="db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com"
    serverPort="5432"
    user="username"
    password="password"
    database="employees"
    jdbcProtocol="jdbc:postgresql:"
    targetDataSourceClassName="org.postgresql.ds.PGSimpleDataSource"
    targetDataSourceProperties="wrapperPlugins: auroraConnectionTracker,efm2,failover2; wrapperDialect: aurora-pg; clusterId: my-cluster" />
</dataSource>
```
