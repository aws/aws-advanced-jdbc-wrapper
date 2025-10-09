# Simple Read/Write Splitting Plugin

The simple read/write splitting plugin adds functionality to switch between endpoints via calls to the `Connection#setReadOnly` method. Based off the values provided in the properties, upon calling `setReadOnly(true)`, the plugin will connect to the given endpoint for read operations. When `setReadOnly(false)` is called, the plugin will connect to the given endpoint for write operations. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the boolean argument you supply to the `setReadOnly` method.

## Loading the Simple Read/Write Splitting Plugin

The simple read/write splitting plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter. If you would like to load the simple read/write splitting plugin alongside the failover and host monitoring plugins, the simple read/write splitting plugin must be listed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the read/write splitting plugin with these plugins.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "srw,failover,efm");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
```

If you would like to use the simple read/write splitting plugin without the failover plugin, make sure you have the `srw` plugin in the `wrapperPlugins` property, and that the failover plugin is not part of it.
```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "srw");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
```

## Federated Authentication Plugin Parameters
| Parameter                 |  Value  | Required | Description                                                                                   | Default Value | Example Value                                                |
|---------------------------|:-------:|:--------:|:----------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------|
| `srwWriteEndpoint`        | String  |   Yes    | The endpoint to connect to when `setReadOnly(false)` is called.                               | `null`        | `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`    |
| `srwReadEndpoint`         | String  |    No    | The endpoint to connect to when `setReadOnly(true)` is called.                                | `null`        | `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com` |
| `verifyNewSrwConnections` | Boolean |    No    | Enables role-verification for new connections made by the simple read/write splitting plugin. | `true`        | `true`, `false`                                              |

### Limitations of `verifyNewSrwConnections`

The property `verifyNewSrwConnections` is enabled by default. This means that when new connections are made with the simple read/write splitting plugin, a query is sent to the new connection to verify its role. If a write connection connects to a reader, an error is thrown. If a read connection connects to the writer, a warning is logged and the connection is not stored for reuse.

#### Non-Aurora clusters
The verification step uses an Aurora specific query to determine the role of the connection, if the endpoint is not to an Aurora database, disable `verifyNewSrwConnections` by setting it to `false` in the properties.

#### Autocommit
If `verifyNewSrwConnections` is enabled and autocommit is set to false when the simple read/write splitting plugin makes a new connection, unexpected errors such as `Cannot change transaction read-only property in the middle of a transaction.` will occur. As part of verification, the plugin executes a query against a new connection, which when autocommit is set to false will open a transaction. 
If autocommit is essential to a workflow, either establish both internal reader and writer connections before setting autocommit to false or disable `verifyNewSrwConnections` by setting it to `false` in the properties.

## Supplying the connection string

When using the simple read/write splitting plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply just the URL for the initial instance to which you're connecting. You must also include either the failover plugin or the Aurora host list plugin in your plugin chain so that the driver knows to query Aurora for its topology. See the section on [loading the simple read/write splitting plugin](#loading-the-simple-readwrite-splitting-plugin) for more info.

## Using the Simple Read/Write Splitting Plugin with RDS Proxy

RDS Proxy provides connection pooling and management that significantly improves application scalability by reducing database connection overhead and enabling thousands of concurrent connections through
connection multiplexing. Connecting exclusively through the proxy endpoint ensures consistent connection management, automatic failover handling, and centralized monitoring, while protecting the underlying database from connection exhaustion
and providing a stable abstraction layer that remains consistent even when database topology changes. By providing the read/write endpoint and a read-only endpoint to the simple read/write splitting plugin, the AWS JDBC Driver will connect using 
these endpoints any time setReadOnly is called. 

To take full advantage of the many benefits of RDS Proxy it is recommended to only connect through the proxy endpoints. Other plugins such as the [Failover Plugin](UsingTheFailoverPlugin.md) connect using instance endpoints.

## Using the Simple Read/Write Splitting Plugin against non-Aurora clusters

The simple read/write splitting plugin can be used to switch between any two endpoints. If the endpoints do not direct to an Aurora database, ensure the property `verifyNewSrwConnections` is set to `false`. See [Limitations of verifyNewSrwConnections](UsingTheSimpleReadWriteSplittingPlugin.md#non-aurora-clusters) for details.

## Limitations

### General plugin limitations

When a Statement or ResultSet is created, it is internally bound to the database connection established at that moment. There is no standard JDBC functionality to change the internal connection used by Statement or ResultSet objects. Consequently, even if the read/write plugin switches the internal connection, any Statements/ResultSets created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after switching between the writer/reader.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

### Failover

Immediately following a failover event, due to DNS caching, an Aurora cluster endpoint may connect to the previous writer, and the read-only endpoint may connect to the new primary instance. To avoid stale DNS connections, as well as errors/warnings from the verification logic we recommend including the [Aurora Initial Connection Strategy Plugin](./UsingTheAuroraInitialConnectionStrategyPlugin.md) in your configuration.

Following failover, endpoints that point to specific instances will be impacted if their target instance was demoted to a reader or promoted to a writer. The simple read/write splitting plugin, always connects to the endpoint provided in the properties when `setReadOnly` is called. We suggest using endpoints that return connections with a specific role such as cluster or read-only endpoints, or using the [ReadWriteSplitting](UsingTheReadWriteSplittingPlugin.md) plugin to connect to instances based on the cluster's current topology.

### Session state

The plugin supports session state transfer when switching connection. All attributes mentioned in [Session State](../SessionState.md) are automatically transferred to a new connection.

## Examples
[SimpleReadWriteSplittingPostgresExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/SimpleReadWriteSplittingPostgresExample.java) and [SimpleReadWriteSplittingMySQLExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/SimpleReadWriteSplittingMySQLExample.java) demonstrate how to enable and configure Simple Read/Write Splitting with the AWS JDBC Driver.
