# Simple Read/Write Splitting Plugin

The Simple Read/Write Splitting Plugin adds functionality to switch between endpoints via calls to the `Connection#setReadOnly` method. Based on the values provided in the properties, upon calling `setReadOnly(true)`, the plugin will connect to the specified endpoint for read operations. When `setReadOnly(false)` is called, the plugin will connect to the specified endpoint for write operations. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the boolean argument you supply to the `setReadOnly` method.

The plugin will use the current connection, which may be the writer or initial connection, as a fallback if the reader connection is unable to be established, or if connection verification is enabled and the connection is not to a reader host.

The plugin does not rely on cluster topology. It relies purely on the provided endpoints and their DNS resolution.

## Plugin Availability
The plugin is available since version 3.0.0.

## Loading the Simple Read/Write Splitting Plugin

The Simple Read/Write Splitting Plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter. 

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "srw");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
```

## Simple Read/Write Splitting Plugin Parameters
| Parameter                        |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                         | Default Value | Example Values                                               |
|----------------------------------|:-------:|:--------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------|
| `srwWriteEndpoint`               | String  |   Yes    | The endpoint to connect to when `setReadOnly(false)` is called.                                                                                                                                                                                                                                                                     | `null`        | `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`    |
| `srwReadEndpoint`                | String  |   Yes    | The endpoint to connect to when `setReadOnly(true)` is called.                                                                                                                                                                                                                                                                      | `null`        | `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com` |
| `verifyNewSrwConnections`        | Boolean |    No    | Enables writer/reader verification for new connections made by the Simple Read/Write Splitting Plugin. More details below.                                                                                                                                                                                                          | `true`        | `true`, `false`                                              |
| `verifyInitialConnectionType`    | String  |    No    | If `verifyNewSrwConnections` is set to `true`, this parameter will verify the initial opened connection to be either a writer or a reader. More details below.                                                                                                                                                                      | `null`        | `writer`, `reader`                                           |
| `srwConnectRetryTimeoutMs`       | Integer |    No    | If `verifyNewSrwConnections` is set to `true`, this parameter sets the maximum allowed time in milliseconds for retrying connection attempts. More details below.                                                                                                                                                                   | `60000`       | `60000`                                                      |
| `srwConnectRetryIntervalMs`      | Integer |    No    | If `verifyNewSrwConnections` is set to `true`, this parameter sets the time delay in milliseconds between each retry of opening a connection. More details below.                                                                                                                                                                   | `1000`        | `1000`                                                       |
| `cachedReaderKeepAliveTimeoutMs` | Integer |    No    | Timeout value for the cached reader connection. Once the reader has expired, the next call to `setReadOnly(true)` will create a new reader connection using the `srwReadEndpoint`. The default value of 0 means the wrapper will keep reusing the same cached reader connection for the entire lifetime of the `Connection` object. | `0`           | `600000`                                                     |

## How the Simple Read/Write Splitting Plugin Verifies Connections

The property `verifyNewSrwConnections` is enabled by default. This means that when new connections are made with the Simple Read/Write Splitting Plugin, a query is sent to the new connection to verify its role. If the connection cannot be verified as having the correct role—that is, a write connection is not connected to a writer, or a read connection is not connected to a reader—the plugin will retry the connection up to the time limit of `srwConnectRetryTimeoutMs`. 

The values of `srwConnectRetryTimeoutMs` and `srwConnectRetryIntervalMs` control the timing and aggressiveness of the plugin's retries.

Additionally, to consistently ensure the role of connections made with the plugin, the plugin also provides role verification for the initial connection. When connecting with an RDS writer cluster or reader cluster endpoint, the plugin will retry the initial connection up to `srwConnectRetryTimeoutMs` until it has verified the intended role of the endpoint.
If it is unable to return a verified initial connection, it will log a message and continue with the normal workflow of the other plugins.
When connecting with custom endpoints and other non-standard URLs, role verification on the initial connection can also be triggered by providing the expected role through the `verifyInitialConnectionType` parameter. Set this to `writer` or `reader` accordingly.

## Limitations When Verifying Connections

#### Non-RDS clusters
At this time, the AWS JDBC Driver only supports verifying the role of the connection for Aurora and RDS clusters. Thus, when not connecting to an Aurora or RDS cluster `verifyNewSrwConnections` must be set to `false`.
The Simple Read/Write Splitting Plugin will continue to function, relying purely on the endpoints from the `srwWriteEndpoint` and `srwReadEndpoint` parameters. For further information on compatible endpoints see [Compatibility](../CompatibilityEndpoints.md).

#### Autocommit
The verification logic results in errors such as `Cannot change transaction read-only property in the middle of a transaction` from the underlying driver when:
- autocommit is set to false 
- setReadOnly is called 
- as part of setReadOnly, a new connection is opened
- that connection's role is verified

This is a result of the plugin executing the role-verification query against a new connection, and when autocommit is false, this opens a transaction. 

If autocommit is essential to a workflow, either ensure the plugin has connected to the desired target connection of the setReadOnly query before setting autocommit to false or disable `verifyNewSrwConnections`. Examples of the former can be found in the [Simple Read/Write Splitting Examples](UsingTheSimpleReadWriteSplittingPlugin.md#examples).

## Using the Simple Read/Write Splitting Plugin with RDS Proxy

RDS Proxy provides connection pooling and management that significantly improves application scalability by reducing database connection overhead and enabling thousands of concurrent connections through
connection multiplexing. Connecting exclusively through the proxy endpoint ensures consistent connection management, automatic failover handling, and centralized monitoring, while protecting the underlying database from connection exhaustion
and providing a stable abstraction layer that remains consistent even when database topology changes. By providing the read/write endpoint and a read-only endpoint to the Simple Read/Write Splitting Plugin, the AWS JDBC Driver will connect using 
these endpoints any time setReadOnly is called. 

To take full advantage of the benefits of RDS Proxy, it is recommended to only connect through RDS Proxy endpoints. See [Using the AWS JDBC Driver with RDS Proxy](./../../../README.md#rds-proxy) for limitations.

## Using the Simple Read/Write Splitting Plugin against non-RDS clusters

The Simple Read/Write Splitting Plugin can be used to switch between any two endpoints. If the endpoints do not direct to an RDS cluster, ensure the property `verifyNewSrwConnections` is set to `false`. See [Limitations of verifyNewSrwConnections](UsingTheSimpleReadWriteSplittingPlugin.md#non-rds-clusters) for details.

## Limitations

### General plugin limitations

When a Statement or ResultSet is created, it is internally bound to the database connection established at that moment. There is no standard JDBC functionality to change the internal connection used by Statement or ResultSet objects. Consequently, even if the read/write plugin switches the internal connection, any Statements/ResultSets created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after switching between the writer/reader.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

### Failover

Immediately following a failover event, due to DNS caching, an RDS cluster endpoint may connect to the previous writer, and the read-only endpoint may connect to the new writer instance. 

To avoid stale DNS connections, enable `verifyNewSrwConnections`, as this will retry the connection until the role has been verified. Service for Aurora clusters is typically restored in less than 60 seconds, and often less than 30 seconds. RDS Proxy endpoints to Aurora databases can update in as little as 3 seconds. Depending on your configuration and cluster availability `srwConnectRetryTimeoutMs` and `srwConnectRetryIntervalMs` may be set to customize the timing of the retries.

Following failover, endpoints that point to specific instances will be impacted if their target instance was demoted to a reader or promoted to a writer. The Simple Read/Write Splitting Plugin always connects to the endpoint provided in the initial connection properties when `setReadOnly` is called. We suggest using endpoints that return connections with a specific role such as cluster or read-only endpoints, or using the [Read/Write Splitting Plugin](UsingTheReadWriteSplittingPlugin.md) to connect to instances based on the cluster's current topology.

### Session state

The plugin supports session state transfer when switching connections. All attributes mentioned in [Session State](../SessionState.md) are automatically transferred to a new connection.

## Examples
[SimpleReadWriteSplittingPostgresExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/SimpleReadWriteSplittingPostgresExample.java) and [SimpleReadWriteSplittingMySQLExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/SimpleReadWriteSplittingMySQLExample.java) demonstrate how to enable and configure Simple Read/Write Splitting with the AWS JDBC Driver.
