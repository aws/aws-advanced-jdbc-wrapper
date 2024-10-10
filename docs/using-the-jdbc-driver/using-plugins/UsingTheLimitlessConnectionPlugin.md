# Using the Limitless Connection Plugin

## What is Amazon Aurora Limitless Database?

Amazon Aurora Limitless Database is a new type of database that can horizontally scale to handle millions of write transactions per second and manage petabytes of data.
Users will be able to use the AWS JDBC Driver with Aurora Limitless Databases and optimize their experience using the Limitless Connection Plugin. 
To learn more about Aurora Limitless Database, see the [Amazon Aurora Limitless documentation](https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-aurora-limitless-database/).

## Why use the Limitless Connection Plugin?

Aurora Limitless Database introduces a new endpoint for the databases - the DB shard group (limitless) endpoint that's managed by Route 53. 
When connecting to Aurora Limitless Database, clients will connect using this endpoint, and be routed to a transaction router via Route 53.
Unfortunately, Route 53 is limited in its ability to load balance, and can allow uneven work loads on transaction routers.
The Limitless Connection Plugin addresses this by performing client-side load balancing with load awareness. 

The Limitless Connection Plugin achieves this by periodically polling for available transaction routers and their load metrics, and then caching them.
When a new connection is made, the plugin directs the connection to a transaction router selected from the cache using a weighted round-robin strategy.
Routers with a higher load are assigned a lower weight, and routers with a lower load are assigned a higher weight.

## How to use the Limitless Connection Plugin with the AWS JDBC Driver
To enable the Limitless Connection Plugin, add the plugin code `limitless` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

The URL used to connect to a limitless database is the DB shard group URL.

### Limitless Connection Plugin Parameters
| Parameter                                          |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default Value | Example Value |
|----------------------------------------------------|:-------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `limitlessTransactionRouterMonitorIntervalMs`      | Integer |    No    | Interval in milliseconds between polling for load metric metadata of transaction routers. Note that the default value of 15 seconds was chosen to match the expected refresh rate of load metric metadata from the database.                                                                                                                                                                                                                                               | `15000`       | `30000`       |
| `limitlessTransactionRouterMonitorDisposalTimeMs`  | Integer |    No    | Interval in milliseconds for Limitless monitor to be considered inactive and to be disposed.                                                                                                                                                                                                                                                                                                                                                                               | `600000`      | `300000`      |
| `limitlessConnectMaxRetries`                       | Integer |    No    | Max number of connection retries the Limitless Connection Plugin will attempt.                                                                                                                                                                                                                                                                                                                                                                                             | `5`           | `13`          |
| `limitlessWaitForTransactionRouterInfo`            | Boolean |    No    | In scenarios such as application start-up, the cache of available transaction routers may be empty. If the cache is empty and this property is set to `true`, the plugin will synchronously fetch info on available transaction routers and then directly connect to a transaction router. If the cache is empty and this property set to `false`, the plugin will connect using the DB Shard Group endpoint where it will be routed to a transaction router via Route 53. | `true`        | `false`       |
| `limitlessGetTransactionRouterInfoRetryIntervalMs` | Integer |    No    | Interval in millis between retries fetching Limitless Transaction Router information.                                                                                                                                                                                                                                                                                                                                                                                      |               |               |
| `limitlessGetTransactionRouterInfoMaxRetries`      | Integer |    No    | Max number of connection retries fetching Limitless Transaction Router information.                                                                                                                                                                                                                                                                                                                                                                                        |               |               |

### Use with other plugins
The Limitless Connection Plugin is compatible with authentication type plugins such as the IAM and AWS Secrets Manager Plugins.

> [!IMPORTANT]\
> The Failover, Host Monitoring, and Read Write Splitting Plugins are also compatible with the Limitless Connection Plugin.  
However, we don't recommend using them with the Limitless Connection Plugin because they're not designed to be used with Aurora Limitless Database. 
They don't provide any extra value, and add unnecessary computation and memory overhead.

### Use with Connection Pools
Connection pools keep connections open for reuse, but this can work against the client-side load-balancing of the Limitless Connection Plugin and cause an imbalanced load on transaction routers.
To mitigate this, consider setting connection properties that can reduce the number of idle connections or increase the lifetime of connections.
If you're using HikariCP, some of these properties are `idleTimeout`, `maxLifetime`, `minimumIdle`.

## Sample Code
[Limitless Postgresql Example](../../../examples/AWSDriverExample/src/main/java/software/amazon/LimitlessPostgresqlExample.java)
