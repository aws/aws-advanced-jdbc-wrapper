# Using the Limitless Plugin

## What is Amazon Aurora Limitless Database?

Amazon Aurora Limitless Database allows users to horizontally scale their Aurora Databases, handling millions of write transactions per second and managing petabytes of data. To learn more, please [read Amazon Aurora Limitless documentation](// TODO).

## Why use the Limitless Connection Plugin?

Amazon Aurora Limitless Databases have transaction routers that abstract and manage the distributed behavior of the database. If there is an imbalanced load on these transaction routers, users can experience performance degradation. The Limitless Connection Plugin addresses this by performing client-side load balancing, spreading out loads evenly amongst routers.
The Limitless Connection Plugin achieves this by being load aware. It will periodically poll for metadata containing the load distribution of the transaction routers, and cache it. When a new connection is made, the plugin will direct it to the transaction router with the least load.

## How to use the Endpoint Connection Plugin with the AWS JDBC Driver
To enable the Endpoint Connection Plugin, add the plugin code `endpoint` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

### Endpoint Connection Plugin Parameters
| Parameter                                         |  Value  | Required | Description                                                                                    | Default Value | Example Value |
|---------------------------------------------------|:-------:|:--------:|:-----------------------------------------------------------------------------------------------|---------------|---------------|
| `limitlessTransactionRouterMonitorIntervalMs`     | Integer |    No    | Interval in milliseconds between polling for endpoints to the database.                        | `30000`       | `15000`       |
| `limitlessTransactionRouterMonitorDisposalTimeMs` | Integer |    No    | Interval in milliseconds for an endpoint monitor to be considered inactive and to be disposed. | `600000`      | `300000`      |

## Sample Code
[EndpointPluginExample](../../../examples/AWSDriverExample/src/main/java/software/amazon/EndpointPluginExample.java)
