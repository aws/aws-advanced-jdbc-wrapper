# Aurora Initial Connection Strategy Plugin
The AWS Advanced JDBC Driver allows the user to configure their initial connection strategy using this plugin. The initial connection strategy chooses which reader endpoint is used to replace an old reader endpoint during failover. With this plugin enabled, the initial connection to a reader cluster endpoint will be chosen based on the configured strategy. Users can also choose how often to retry and the maximum allowed time for finding the reader in these initial connections using the parameters. This allows for more control over choosing the reader. This plugin is used to replace the old endpoint with a current endpoint while DNS is still updating. When a user connects to a cluster endpoint, the actual instance for a new connection is resolved by DNS. During failover, the cluster elects another instance to be the writer. While DNS is updating, which can take up to 40-60 seconds, if a user tries to connect to the endpoint they may be connected to an old node. Changing the initial connection strategy would change which reader endpoint is chosen to replace the old reader endpoint in this scenario.
## Enabling the Aurora Initial Connection Strategy Plugin

To enable the Aurora Initial Connection Strategy Plugin, add `initialConnection` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value.

## Aurora Initial Connection Strategy Connection Parameters

The following properties can be used to configure the Aurora Initial Connection Strategy Plugin.

| Parameter                                     |  Value  | Required | Description                                                                                                                                                                                                              | Example            | Default Value |
|-----------------------------------------------|:-------:|:--------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------|
| `readerInitialConnectionHostSelectorStrategy` | String  |    No    | The strategy that will be used to select a new reader host when opening a new connection. <br><br> For more information on the available reader selection strategies, see this [table](../ReaderSelectionStrategies.md). | `leastConnections` | `random`      |
| `openConnectionRetryTimeoutMs`                | Integer |    No    | The maximum allowed time for retries when opening a connection in milliseconds.                                                                                                                                          | `40000`            | `30000`       |
| `openConnectionRetryIntervalMs`               | Integer |    No    | The time between retries when opening a connection in milliseconds.                                                                                                                                                      | `2000`             | `1000`        |

## Examples

Enabling the plugin:

```java
properties.setProperty("wrapperPlugins", "initialConnection");
```
Configuring the plugin using the connection parameters:

```java
properties.setProperty("openConnectionRetryTimeoutMs", 40000);
```
