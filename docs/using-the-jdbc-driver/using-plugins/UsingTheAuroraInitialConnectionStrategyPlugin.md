# Aurora Initial Connection Strategy Plugin
The AWS Advanced JDBC Driver allows the user to configure their initial connection strategy using this plugin. With this plugin enabled, the initial connection will be chosen based on the configured strategy. The time between retries and timeouts for these initial connections can also be configured.
<br><br>
When a user uses cluster endpoints, then the actual instance for a new connection is resolved by DNS. During failover, the cluster elects another instance. DNS is updated, but it takes time (up to 40–60s). While DNS is updating, if a user tries to connect to the endpoint, it gets connected to an old node. This plugin is used to replace the old endpoint with a current endpoint while DNS is still updating. Changing the initial connection strategy would change which endpoint is chosen to replace the old one in this scenario.

## Enabling the Aurora Initial Connection Strategy Plugin

To enable the Aurora Initial Connection Strategy Plugin, add `initialConnection` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value.

## Aurora Initial Connection Strategy Connection Parameters

The following properties can be used to configure the Aurora Initial Connection Strategy Plugin.

| Parameter                                     |  Value  | Required | Description                                                                                                                                                                                                                   | Example            | Default Value |
|-----------------------------------------------|:-------:|:--------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------|
| `readerInitialConnectionHostSelectorStrategy` | String  |    No    | The strategy that should be used to select a new reader host while opening a new connection. <br><br> For more information on the different strategies for this parameter, see this [table](../ReaderSelectionStrategies.md). | `leastConnections` | `random`      |
| `openConnectionRetryTimeoutMs`                | Integer |    No    | The maximum allowed time for retries when opening a connection in milliseconds.                                                                                                                                               | `200`              | `30000`       |
| `openConnectionRetryIntervalMs`               | Integer |    No    | The time between retries when opening a connection in milliseconds.                                                                                                                                                           | `200`              | `1000`        |

## Examples

Enabling the plugin using the properties. 

```java
properties.setProperty("wrapperPlugins", "initialConnection");
```
Setting the parameters using the properties.

```java
properties.setProperty("openConnectionRetryTimeoutMs", 200);
```
