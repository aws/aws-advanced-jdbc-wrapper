# Aurora Initial Connection Strategy Plugin

The AWS Advanced JDBC Driver allows the user to configure their initial connection strategy using this plugin. With this plugin enabled, the initial connection will be chosen based on the configured strategy. The time between retries and timeouts for these initial connections can also be configured.

## Enabling the Aurora Initial Connection Strategy Plugin

To enable the Aurora Initial Connection Strategy Plugin, add `initialConnection` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

## Aurora Initial Connection Strategy Connection Parameters

The following properties can be used to configure the Aurora Initial Connection Strategy Plugin.

| Parameter                                     |  Value  | Required | Description                                                                                                                                                                                                                   | Example            | Default Value |
|-----------------------------------------------|:-------:|:--------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------|
| `readerInitialConnectionHostSelectorStrategy` | String  |    No    | The strategy that should be used to select a new reader host while opening a new connection. <br><br> For more information on the different strategies for this parameter, see this [table](../ReaderSelectionStrategies.md). | `leastConnections` | `random`      |
| `openConnectionRetryTimeoutMs`                | Integer |    No    | The maximum allowed time for retries when opening a connection in milliseconds.                                                                                                                                               | `200`              | `30000`       |
| `openConnectionRetryIntervalMs`               | Integer |    No    | The time between retries when opening a connection in milliseconds.                                                                                                                                                           | `200`              | `1000`        |

