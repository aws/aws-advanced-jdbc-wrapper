# Host Monitoring Plugin

## Enhanced Failure Monitoring
<div style="text-align:center"><img src="../../images/enhanced_failure_monitoring_diagram.png"/></div>

The figure above shows a simplified workflow of Enhanced Failure Monitoring. Enhanced Failure Monitoring is a feature available from the Host Monitoring Connection Plugin. The monitor will periodically check the connected database node's health. If a database node is determined to be unhealthy, the query will be retried with a new database node and the monitor restarted. Enhanced Failure Monitoring and the Host Monitoring plugin is most useful when a user application executes a long query because a database node may become unhealthy during the execution time.

Enhanced Failure Monitoring will NOT be loaded unless the Host Monitoring plugin is explicitly included by adding the plugin code `efm` to the [`wrapperPlugins`](../../using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#connection-plugin-manager-parameters) value, or if it is added to the current [driver profile](../../using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#connection-plugin-manager-parameters). When the Host Monitoring plugin is loaded, it is enabled by default with the parameter `failureDetectionEnabled` set to `true`.

### Enhanced Failure Monitoring Parameters
The parameters `failureDetectionTime`, `failureDetectionInterval`, and `failureDetectionCount` are similar to TCP Keep Alive parameters.

<details>
<summary>Enhanced Failure Monitoring Parameters</summary>

| Parameter                  |  Value  | Required | Description                                                                                                  | Default Value |
|----------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------|---------------|
| `failureDetectionCount`    | Integer |    No    | Number of failed connection checks before considering database node as unhealthy.                            | `3`           |
| `failureDetectionEnabled`  | Boolean |    No    | Set to `true` to enable Enhanced Failure Monitoring. Set to `false` to disable it.                           | `true`        |
| `failureDetectionInterval` | Integer |    No    | Interval in milliseconds between probes to database node.                                                    | `5000`        |
| `failureDetectionTime`     | Integer |    No    | Interval in milliseconds between sending a SQL query to the server and the first probe to the database node. | `30000`       |
| `monitorDisposalTime`      | Integer |    No    | Interval in milliseconds for a monitor to be considered inactive and to be disposed.                         | `60000`       |
</details>

>### :warning: Warnings About Usage of the AWS Advanced JDBC Wrapper with RDS Proxy
> Using RDS Proxy endpoints with the AWS Advanced JDBC Wrapper with Host Monitoring Connection Plugin doesn't cause any critical issues. However, this approach is not recommended. The main reason is that RDS Proxy transparently re-routes requests to one database instance. RDS Proxy decides which database instance is used based on many criteria, and it's on a per-request basis. Such switching between different instances makes the Host Monitoring plugin useless in terms of instance health monitoring. The plugin will not be able to identify what actual instance it's connected to and which one it's monitoring. That could be a source of false positive failure detections. At the same time, the plugin can still proactively monitor network connectivity to RDS Proxy endpoints and report outages back to a user application if they occur.
>
> It is recommended to either turn off the Host Monitoring plugin, or to avoid using RDS Proxy endpoints when the plugin is active.
