# Host Monitoring Plugin

## Enhanced Failure Monitoring
<div style="text-align:center"><img src="../../images/enhanced_failure_monitoring_diagram.png"/></div>

The figure above shows a simplified workflow of Enhanced Failure Monitoring (EFM). Enhanced Failure Monitoring is a feature available from the Host Monitoring Connection Plugin. There is a monitor that will periodically check the connected database node's health, or availability. If a database node is determined to be unhealthy, the connection will be aborted. The Host Monitoring Connection Plugin uses the [Enhanced Failure Monitoring Parameters](#enhanced-failure-monitoring-parameters) and a database node's responsiveness to determine whether a node is healthy.

### The Benefits Enhanced Failure Monitoring
Enhanced Failure Monitoring helps user applications detect failures earlier. When a user application executes a query, EFM may detect that the connected database node is unavailable. When this happens, the query is cancelled and the connection will be aborted. This allows queries to fail fast instead of waiting indefinitely or failing due to a timeout.

One use case is to pair EFM with the [Failover Connection Plugin](./UsingTheFailoverPlugin.md). When EFM discovers a database node failure, the connection will be aborted. Without the Failover Connection Plugin, the connection would be terminated up to the user application level. With the Failover Connection Plugin, the JDBC Wrapper can attempt to failover to a different, healthy database node where the query can be executed.

Not all user applications will have a need for Enhanced Failure Monitoring. If a user application's query times are predictable and short, and the application does not execute any long-running SQL queries, Enhanced Failure Monitoring may be replaced with one of the following alternatives that consumes fewer resources and is simpler to configure. 

Two [alternatives](#enhanced-failure-monitoring-alternatives) are: 
1. setting a [simple network timeout](#simple-network-timeout), or 
2. using [TCP Keepalive](#tcp-keepalive).

Although these alternatives are available, EFM is more configurable than simple network timeouts, and it is also simpler to configure than TCP Keepalive. Users should keep these advantages and disadvantages in mind when deciding whether Enhanced Failure Monitoring is suitable for their application.

### Enhanced Failure Monitoring Alternatives

#### Simple Network Timeout
This option is useful when a user application executes quick statements that run for predictable lengths of time. In this case, the network timeout should be set to a value such as the 95th to 99th percentile. One way to do this is with the `setNetworkTimeout` method.

#### TCP Keepalive
This option is useful because it is built into the TCP protocol. How to enable it depends on the underlying driver provided to the JDBC Wrapper. For example, to enable TCP Keepalive with an underlying PostgreSQL driver, the user will need to set the property `tcpKeepAlive` to `true`. TCP Keepalive settings, which are similar to some Enhanced Failure Monitoring parameters, can all be configured. However, this is specific to operating systems, so users should verify what they need to do to configure TCP Keepalive on their system before using this alternative. See [this page](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) for more information on how to set TCP Keepalive parameters.

### Enabling the Host Monitoring Connection Plugin
Enhanced Failure Monitoring will NOT be enabled unless the Host Monitoring Connection Plugin is explicitly loaded by adding the plugin code `efm` to the [`wrapperPlugins`](https://github.com/awslabs/aws-advanced-jdbc-wrapper/docs/using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#aws-advanced-jdbc-wrapper-parameters) value, or if it is added to the current [driver profile](https://github.com/awslabs/aws-advanced-jdbc-wrapper/docs/using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#aws-advanced-jdbc-wrapper-parameters). Enhanced Failure Monitoring is enabled by default when the Host Monitoring Connection Plugin is loaded, but it can be disabled with the parameter `failureDetectionEnabled` set to `false`.

> :warning: **Note:** When loading the Host Monitoring Connection Plugin, the order plugins are loaded in matters. It is recommended that the Host Monitoring Connection Plugin is loaded at the end or as close to the end as possible. When used in conjunction with the Failover Connection Plugin, the Host Monitoring Connection Plugin must be loaded after the Failover Connection Plugin. For example, when loading plugins with the `wrapperPlugins` parameter, the parameter value should be `failover,...,efm`.
> 
### Enhanced Failure Monitoring Parameters
<div style="text-align:center"><img src="../../images/efm_monitor_process.png" /></div>

The parameters `failureDetectionTime`, `failureDetectionInterval`, and `failureDetectionCount` are similar to TCP Keepalive parameters. Each connection has its own set of parameters. The `failureDetectionTime` is how long the monitor waits after a SQL query is started to send a probe to a database node. The `failureDetectionInterval` is how often the monitor sends a probe to a database node. The `failureDetectionCount` is how many times a monitor probe can go unacknowledged before the database node is deemed unhealthy. 

To determine the health of a database node: 
1. The monitor will first wait for a time equivalent to the `failureDetectionTime`. 
2. Then, every `failureDetectionInterval`, the monitor will send a probe to the database node. 
3. If the probe is not acknowledged by the database node, a counter is incremented. 
4. If the counter reaches the `failureDetectionCount`, the database node will be deemed unhealthy and the connection will be aborted.

If a more aggressive approach to failure checking is necessary, all of these parameters can be reduced to reflect that. However, increased failure checking may also lead to an increase in false positives. For example, if the `failureDetectionInterval` was shortened, the plugin may complete several connection checks that all fail. The database node would then be considered unhealthy, but it may have been about to recover and the connection checks were completed before that could happen.

| Parameter                  |  Value  | Required | Description                                                                                                  | Default Value |
|----------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------|---------------|
| `failureDetectionCount`    | Integer |    No    | Number of failed connection checks before considering database node as unhealthy.                            | `3`           |
| `failureDetectionEnabled`  | Boolean |    No    | Set to `true` to enable Enhanced Failure Monitoring. Set to `false` to disable it.                           | `true`        |
| `failureDetectionInterval` | Integer |    No    | Interval in milliseconds between probes to database node.                                                    | `5000`        |
| `failureDetectionTime`     | Integer |    No    | Interval in milliseconds between sending a SQL query to the server and the first probe to the database node. | `30000`       |
| `monitorDisposalTime`      | Integer |    No    | Interval in milliseconds for a monitor to be considered inactive and to be disposed.                         | `60000`       |

>### :warning: Warnings About Usage of the AWS Advanced JDBC Wrapper with RDS Proxy
> It is recommended to either disable the Host Monitoring Connection Plugin, or to avoid using RDS Proxy endpoints when the Host Monitoring Connection Plugin is active.
>
> Although using RDS Proxy endpoints with the AWS Advanced JDBC Wrapper with Enhanced Failure Monitoring doesn't cause any critical issues, this approach is not recommended. The main reason is that RDS Proxy transparently re-routes requests to one database instance. RDS Proxy decides which database instance is used based on many criteria, and it's on a per-request basis. Such switching between different instances makes the Host Monitoring Connection Plugin useless in terms of instance health monitoring. The plugin will not be able to identify what actual instance it's connected to and which one it's monitoring. That could be a source of false positive failure detections. At the same time, the plugin can still proactively monitor network connectivity to RDS Proxy endpoints and report outages back to a user application if they occur.
