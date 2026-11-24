# Aurora Connection Tracker Plugin

This plugin tracks all the opened connections. In the event of a cluster failover, this plugin will close all the impacted connections.
If no plugins are explicitly specified, this plugin is enabled by default.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Plugin Availability
The plugin is available since version 1.0.0.

## Use Case
User applications can have two types of connections:

1. active connections that are used to execute statements or perform other types of database operations.
2. idle connections that the application holds references but are not used for any operations.

For instance, the user application had an active connection and an idle connection to node A where node A was a writer instance. The user application was executing DML statements against node A when a cluster failover occurred. A different node was promoted as the writer, so node A is now a reader. The driver will failover the active connection to the new writer, but it would not modify the idle connection.

When the application tries to continue the workflow with the idle connection that is still pointing to a node that has changed roles, i.e. node A, users may get an error caused by unexpected behaviour, such as `ERROR: cannot execute UPDATE in a read-only transaction`.

Since the Aurora Connection Tracker Plugin keeps track of all the open connections, the plugin can close all impacted connections after failover.
When the application tries to use the outdated idle connection, the application will get a `connection's closed` error instead.

## Aurora Global Database switchover

When the primary region of an Aurora Global Database changes, an application in the new primary region still holds the connections it opened while that region was secondary. They point to reader instances. The plugin closes the tracked connections to every instance of the new primary region except the new writer, so the pool replaces them with writer connections.

While the switchover settles, a new connection can be opened from an outdated topology that still places the writer in the other region. Such a connection goes through the cluster writer endpoint without role verification and may reach a reader. When a new connection through a cluster writer endpoint reaches an instance other than the current writer, and that writer is in the endpoint's region, the plugin closes it and fails the attempt with SQL state `08001`, so that it is opened again. The [Aurora Initial Connection Strategy Plugin](./UsingTheAuroraInitialConnectionStrategyPlugin.md) retries such an attempt.

Both apply only with a Global Database dialect (`global-aurora-mysql`, `global-aurora-pg`).

| Parameter                         | Available Since Version | Value   | Required | Description                                                                                                                   | Default Value |
|-----------------------------------|-------------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------------------|---------------|
| `invalidateOnPrimaryRegionChange` | 4.8.0                   | Boolean | No       | Close connections to the other instances of the new primary region after a Global Database switchover, and reject new connections through an active cluster writer endpoint that did not reach the writer. | `true`        |

