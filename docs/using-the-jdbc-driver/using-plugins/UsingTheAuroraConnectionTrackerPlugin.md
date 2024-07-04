# Aurora Connection Tracker Plugin

This plugin tracks all the opened connections. In the event of a cluster failover, this plugin will close all the impacted connections.
If no plugins are explicitly specified, this plugin is enabled by default.

## Use Case
User applications can have two types of connections:

1. active connections that are used to execute statements or perform other types of database operations.
2. idle connections that the application holds references but are not used for any operations.

For instance, the user application had an active connection and an idle connection to node A where node A was a writer instance. The user application was executing DML statements against node A when a cluster failover occurred. A different node was promoted as the writer, so node A is now a reader. The driver will failover the active connection to the new writer, but it would not modify the idle connection.

When the application tries to continue the workflow with the idle connection that is still pointing to a node that has changed roles, i.e. node A, users may get an error caused by unexpected behaviour, such as `ERROR: cannot execute UPDATE in a read-only transaction`.

Since the Aurora Connection Tracker Plugin keeps track of all the open connections, the plugin can close all impacted connections after failover.
When the application tries to use the outdated idle connection, the application will get a `connection's closed` error instead.
