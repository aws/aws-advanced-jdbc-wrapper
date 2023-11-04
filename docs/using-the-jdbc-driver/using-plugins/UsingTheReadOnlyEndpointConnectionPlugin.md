# Read Only Endpoint Connection Plugin

When making connections to the reader cluster endpoint of an Aurora database, each connection is made to a random available reader instance. This may cause errors if operations depend on making connections to the same instance. For example, to execute a query timeout with the MySQL Connector/J 8.0 Driver, a new internal connection will be made to kill the initial query. If the reader endpoint is used, the kill query may fail to cancel the initial query because the queries will be made to different hosts.

When the Read Only Endpoint Connection Plugin is enabled and a connection to the read only endpoint is made, the connection will be made to a random reader. Before that connection is return, the Read Only Endpoint Connection Plugin will query for the current instance name and will create a new connection to that instance. The first connection that was made using the read only endpoint will be replaced by the plugin with the instance endpoint and the instance connection will be returned to the user.

## Prerequisites
This plugin should only be used when connecting to an Aurora database with a read only endpoint.

## Enabling the Read Only Endpoint Connection Plugin

To enable the Reade Only Endpoint Connection Plugin, add the plugin code `readOnlyEndpoint` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).
