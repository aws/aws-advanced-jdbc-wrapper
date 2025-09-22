# Session States

## What is a session state?

Every connection is associated with a connection session on the server and a group of related session settings like the autoCommit flag or the transaction isolation level. The following session settings are tracked by the AWS Advanced JDBC Wrapper and together they form a session state:
- autoCommit (`setAutoCommit`, `getAutoCommit`)
- readOnly (`isReadOnly`, `setReadOnly`)
- transaction isolation level (`setTransactionIsolation`, `getTransactionIsolation`)
- holdability (`setHoldability`, `getHoldability`)
- network timeout (`setNetworkTimeout`, `getNetworkTimeout`)
- catalog (`setCatalog`, `getCatalog`)
- schema (`setSchema`, `getSchema`)
- types mapping (`setTypeMap`, `getTypeMap`)

Since the AWS Advanced JDBC Wrapper can transparently switch physical connection to a server (for instance, during a cluster failover), it's important to re-apply a current session state to a new connection during such switch.   

## Tracking Session States Changes
<div style="center"><img src="../images/session_state_switch_connection.jpg" alt="diagram for the session state transfer"/></div>

The diagram above shows the process of switching one database connection `A` to a new connection `B`. After connection `A` is established, it's returned to the user application. A user application may use this connection to query data from the database as well as to change some session settings. For example, if the user application calls `setReadOnly` on a connection, the AWS Advanced JDBC Wrapper intercepts this call and stores a new session setting for the `readOnly` setting. At the same time, the driver verifies if the original session setting is known or not. If the original setting is not known, the driver will make an additional `getReadOnly` call and store the result as a pristine value in order to save the original session setting. Later, the driver may need the pristine value to restore the connection session state to its original state.   

## Restore to the Original Session State

Before closing an existing connection, the AWS Advanced JDBC Wrapper may try to reset all changes to the session state made by the user application. Some application frameworks and connection pools, like the Spring Framework or HikariCP, intercept calls to `close()` and may perform additional connection configuration. Since the AWS Advanced JDBC Wrapper might change the internal physical connection to a server, a new physical connection's settings may become unexpected to the user application and may cause errors. It is also important to mention that calling `close()` on a connection while using connection pooling doesn't close the connection or stop communicating to a server. Instead the connection is returned to a pool of available connections. Cleaning up a session state before returning a connection to a pool is necessary to avoid side effects and errors when a connection is retrieved from a pool to be reused.

Before closing a connection, the AWS Advanced JDBC Wrapper sets its session state settings with the pristine values that have been previously stored in the driver. If a pristine value isn't available, it means that there have been no changes to that particular setting made by the user application, and that it's safe to assume that this setting is in its original/unchanged state. 

Session state reset could be disabled by using `resetSessionStateOnClose` configuration parameter.

## Transfer Session State to a new Connection

When the driver needs to switch to a new connection, it opens a new connection and transfers a session state to it. All current session state values are applied to the new connection. Pristine values for a new connection are also fetched and stored if needed. When a new connection is configured, it replaces the current internal connection.

Session transfer cab be disabled by using the `transferSessionStateOnSwitch` configuration parameter.

## Session State Custom handlers

It's possible to extend or replace existing logic of resetting session state and transferring session state with custom handlers. Use the following methods on `software.amazon.jdbc.Driver` class to set and reset custom handlers:
- `setResetSessionStateOnCloseFunc`
- `resetResetSessionStateOnCloseFunc`
- `setTransferSessionStateOnSwitchFunc`
- `resetTransferSessionStateOnSwitchFunc`
