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

When the driver needs to switch to a new connection, it opens a new connection and transfers a session state to it. All current session state values are applied to the new connection. Pristine values for a new connection are also fetched and stored if needed. Pristine values are remembered for each physical connection, so a connection the driver switches back to (for example, a cached writer or reader connection used by read/write splitting) is not queried for them again. When a new connection is configured, it replaces the current internal connection.

Session transfer can be disabled by using the `transferSessionStateOnSwitch` configuration parameter. Setting it to `false` also stops tracking session state changes. To skip the transfer only on read/write splitting switches, see [Read/write splitting switches](#readwrite-splitting-switches).

### Read/write splitting switches

The read/write splitting plugins switch back and forth between a writer connection and a reader connection, and keep both open. By default, each switch applies the current session state to the connection switched to, as described above. The `rwTransferSessionStateOnSwitch` parameter (default `true`) controls this for all read/write splitting plugins: `readWriteSplitting`, `autoReadWriteSplitting`, `srw`, `autoSimpleReadWriteSplitting`, `gdbReadWriteSplitting`, `gdbAutoReadWriteSplitting`, `gdbSimpleReadWriteSplitting` and `gdbAutoSimpleReadWriteSplitting`.

With `rwTransferSessionStateOnSwitch=false`, a read/write splitting switch to an already-open reader or writer connection does not apply the session state to it. A connection that the switch opens (first use, or a replacement for a closed or expired cached connection) still receives the full session state, so settings made before it was opened (for example, a transaction isolation level set once by a connection pool) reach it. A skipped switch does not call the custom transfer handler set with `setTransferSessionStateOnSwitchFunc`.

The trade-off: catalog, schema, transaction isolation, autoCommit, network timeout, holdability and type map changed through JDBC after both connections are open are not propagated to the other connection on a switch. A `setReadOnly` call still reaches its target, because the wrapper applies it after the switch. A SQL-routed switch (`auto*` plugins) leaves the target's readOnly setting as it was. The session state is still tracked, and other switches, such as failover, still transfer it. With the internal connection pool, the idle connection is returned to the pool on each switch, so most switches obtain a connection and transfer the session state as usual. Use `false` only when the application sets session settings before the connections are opened and does not change them between switches.

With MySQL Connector/J, consider setting `useLocalSessionState=true` (a driver property the wrapper passes through to the driver). The driver then skips `SET autocommit` and `SET SESSION TRANSACTION ...` statements when the value is unchanged, and answers `isReadOnly()` and `getTransactionIsolation()` locally. This removes most session state round trips on a switch without changing what is transferred. It is only safe when the session is changed through JDBC calls, not through raw `SET` statements.

## Session State Custom handlers

It's possible to extend or replace existing logic of resetting session state and transferring session state with custom handlers. Use the following methods on `software.amazon.jdbc.Driver` class to set and reset custom handlers:
- `setResetSessionStateOnCloseFunc`
- `resetResetSessionStateOnCloseFunc`
- `setTransferSessionStateOnSwitchFunc`
- `resetTransferSessionStateOnSwitchFunc`
