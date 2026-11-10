# Using the Connection Lifetime Plugin

The Connection Lifetime Plugin limits how long the AWS Advanced JDBC Wrapper uses a physical database connection. When an expired connection is about to make a network-bound JDBC call, the plugin either closes the connection or reconnects through the host originally supplied by the application.

## Plugin Availability

The plugin is available since version 4.7.0.

## Enabling the Connection Lifetime Plugin

Add the plugin code `connectionLifetime` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value or to the current driver profile. The required `connectionLifetimeMs` property must also be set.

For example:

```text
jdbc:aws-wrapper:postgresql://example.com:5432/database?wrapperPlugins=connectionLifetime&connectionLifetimeMs=300000
```

## Configuration Parameters

| Parameter                  | Available Since Version | Value  | Required | Description                                                                                          | Default Value |
|----------------------------|-------------------------|--------|----------|------------------------------------------------------------------------------------------------------|---------------|
| `connectionLifetimeMs`     | 4.7.0                   | Long   | Yes      | Maximum physical connection lifetime in milliseconds. The value must be at least `1`.                | None          |
| `connectionLifetimeAction` | 4.7.0                   | String | No       | Action taken after expiration. Accepted values are `reconnect` and `close`, ignoring letter case.    | `reconnect`   |

## Expiration Checks

The lifetime starts when a physical connection is successfully created. A connection is expired when its elapsed lifetime is greater than or equal to `connectionLifetimeMs`. The boundary is inclusive, so a connection with a configured lifetime of 1000 ms is expired at exactly 1000 ms.

The plugin checks the lifetime immediately before network-bound JDBC calls identified by the target driver dialect. It does not run a background timer. An expired connection therefore remains open until the next network-bound call triggers the configured action.

Lifecycle and local calls do not trigger an expiration action. These exclusions include `Connection.close`, `Connection.abort`, `Connection.isClosed`, wrapper operations such as `unwrap`, and local JDBC property access. The exact set of network-bound methods depends on the target driver dialect.

The plugin applies only to the connection used by the application. Internal connections opened by other plugins, such as monitoring or topology-refresh connections, have their own lifecycle and are not tracked.

The configured action runs before the pending network call. The pending call is not executed when the plugin closes or reconnects the connection.

## Close Action

With `connectionLifetimeAction=close`, the plugin closes the current physical connection before the pending call. It then throws `ConnectionLifetimeSQLException` with SQLState `08003` to report that the connection reached its maximum lifetime and is closed.

The wrapper connection remains permanently invalid. The application must dispose of it rather than retrying with it. A connection pool should remove the wrapper connection from the pool.

## Reconnect Action

With the default `connectionLifetimeAction=reconnect`, the plugin opens a replacement physical connection using the original application-provided `HostSpec`, including its host and port. It does not use a host selected later by routing, read/write splitting, or failover. The driver installs the replacement in the existing wrapper connection and resets the lifetime only after installation succeeds.

After a successful reconnect, the plugin throws `ConnectionLifetimeSuccessSQLException` with SQLState `08S02`. This exception reports that the existing wrapper connection now contains a usable replacement physical connection. The pending JDBC call was not executed. The application must handle the exception, restore any required session state, and retry the operation when appropriate.

Statements, prepared statements, callable statements, and result sets created before reconnection are associated with the previous physical connection and may be stale. Close them and create new JDBC objects from the wrapper connection before retrying.

If creating or installing the replacement fails, the plugin throws `ConnectionLifetimeFailedSQLException` with SQLState `08001`. The reconnect failure is preserved as the exception cause. The application should dispose of the wrapper connection.

A successful reconnect starts a new lifetime interval. A failed reconnect does not reset the timer. If another plugin or driver operation switches the physical connection, the plugin tracks the new physical connection separately without resetting the timer for host-only metadata changes.

## Transactions

Neither action runs while a local or XA transaction is active. If the lifetime expires during a transaction, network-bound calls continue on the current physical connection. This allows transaction completion calls, including `commit` and `rollback`, to execute without closing or reconnecting first. Deferral does not extend or reset the lifetime.

After the transaction ends, the first eligible network-bound call runs the configured action before that call: `close` closes the connection and throws `ConnectionLifetimeSQLException`; `reconnect` reconnects and throws `ConnectionLifetimeSuccessSQLException`. The pending call is not executed in either case.

## Connection Pooling

Connection pools should treat the lifetime exceptions according to whether the wrapper connection remains usable:

- `ConnectionLifetimeSuccessSQLException` (`08S02`) leaves the wrapper connection usable because the replacement physical connection is already installed.
- The terminal close exception (`08003`) and `ConnectionLifetimeFailedSQLException` (`08001`) indicate that the pool should evict the wrapper connection.

When using HikariCP, configure the wrapper's exception override so HikariCP retains successful reconnects but follows its normal SQLState-based eviction path for closed connections and reconnect failures:

```java
HikariDataSource dataSource = new HikariDataSource();
dataSource.setExceptionOverrideClassName("software.amazon.jdbc.util.HikariCPSQLException");
```

Other connection pools may require equivalent exception or SQLState handling.
