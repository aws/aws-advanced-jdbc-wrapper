# Automatic Simple Read/Write Splitting Plugin

The Automatic Simple Read/Write Splitting Plugin (`autoSimpleReadWriteSplitting`) combines the automatic, SQL-driven routing of the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md) with the two-endpoint model of the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md).

Instead of relying on cluster topology, the plugin routes between two endpoints you supply — one for reads and one for writes — and it decides *which* endpoint to use per statement by analyzing the SQL:

- `SELECT` statements are routed to the read endpoint.
- `INSERT`, `UPDATE`, `DELETE`, and DDL statements are routed to the write endpoint.
- `SELECT ... FOR UPDATE` (and other row-locking `SELECT` variants) are routed to the write endpoint.
- The `/*@reader*/`, `/*@writer*/`, and `/*@keep*/` routing hints are honored, exactly as in the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md#routing-hints).
- While a transaction is open or autocommit is disabled, the connection is pinned (no re-routing).

The plugin also still honors `Connection#setReadOnly` calls, so applications may mix explicit read-only toggles with automatic routing.

This plugin is intended for deployments that have no Aurora topology to query — for example community databases, RDS Proxy, or custom routing — but still want per-statement read/write routing without changing application code.

## Plugin Availability

This plugin is part of the unified read/write splitting plugin family. See the CHANGELOG for the release it first appears in.

> [!WARNING]
> Use exactly one read/write splitting plugin per connection. Do not combine `autoSimpleReadWriteSplitting` with any other read/write splitting plugin — `readWriteSplitting`, `autoReadWriteSplitting`, `srw`, `gdbReadWriteSplitting`, `gdbAutoReadWriteSplitting`, `gdbSimpleReadWriteSplitting`, or `gdbAutoSimpleReadWriteSplitting` — for the same connection. They are all read/write splitting plugins and will conflict.

## Loading the Automatic Simple Read/Write Splitting Plugin

The plugin is not loaded by default. To load it, include `autoSimpleReadWriteSplitting` in the `wrapperPlugins` connection parameter. Because routing is driven by SQL analysis, the `sqlParser` plugin **must be listed before** it in the plugin chain, and the `com.github.jsqlparser:jsqlparser` dependency must be on the classpath (see [Required dependency: JSQLParser](./UsingTheAutoReadWriteSplittingPlugin.md#required-dependency-jsqlparser)).

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,autoSimpleReadWriteSplitting");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
```

The driver performs plugin sorting by default (see the [`autoSortWrapperPluginOrder` configuration parameter](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters)), which keeps `sqlParser` ahead of `autoSimpleReadWriteSplitting`.

## Configuration Parameters

This plugin accepts the same endpoint parameters as the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#simple-readwrite-splitting-plugin-parameters):

| Parameter | Value | Required | Description | Default Value |
|---|:---:|:---:|---|---|
| `srwWriteEndpoint` | String | Yes | The endpoint to connect to for write routing (and when `setReadOnly(false)` is called). | `null` |
| `srwReadEndpoint` | String | Yes | The endpoint to connect to for read routing (and when `setReadOnly(true)` is called). | `null` |
| `verifyNewSrwConnections` | Boolean | No | Enables writer/reader verification for new connections. See the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#how-the-simple-readwrite-splitting-plugin-verifies-connections). | `true` |
| `verifyInitialConnectionType` | String | No | If `verifyNewSrwConnections` is `true`, verifies the initial opened connection to be a `writer` or `reader`. | `null` |
| `srwConnectRetryTimeoutMs` | Integer | No | Maximum allowed time in milliseconds for retrying connection attempts. | `60000` |
| `srwConnectRetryIntervalMs` | Integer | No | Time delay in milliseconds between connection retries. | `1000` |
| `cachedReaderKeepAliveTimeoutMs` | Integer | No | Timeout for the cached reader connection. `0` reuses the same cached reader for the lifetime of the `Connection`. | `0` |

It also accepts the family-wide `queryLevelLoadBalancing`, `loadBalancingIncludeWriter`, and `allowStatementRecreationOnConnectionSwitch` parameters — see [Query-level load balancing](./UsingTheReadWriteSplittingPlugin.md#query-level-load-balancing). Because each read statement is a routing point, query-level load balancing rotates reads per query.

## Limitations

All limitations of the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md#limitations) (including the handling of `CallableStatement` and unparseable SQL) and the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#limitations) apply.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Related pages

- [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md) — SQL-driven routing over cluster topology.
- [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md) — `setReadOnly`-driven routing over two endpoints.
- [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) — shared behavior, internal connection pooling, and query-level load balancing.
