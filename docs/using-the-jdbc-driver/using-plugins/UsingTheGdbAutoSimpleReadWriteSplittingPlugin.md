# Global Database (GDB) Automatic Simple Read/Write Splitting Plugin

The GDB Automatic Simple Read/Write Splitting Plugin (`gdbAutoSimpleReadWriteSplitting`) is the SQL-routed variant of the [GDB Simple Read/Write Splitting Plugin](./UsingTheGdbSimpleReadWriteSplittingPlugin.md). It combines three behaviors:

- **Two-endpoint routing** — connects between the configured read and write endpoints instead of querying cluster topology (from the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md)).
- **Automatic, SQL-driven routing** — decides which endpoint to use per statement by analyzing the SQL, rather than requiring `Connection#setReadOnly` (from the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md)).
- **Global Database region rules** — applies the home-region constraints and Global Write Forwarding to the configured write endpoint (from the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md)).

Routing follows the same rules as the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md): `SELECT` to the read endpoint; DML/DDL and row-locking `SELECT`s to the write endpoint; `/*@reader*/`, `/*@writer*/`, and `/*@keep*/` hints honored; and no re-routing while a transaction is open or autocommit is disabled. Explicit `setReadOnly` calls are still honored.

## Plugin Availability

This plugin is part of the unified read/write splitting plugin family. See the CHANGELOG for the release it first appears in.

> [!WARNING]
> Use exactly one read/write splitting plugin per connection. Do not combine `gdbAutoSimpleReadWriteSplitting` with any other read/write splitting plugin — `readWriteSplitting`, `autoReadWriteSplitting`, `srw`, `autoSimpleReadWriteSplitting`, `gdbReadWriteSplitting`, `gdbAutoReadWriteSplitting`, or `gdbSimpleReadWriteSplitting` — for the same connection. They are all read/write splitting plugins and will conflict.

## Loading the GDB Automatic Simple Read/Write Splitting Plugin

The plugin is not loaded by default. To load it, include `gdbAutoSimpleReadWriteSplitting` in the `wrapperPlugins` connection parameter along with the two required endpoints. Because routing is driven by SQL analysis, the `sqlParser` plugin **must be listed before** it, and the `com.github.jsqlparser:jsqlparser` dependency must be on the classpath (see [Required dependency: JSQLParser](./UsingTheAutoReadWriteSplittingPlugin.md#required-dependency-jsqlparser)).

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,gdbAutoSimpleReadWriteSplitting");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("gdbRwHomeRegion", "us-east-2");
```

The driver performs plugin sorting by default (see the [`autoSortWrapperPluginOrder` configuration parameter](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters)), which keeps `sqlParser` ahead of `gdbAutoSimpleReadWriteSplitting`.

## Configuration Parameters

This plugin accepts the same parameters as the [GDB Simple Read/Write Splitting Plugin](./UsingTheGdbSimpleReadWriteSplittingPlugin.md#configuration-parameters): the endpoint parameters (`srwWriteEndpoint`, `srwReadEndpoint`, `verifyNewSrwConnections`, `verifyInitialConnectionType`, `srwConnectRetryTimeoutMs`, `srwConnectRetryIntervalMs`, `cachedReaderKeepAliveTimeoutMs`) and the Global Database region parameters (`gdbRwHomeRegion`, `gdbRwRestrictWriterToHomeRegion`, `gdbRwRestrictReaderToHomeRegion`, `gdbEnableGlobalWriteForwarding`, `gdbAccessibleRegions`).

It also accepts the family-wide `queryLevelLoadBalancing`, `loadBalancingIncludeWriter`, and `allowStatementRecreationOnConnectionSwitch` parameters — see [Query-level load balancing](./UsingTheReadWriteSplittingPlugin.md#query-level-load-balancing). Because each read statement is a routing point, query-level load balancing rotates reads per query.

## Limitations

All limitations of the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md#limitations) (including the handling of `CallableStatement` and unparseable SQL), the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#limitations), and the region behavior of the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md) apply.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Related pages

- [GDB Simple Read/Write Splitting Plugin](./UsingTheGdbSimpleReadWriteSplittingPlugin.md) — the `setReadOnly`-driven variant of this plugin.
- [GDB Automatic Read/Write Splitting Plugin](./UsingTheGdbAutoReadWriteSplittingPlugin.md) — SQL-driven routing over Global Database topology.
- [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) — shared behavior, internal connection pooling, and query-level load balancing.
