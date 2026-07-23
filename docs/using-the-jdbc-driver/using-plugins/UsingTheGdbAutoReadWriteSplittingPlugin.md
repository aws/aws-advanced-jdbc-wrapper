# Global Database (GDB) Automatic Read/Write Splitting Plugin

The GDB Automatic Read/Write Splitting Plugin (`gdbAutoReadWriteSplitting`) combines the automatic, SQL-driven routing of the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md) with the Aurora Global Database home-region awareness of the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md).

It discovers reader and writer instances from the Global Database topology and constrains connections to the configured home region, but decides *which* role to route each statement to by analyzing the SQL rather than requiring the application to call `Connection#setReadOnly`:

- `SELECT` statements are routed to a reader instance (according to the configured [reader selection strategy](../HostSelectionStrategies.md)).
- `INSERT`, `UPDATE`, `DELETE`, and DDL statements are routed to the writer.
- `SELECT ... FOR UPDATE` (and other row-locking `SELECT` variants) are routed to the writer.
- The `/*@reader*/`, `/*@writer*/`, and `/*@keep*/` routing hints are honored, exactly as in the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md#routing-hints).
- While a transaction is open or autocommit is disabled, the connection is pinned (no re-routing).

Explicit `Connection#setReadOnly` calls are still honored, so applications may mix explicit toggles with automatic routing.

## Plugin Availability

This plugin is part of the unified read/write splitting plugin family. See the CHANGELOG for the release it first appears in.

> [!WARNING]
> Use exactly one read/write splitting plugin per connection. Do not combine `gdbAutoReadWriteSplitting` with any other read/write splitting plugin — `readWriteSplitting`, `autoReadWriteSplitting`, `srw`, `autoSimpleReadWriteSplitting`, `gdbReadWriteSplitting`, `gdbSimpleReadWriteSplitting`, or `gdbAutoSimpleReadWriteSplitting` — for the same connection. They are all read/write splitting plugins and will conflict.

## Loading the GDB Automatic Read/Write Splitting Plugin

The plugin is not loaded by default. To load it, include `gdbAutoReadWriteSplitting` in the `wrapperPlugins` connection parameter. Because routing is driven by SQL analysis, the `sqlParser` plugin **must be listed before** it, and the `com.github.jsqlparser:jsqlparser` dependency must be on the classpath (see [Required dependency: JSQLParser](./UsingTheAutoReadWriteSplittingPlugin.md#required-dependency-jsqlparser)).

If you load it alongside the failover and host monitoring plugins, the read/write splitting plugin **must be listed before** them so failover exceptions are processed correctly:

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,gdbAutoReadWriteSplitting,failover2,efm2");
```

The driver performs plugin sorting by default (see the [`autoSortWrapperPluginOrder` configuration parameter](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters)), which keeps `sqlParser` ahead of `gdbAutoReadWriteSplitting`.

## Configuration Parameters

This plugin accepts the same parameters as the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md#configuration-parameters), including `readerHostSelectorStrategy`, `cachedReaderKeepAliveTimeoutMs`, `gdbRwHomeRegion`, `gdbRwRestrictWriterToHomeRegion`, `gdbRwRestrictReaderToHomeRegion`, `gdbEnableGlobalWriteForwarding`, and `gdbAccessibleRegions`. See that page for the full descriptions and defaults.

> [!NOTE]
> For an in-depth explanation of `gdbAccessibleRegions`, see [Restricting Aurora Global Database Access by Region](./UsingGlobalAuroraAccessibleRegions.md). The topology monitor also honors [`gdbMonitoringConnectionPriority`](./UsingMonitoringConnectionPriority.md).

It also accepts the family-wide `queryLevelLoadBalancing`, `loadBalancingIncludeWriter`, and `allowStatementRecreationOnConnectionSwitch` parameters — see [Query-level load balancing](./UsingTheReadWriteSplittingPlugin.md#query-level-load-balancing). Because each read statement is a routing point, query-level load balancing rotates reads per query.

## Limitations

All limitations of the [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md#limitations) (including the handling of `CallableStatement` and unparseable SQL) and the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md) apply.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Related pages

- [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md) — `setReadOnly`-driven routing over Global Database topology.
- [Automatic Read/Write Splitting Plugin](./UsingTheAutoReadWriteSplittingPlugin.md) — SQL-driven routing over a single-region cluster.
- [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) — shared behavior, internal connection pooling, and query-level load balancing.
