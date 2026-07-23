# Global Database (GDB) Simple Read/Write Splitting Plugin

The GDB Simple Read/Write Splitting Plugin (`gdbSimpleReadWriteSplitting`) combines the two-endpoint model of the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md) with the Aurora Global Database region rules of the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md).

Like the Simple Read/Write Splitting Plugin, it does not rely on cluster topology: it routes between two endpoints you supply on calls to `Connection#setReadOnly` (`setReadOnly(true)` connects to the read endpoint, `setReadOnly(false)` to the write endpoint). In addition, it applies the Global Database home-region rules and Global Write Forwarding to the configured **write endpoint**, so a write endpoint outside the accessible/home region is rejected (or, with Global Write Forwarding enabled, write queries can be forwarded to the primary region).

Use this plugin for Global Database deployments where you connect through fixed read/write endpoints (for example RDS Proxy or custom routing) rather than querying Aurora topology, but still need home-region constraints.

## Plugin Availability

This plugin is part of the unified read/write splitting plugin family. See the CHANGELOG for the release it first appears in.

> [!WARNING]
> Use exactly one read/write splitting plugin per connection. Do not combine `gdbSimpleReadWriteSplitting` with any other read/write splitting plugin — `readWriteSplitting`, `autoReadWriteSplitting`, `srw`, `autoSimpleReadWriteSplitting`, `gdbReadWriteSplitting`, `gdbAutoReadWriteSplitting`, or `gdbAutoSimpleReadWriteSplitting` — for the same connection. They are all read/write splitting plugins and will conflict.

## Loading the GDB Simple Read/Write Splitting Plugin

The plugin is not loaded by default. To load it, include `gdbSimpleReadWriteSplitting` in the `wrapperPlugins` connection parameter along with the two required endpoints.

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "gdbSimpleReadWriteSplitting");
properties.setProperty("srwWriteEndpoint", "test-db.cluster-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("srwReadEndpoint", "test-db.cluster-ro-XYZ.us-east-2.rds.amazonaws.com");
properties.setProperty("gdbRwHomeRegion", "us-east-2");
```

## Configuration Parameters

This plugin accepts the endpoint parameters of the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#simple-readwrite-splitting-plugin-parameters):

| Parameter | Value | Required | Description | Default Value |
|---|:---:|:---:|---|---|
| `srwWriteEndpoint` | String | Yes | The endpoint to connect to when `setReadOnly(false)` is called. | `null` |
| `srwReadEndpoint` | String | Yes | The endpoint to connect to when `setReadOnly(true)` is called. | `null` |
| `verifyNewSrwConnections` | Boolean | No | Enables writer/reader verification for new connections. | `true` |
| `verifyInitialConnectionType` | String | No | If `verifyNewSrwConnections` is `true`, verifies the initial opened connection to be a `writer` or `reader`. | `null` |
| `srwConnectRetryTimeoutMs` | Integer | No | Maximum allowed time in milliseconds for retrying connection attempts. | `60000` |
| `srwConnectRetryIntervalMs` | Integer | No | Time delay in milliseconds between connection retries. | `1000` |
| `cachedReaderKeepAliveTimeoutMs` | Integer | No | Timeout for the cached reader connection. `0` reuses the same cached reader for the lifetime of the `Connection`. | `0` |

In addition, the Global Database region parameters of the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md#configuration-parameters) apply to the configured write endpoint: `gdbRwHomeRegion`, `gdbRwRestrictWriterToHomeRegion`, `gdbRwRestrictReaderToHomeRegion`, `gdbEnableGlobalWriteForwarding`, and `gdbAccessibleRegions`. See that page for full descriptions and defaults.

> [!NOTE]
> For an in-depth explanation of `gdbAccessibleRegions`, see [Restricting Aurora Global Database Access by Region](./UsingGlobalAuroraAccessibleRegions.md).

The family-wide `queryLevelLoadBalancing` and `loadBalancingIncludeWriter` parameters are also accepted — see [Query-level load balancing](./UsingTheReadWriteSplittingPlugin.md#query-level-load-balancing).

## Limitations

All limitations of the [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md#limitations) (including the autocommit interaction with connection verification and statement/`ResultSet` binding) and the region behavior of the [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md) apply.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Related pages

- [Simple Read/Write Splitting Plugin](./UsingTheSimpleReadWriteSplittingPlugin.md) — two-endpoint routing without region rules.
- [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md) — topology-based routing with home-region rules.
- [GDB Automatic Simple Read/Write Splitting Plugin](./UsingTheGdbAutoSimpleReadWriteSplittingPlugin.md) — the SQL-routed variant of this plugin.
