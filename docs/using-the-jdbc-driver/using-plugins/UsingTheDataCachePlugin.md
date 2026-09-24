# Data Local Cache Plugin

The Data Local Cache Plugin caches query result sets in the JVM's own memory, keyed on the text of the SQL statement. It is opt-in: nothing is cached until you set `dataCacheTriggerCondition` to a regular expression, and only statements whose SQL matches that expression are considered.

When a matching statement runs, the plugin looks for a cached result for that exact SQL string. On a miss the statement goes to the database as usual, and the returned rows are copied into an in-memory result set which is both handed to the application and stored in the cache. On a hit the stored result set is rewound and returned without contacting the database at all.

The cached value is a materialized copy of the rows, not the driver's own result set, so it stays usable after the statement and connection it came from are closed. The cache is process-wide and shared by every connection in the JVM, so a result cached by one connection can be served to another.

## Plugin Availability

The plugin is available since version 1.0.0. The `dataCacheTtlMs` and `dataCacheMaxSize` parameters are available since version 4.4.0.

## When not to use this plugin

This is a read-through cache with no invalidation. Nothing tells the plugin that the underlying rows have changed, so a cached result is served until it expires or is cleared, even if a write has happened in the meantime through this connection, another connection, or another application. `dataCacheTtlMs` is therefore the longest an application can observe stale data, and it is the only bound on staleness there is.

Use it for data that is effectively static for the lifetime of the TTL, such as reference or lookup tables. Do not use it for data an application expects to read back after writing.

Because the cache key is the raw SQL text, an application that inlines literals instead of binding parameters produces a distinct key per literal value. `dataCacheMaxSize` bounds the resulting memory growth, but such a workload gets few hits and is better served by binding parameters.

If you want a cache that is shared across processes, scales independently, and is opted into per query, see the [Remote Query Cache Plugin](./UsingTheRemoteQueryCachePlugin.md) instead.

## Using the Data Local Cache Plugin

The plugin is not loaded by default. Include `dataCache` in the `wrapperPlugins` connection parameter and set a trigger condition.

```java
final Properties props = new Properties();
props.setProperty(PropertyDefinition.PLUGINS.name, "dataCache");

// Cache results only for statements that read from the currency_rates table.
props.setProperty("dataCacheTriggerCondition", ".*currency_rates.*");
props.setProperty("dataCacheTtlMs", "60000");
props.setProperty("dataCacheMaxSize", "200");

Connection conn = DriverManager.getConnection(
    "jdbc:aws-wrapper:postgresql://mydb.amazonaws.com:5432/postgres", props);
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT code, rate FROM currency_rates");
```

`dataCacheTriggerCondition` is matched against the whole statement with `String.matches`, so the expression has to match the entire SQL text — hence the leading and trailing `.*` above.

## Configuration Parameters

| Parameter                   | Available Since Version | Value   | Required | Description                                                                                                                                                                                                                              | Default Value |
|-----------------------------|-------------------------|:-------:|:--------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `dataCacheTriggerCondition` | 1.0.0                   | String  |   Yes    | A regular expression matched against the full SQL text. Results are cached only for statements that match. When unset, the plugin caches nothing.                                                                                         | `null`        |
| `dataCacheTtlMs`            | 4.4.0                   | Long    |    No    | Time in milliseconds that a cached result stays valid. Since nothing invalidates a cached result on write, this is the longest an application can observe stale data. Set to `0` to keep entries until the cache is cleared.               | `300000`      |
| `dataCacheMaxSize`          | 4.4.0                   | Long    |    No    | Maximum number of distinct SQL statements held in the cache. Once the limit is reached, expired entries are purged first; if the cache is still full, further results are returned to the application without being cached. Set to `0` for no limit. | `1000`        |

Both `dataCacheTtlMs` and `dataCacheMaxSize` must be zero or positive. A negative value throws an `IllegalArgumentException` when the property is read rather than being interpreted as "unlimited"; `0` is the value that opts out of expiration or the size limit.

> [!WARNING]\
> Setting `dataCacheTtlMs=0` and `dataCacheMaxSize=0` together leaves the cache unbounded in both staleness and size. That was the behaviour before version 4.4.0 and is not recommended.

## Clearing the cache

`software.amazon.jdbc.plugin.cache.DataLocalCacheConnectionPlugin.clearCache()` empties the cache for the whole JVM. It is also called by `software.amazon.jdbc.Driver.releaseResources()`.

## Telemetry

When [telemetry](../Telemetry.md) is enabled and a metrics backend is configured through `telemetryMetricsBackend`, the plugin submits the following metrics:

| Metric name                  | Metric type | Description                                                                             |
|------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `dataCache.cache.totalCalls` | Counter     | Number of result-set-returning calls the plugin considered.                              |
| `dataCache.cache.hit`        | Counter     | Number of calls served from the cache.                                                   |
| `dataCache.cache.miss`       | Counter     | Number of matching calls with no usable cached result, including expired ones.            |
| `dataCache.cache.size`       | Gauge       | Current number of entries in the cache, including entries that have expired but not yet been removed. |
