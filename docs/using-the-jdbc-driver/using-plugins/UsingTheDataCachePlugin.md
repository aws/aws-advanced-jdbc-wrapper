# Data Local Cache Plugin

The Data Local Cache Plugin caches query result sets in the JVM's own memory, keyed on the SQL text together with the database identity the query ran under. It is opt-in: nothing is cached until you set `dataCacheTriggerCondition` to a regular expression, and only statements whose SQL matches that expression are considered.

When a matching statement runs, the plugin looks for a cached result for that exact SQL string under the same identity. On a miss the statement goes to the database as usual, and the returned rows are copied into an in-memory result set which is both handed to the application and stored in the cache. On a hit the stored rows are returned in a new result set, with its own cursor, without contacting the database at all.

The cached value is a materialized copy of the rows, not the driver's own result set, so it stays usable after the statement and connection it came from are closed. The cache is process-wide and shared by every connection in the JVM, so a result cached by one connection can be served to another connection that has the same identity. See [How cached results are isolated](#how-cached-results-are-isolated).

## Plugin Availability

The plugin is available since version 1.0.0. The `dataCacheTtlMs` and `dataCacheMaxSize` parameters are available since version 4.5.0.

## When not to use this plugin

This is a read-through cache with no invalidation. Nothing tells the plugin that the underlying rows have changed, so a cached result is served until it expires or is cleared, even if a write has happened in the meantime through this connection, another connection, or another application. `dataCacheTtlMs` is therefore the longest an application can observe stale data, and it is the only bound on staleness there is.

Use it for data that is effectively static for the lifetime of the TTL, such as reference or lookup tables. Do not use it for data an application expects to read back after writing.

Only `Statement.executeQuery(String)` results are cached. The plugin reads the SQL from that argument, so `PreparedStatement`, `CallableStatement` and `Statement.execute(String)` results are never cached. Because the SQL text is part of the key, an application that inlines literals produces a distinct key per literal value. `dataCacheMaxSize` bounds the resulting memory growth, but such a workload gets few hits.

If you want a cache that is shared across processes, scales independently, and is opted into per query, see the [Remote Query Cache Plugin](./UsingTheRemoteQueryCachePlugin.md) instead.

> [!WARNING]
> `dataCache` and `remoteQueryCache` are incompatible together, do not enable both for the same connection.

## How cached results are isolated

The cache key combines the SQL text with the following, so that connections with a different database identity do not share cached results:

- the host and port the connection uses, which can be a cluster, reader or instance endpoint; connections that reach different readers through the same reader endpoint can share entries
- the catalog of the connection, and the schema if the application set one with `Connection.setSchema`
- the `user` connection property
- the authorization session state the wrapper reads from the database: the session user and the current user or role, `search_path` and the resolved search path on PostgreSQL, and the active roles and current database on MySQL and MariaDB

The wrapper reads the authorization session state when the connection opens, and again after the application runs SQL that changes it, such as `SET ROLE`, `SET search_path` or `USE`. It uses the same session-state tracking as the [Remote Query Cache Plugin](./UsingTheRemoteQueryCachePlugin.md) with `cacheEnableDatabaseMultiTenancy` enabled, and has the same limits, with one difference: when a transaction ends or auto-commit is turned back on and the state is still known, this plugin keeps it instead of reading it again, so a change the wrapper does not recognize, such as a role set inside a function, stays out of the key until the next recognized change or connection change. The tracking is best-effort hardening and is not a security boundary. It does not see session settings outside the authorization state, such as `SET TIME ZONE` or settings applied through connection options, and it does not see statement settings such as `Statement.setMaxRows`. See [Security scope and application requirements](./UsingTheRemoteQueryCachePlugin.md#security-scope-and-application-requirements).

When the wrapper cannot tell which identity a query would run under, the query bypasses the cache and goes to the database. This happens when:

- The target driver is not the PostgreSQL JDBC driver, MySQL Connector/J or MariaDB Connector/J. With any other driver nothing is cached, and the plugin logs one warning.
- The connection has run SQL whose effect on the session the wrapper cannot track, such as `SET app.tenant_id = '42'` or `set_config(...)` on PostgreSQL, `SET @tenant = 42` on MySQL, `CALL`, `DO`, `EXECUTE`, a temporary table, a `CallableStatement` or a batch. Matching skips comments but not string literals, so a string literal that contains such SQL can also turn caching off. Caching stays off until the wrapper connection is closed.
- The query reads session state that is not in the key, such as `current_setting(...)`, `SHOW` or `currval(...)`.
- The SQL contains more than one statement.
- The authorization session state is unknown. This happens after `SET ROLE`, `SET search_path`, `USE`, `setCatalog` or `setSchema` on a connection with auto-commit off, and after failover or a read/write splitting switch onto a connection with auto-commit off. It also happens after such a change inside a transaction opened with `BEGIN` or `START TRANSACTION`, when reading the state fails, and when a statement that changes it fails. Caching resumes when auto-commit is turned back on, or after the `COMMIT` or `ROLLBACK` statement that ends the transaction.

Returning a connection to an application pool such as HikariCP does not close the wrapper connection, so a connection that ran untracked SQL keeps bypassing the cache. Connections from an internal connection pool (`connectionPoolType`), from an application server's XA connection pool, or from a pooling target data source such as `PGPoolingDataSource` reuse a physical session without resetting it. A setting that an earlier connection applied, such as `SET app.tenant_id`, stays on the session, and the next connection starts with caching enabled again. Do not run untracked session SQL on connections from these pools. A commit or rollback that follows none of these changes leaves caching on. If the underlying connection changes while a query runs, the result is returned without being cached.

Queries inside a transaction are cached like any other query, so a cached result can include the transaction's own uncommitted changes, even if the transaction later rolls back, and be served to other connections with the same identity. Do not cache tables that the application writes inside transactions. The Remote Query Cache Plugin differs here: it bypasses the cache inside a transaction.

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
| `dataCacheTtlMs`            | 4.5.0                   | Long    |    No    | Time in milliseconds that a cached result stays valid. Since nothing invalidates a cached result on write, this is the longest an application can observe stale data. Set to `0` to keep entries until the cache is cleared.               | `300000`      |
| `dataCacheMaxSize`          | 4.5.0                   | Long    |    No    | Maximum number of distinct cache keys held in the cache. Once the limit is reached, expired entries are purged first; if the cache is still full, further results are returned to the application without being cached. Set to `0` for no limit. | `1000`        |

Both `dataCacheTtlMs` and `dataCacheMaxSize` must be zero or positive. A negative value throws an `IllegalArgumentException` when the property is read rather than being interpreted as "unlimited"; `0` is the value that opts out of expiration or the size limit.

> [!WARNING]\
> Setting `dataCacheTtlMs=0` and `dataCacheMaxSize=0` together leaves the cache unbounded in both staleness and size. That was the behaviour before version 4.5.0 and is not recommended.

## Clearing the cache

`software.amazon.jdbc.plugin.cache.DataLocalCacheConnectionPlugin.clearCache()` empties the cache for the whole JVM. It is also called by `software.amazon.jdbc.Driver.releaseResources()`.

## Telemetry

When [telemetry](../Telemetry.md) is enabled and a metrics backend is configured through `telemetryMetricsBackend`, the plugin submits the following metrics:

| Metric name                  | Metric type | Description                                                                             |
|------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `dataCache.cache.totalCalls` | Counter     | Number of calls that return a result set, including calls whose SQL did not match `dataCacheTriggerCondition` and calls that bypassed the cache. |
| `dataCache.cache.hit`        | Counter     | Number of calls served from the cache.                                                   |
| `dataCache.cache.miss`       | Counter     | Number of cacheable calls with no usable cached result, including expired ones. Calls that bypassed the cache are not counted. |
| `dataCache.cache.size`       | Gauge       | Current number of entries in the cache, including entries that have expired but not yet been removed. |
