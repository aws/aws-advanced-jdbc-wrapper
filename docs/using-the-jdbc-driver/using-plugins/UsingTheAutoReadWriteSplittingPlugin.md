# Automatic Read/Write Splitting Plugin

The Automatic Read/Write Splitting Plugin (`autoReadWriteSplitting`) routes individual queries to a writer or reader instance automatically, based on an analysis of the SQL statement being executed. Unlike the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) — which switches connections only when the application calls `Connection#setReadOnly` — this plugin inspects each statement and decides where to run it without any application code changes.

The routing rules are:

- `SELECT` statements are routed to a reader instance (according to the configured [reader selection strategy](../HostSelectionStrategies.md)).
- `INSERT`, `UPDATE`, `DELETE`, and DDL statements are routed to the writer.
- `SELECT ... FOR UPDATE` (and other row-locking `SELECT` variants such as `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`) are routed to the writer, because they take row locks.
- While a transaction is in progress, or while autocommit is disabled, the connection is **not** re-routed — the statement runs on whichever instance the connection is currently using. This preserves transactional guarantees, since a connection cannot be switched in the middle of a transaction.

This plugin extends the Read/Write Splitting Plugin, so it inherits the same connection-switching, reader-selection, session-state-transfer, and internal-connection-pooling behavior described in the [Read/Write Splitting Plugin guide](./UsingTheReadWriteSplittingPlugin.md). This document focuses on the behavior that is specific to automatic routing.

> [!WARNING]
> Do not use the `autoReadWriteSplitting`, `readWriteSplitting`, `srw`, and/or `gdbReadWriteSplitting` plugins (or any combination of them) at the same time for the same connection. They are all read/write splitting plugins and will conflict.

## Loading the Automatic Read/Write Splitting Plugin

The plugin is not loaded by default. To load it, include `autoReadWriteSplitting` in the `wrapperPlugins` connection parameter. The plugin depends on SQL parse results produced by the `sqlParser` plugin, which **must be listed before** `autoReadWriteSplitting` in the plugin chain.

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,autoReadWriteSplitting");
```

When loading alongside the failover and host monitoring plugins, the read/write splitting plugins should be listed before those plugins so that failover exceptions are processed correctly:

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,autoReadWriteSplitting,failover2,efm2");
```

The driver performs plugin sorting by default (see the [`autoSortWrapperPluginOrder` configuration parameter](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters)), which keeps `sqlParser` ahead of `autoReadWriteSplitting`.

## Required dependency: JSQLParser

The `sqlParser` plugin uses [JSQLParser](https://github.com/JSQLParser/JSqlParser) to analyze SQL statements. JSQLParser is an optional dependency of the wrapper and is not bundled into the driver jar, so you must add it to your project.

**Maven:**
```xml
<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>4.9</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'com.github.jsqlparser:jsqlparser:4.9'
```

If JSQLParser is not on the runtime classpath, queries will fail with a `NoClassDefFoundError` for `net/sf/jsqlparser/...`.

## Supplying the connection string

As with the Read/Write Splitting Plugin, you do not need to supply multiple instance URLs when using this plugin against Aurora clusters. Supply the URL for the initial instance you are connecting to. The plugin requires cluster topology, so include either the failover plugin or another plugin that queries Aurora for its topology. See [Supplying the connection string](./UsingTheReadWriteSplittingPlugin.md#supplying-the-connection-string) for details.

## Routing hints

You can override the automatic routing decision for an individual statement by prefixing the SQL with a routing hint comment. Hints take priority over the query-type analysis.

| Hint            | Effect                                                                                     |
|-----------------|--------------------------------------------------------------------------------------------|
| `/*@reader*/`   | Force the statement to a reader instance.                                                  |
| `/*@writer*/`   | Force the statement to the writer instance.                                                |
| `/*@keep*/`     | Run the statement on the current connection without re-routing (writer or reader, as-is).  |

The hint is matched case-insensitively and is stripped from the SQL before it is sent to the database.

```java
// Force a SELECT to the writer (e.g. to read your own recent write):
stmt.executeQuery("/*@writer*/ SELECT * FROM orders WHERE id = 42");

// Force a query to a reader:
stmt.executeQuery("/*@reader*/ SELECT count(*) FROM events");

// Run on whatever connection is currently active, without switching:
stmt.executeQuery("/*@keep*/ SELECT @@server_id");
```

The `/*@keep*/` hint is useful when you want to observe or operate on the current connection without perturbing routing — for example, a diagnostic query that should report the instance currently in use rather than triggering a reader/writer switch.

> [!WARNING]
> Routing hints are parsed from SQL comments. If your application builds SQL by concatenating user-supplied input, an attacker could inject a routing hint. Always use parameterized queries for user input so that hints cannot be injected through it.

## Query-level load balancing

By default, once the connection has switched to a reader it stays on that single reader for all subsequent read queries. Enabling query-level load balancing makes the plugin select a reader **per read query**, spreading reads across the available reader instances.

These parameters control *when* a new reader is selected (per query vs. sticky) — they do not change *how* a reader is picked. Reader selection continues to use the configured [`readerHostSelectorStrategy`](../HostSelectionStrategies.md), and any supported strategy applies.

| Parameter                     | Default | Description                                                                                                                                                 |
|-------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `queryLevelLoadBalancing`     | `false` | When `true`, a read query routed to a reader triggers a per-query reader selection (using `readerHostSelectorStrategy`) instead of reusing the single cached reader. |
| `loadBalancingIncludeWriter`  | `false` | When `true` (and `queryLevelLoadBalancing` is enabled), the writer instance is also included in the pool of balancing candidates.                            |

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,autoReadWriteSplitting");
properties.setProperty("queryLevelLoadBalancing", "true");
// Reader selection uses readerHostSelectorStrategy; any supported strategy works (default: random).
properties.setProperty("readerHostSelectorStrategy", "roundRobin");
// Optional: also send some reads to the writer
// properties.setProperty("loadBalancingIncludeWriter", "true");
```

Behavior notes:

- Only queries that are already routed to a reader are balanced. Writes, `SELECT ... FOR UPDATE`, `/*@writer*/`-hinted, and `/*@keep*/`-hinted statements are unaffected.
- Balancing is suppressed under the same conditions as normal routing: while a transaction is open or autocommit is disabled, the statement stays on the current connection. This preserves transactional guarantees.
- This is a feature of the `autoReadWriteSplitting` plugin only. The `readWriteSplitting`, `srw`, and `gdbReadWriteSplitting` plugins are not affected by these parameters.

> [!IMPORTANT]
> Query-level load balancing can switch the physical connection on every read query. Enabling the [internal connection pool](./UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) is strongly recommended so that switching reuses pooled connections instead of opening a new physical connection per query. Without it, high query rates can cause significant connection churn.

> [!NOTE]
> Because balancing can move a read to a different reader between statements, do not rely on read-your-own-write consistency across separately balanced read queries. Use a transaction, `/*@keep*/`, or `/*@writer*/` when a sequence of statements must observe a consistent view.

## Transactions and autocommit

A query is kept on the current connection (no re-routing) when either of the following is true:

- A transaction is already open, or
- Autocommit is disabled (`Connection#setAutoCommit(false)`), since the next statement will implicitly begin a transaction.

This is required because switching the underlying physical connection in the middle of a transaction would break the transaction. As a result:

- If autocommit is disabled while the connection is on the writer (the typical case — autocommit is usually set before the first query), all statements in the transaction run on the writer.
- A read-only transaction that begins while the connection is already on a reader continues on that reader.

If you need a specific role for a transaction, establish it before the transaction begins — for example, issue a `/*@writer*/` or `/*@reader*/` statement (or call `setReadOnly`) before calling `setAutoCommit(false)`.

## Limitations

### Statements are bound to a connection

As with the Read/Write Splitting Plugin, a `Statement` or `ResultSet` is internally bound to the database connection that was active when it was created. If automatic routing switches the connection, statements created before the switch continue to use the previous connection. Create new `Statement`/`ResultSet` objects after a routing change. See [General plugin limitations](./UsingTheReadWriteSplittingPlugin.md#general-plugin-limitations) for more detail.

### Callable and unparseable statements

If a statement's SQL cannot be parsed, no parse result is available and the plugin falls back to keeping the query on the writer to be safe. A common example is a `CallableStatement` that invokes a stored procedure: the driver sees only the call escape sequence (for example `{call get_order_summary(?)}`), not the statements executed inside the procedure, so it cannot determine whether the call only reads data. Such calls therefore run on the writer, even if the procedure is read-only. The same fallback applies to any statement that carries no parseable SQL text.

### Inherited limitations

All limitations of the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md#limitations) apply, including session-state transfer behavior and the Spring `@Transactional(readOnly = true)` considerations.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Internal connection pooling and reader selection

The plugin inherits internal connection pooling and reader selection from the Read/Write Splitting Plugin. Enabling the internal connection pool is strongly recommended, because automatic routing can switch connections frequently. See [Internal connection pooling](./UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) and [Reader Selection](./UsingTheReadWriteSplittingPlugin.md#reader-selection) for configuration details.
