# Automatic Read/Write Splitting Plugin

The Automatic Read/Write Splitting Plugin (`autoReadWriteSplitting`) routes individual queries to a writer or reader instance automatically, based on an analysis of the SQL statement being executed. Unlike the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) — which switches connections only when the application calls `Connection#setReadOnly` — this plugin inspects each statement and decides where to run it without any application code changes.

The routing rules are:

- `SELECT` statements are routed to a reader instance (according to the configured [reader selection strategy](../HostSelectionStrategies.md)).
- `INSERT`, `UPDATE`, `DELETE`, and DDL statements are routed to the writer.
- `SELECT ... FOR UPDATE` (and other row-locking `SELECT` variants such as `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`) are routed to the writer, because they take row locks.
- While a transaction is in progress, or while autocommit is disabled, the connection is **not** re-routed — the statement runs on whichever instance the connection is currently using. This preserves transactional guarantees, since a connection cannot be switched in the middle of a transaction. See [Transactions and autocommit](#transactions-and-autocommit), which also covers the `assumeWriteTransaction` parameter for transactions that read before they write.

This plugin extends the Read/Write Splitting Plugin, so it inherits the same connection-switching, reader-selection, session-state-transfer, and internal-connection-pooling behavior described in the [Read/Write Splitting Plugin guide](./UsingTheReadWriteSplittingPlugin.md). This document focuses on the behavior that is specific to automatic routing.

> [!WARNING]
> Use exactly one read/write splitting plugin per connection. Do not combine `autoReadWriteSplitting` with any other read/write splitting plugin — `readWriteSplitting`, `srw`, `autoSimpleReadWriteSplitting`, `gdbReadWriteSplitting`, `gdbAutoReadWriteSplitting`, `gdbSimpleReadWriteSplitting`, or `gdbAutoSimpleReadWriteSplitting` — for the same connection. They are all read/write splitting plugins and will conflict.

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

A write is the one exception: it is allowed to switch to the writer while autocommit is disabled but before any statement has run, because at that point no transaction exists yet and a write cannot be served by a read-only reader.

If you need a specific role for a transaction, establish it before the transaction begins — for example, issue a `/*@writer*/` or `/*@reader*/` statement (or call `setReadOnly`) before calling `setAutoCommit(false)`.

### Transaction managers do not signal write intent

Deciding the role for a transaction is only possible if the driver can tell a read-only transaction from a read-write one. In practice it cannot, because transaction managers announce only one of the two:

| Transaction | What the driver observes |
|-------------|--------------------------|
| Spring `@Transactional(readOnly = true)` | `setReadOnly(true)`, then `setAutoCommit(false)` |
| Spring `@Transactional` (the default, `readOnly = false`) | `setAutoCommit(false)` only |
| MyBatis, jOOQ, plain JDBC | `setAutoCommit(false)` only |

A read-write transaction is expressed by the *absence* of a read-only declaration, and that absence is indistinguishable from a plain non-transactional connection. So when a read-write transaction begins with a read — the common `findAll()`-then-`save()` shape of a default `@Transactional` service method — this plugin routes that first read to a reader, the transaction physically begins there, and the write that follows cannot be served: the connection is already pinned to the reader by the rules above, and the reader rejects the write (for example MySQL error 1290, `--super-read-only`).

Whether this happens depends on where the connection happens to be when the transaction starts. A connection that is already on the writer stays there for the whole transaction, so the problem appears intermittently — typically after a pooled connection was left on a reader by an earlier read-only phase.

### `assumeWriteTransaction`

Set `assumeWriteTransaction=true` to close that gap. The plugin then treats a transaction with no explicit read-only declaration as read-write, and routes its reads to the writer so the whole transaction starts and stays there.

| Parameter                 | Default | Description                                                                                                                                                             |
|---------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `assumeWriteTransaction`  | `false` | When `true`, a read that belongs to a transaction which was not declared read-only via `setReadOnly(true)` is routed to the writer instead of a reader.                    |

```java
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "sqlParser,autoReadWriteSplitting");
properties.setProperty("assumeWriteTransaction", "true");
```

Precisely, a read is routed to the writer when all of the following hold:

- `assumeWriteTransaction` is enabled, and
- the connection was not declared read-only (`setReadOnly(true)` was never called, or `setReadOnly(false)` was called), and
- a transaction is open, or autocommit is disabled so the next statement will open one.

Everything else is unchanged:

- Reads outside a transaction (autocommit on) are still offloaded to readers.
- Read-only transactions still run on a reader, because `setReadOnly(true)` states read intent explicitly.
- An explicit `/*@reader*/` or `/*@keep*/` hint still wins, so an individual read inside a read-write transaction can be sent to a reader deliberately.
- Writes, `SELECT ... FOR UPDATE`, and unparseable statements already route to the writer.

The trade-off is direct: transactional reads are no longer offloaded to readers unless the transaction is marked read-only. This is why the setting is opt-in. It is recommended for Spring/JPA applications, where `@Transactional(readOnly = true)` is available to mark read-only work, and for any application that wraps mixed read-then-write logic in a transaction. It applies to the SQL-routing plugins (`autoReadWriteSplitting`, `autoSimpleReadWriteSplitting`, `gdbAutoReadWriteSplitting`, `gdbAutoSimpleReadWriteSplitting`); the `setReadOnly`-driven plugins (`readWriteSplitting`, `srw`, `gdbReadWriteSplitting`, `gdbSimpleReadWriteSplitting`) ignore it, since they only ever route where the application tells them to.

### Recommended call sequence

Routing has to be decided while the connection is still uncommitted to a node: no transaction open, autocommit still on. That gives one ordering rule — declare the role first, disable autocommit second, run statements third, and restore after `commit()`.

**Spring / JPA.** Annotate read-only service methods, and leave writing or mixed methods on the default:

```java
@Transactional(readOnly = true)          // -> setReadOnly(true) before the transaction starts; reads go to a reader
public List<Order> listOrders() { ... }

@Transactional                            // -> read-write; with assumeWriteTransaction=true this runs on the writer
public Order createOrder(...) {
    orderRepository.findAll();            // a leading read no longer moves the transaction to a reader
    return orderRepository.save(order);
}
```

Two Hibernate settings help the read-only case reach the driver reliably: `hibernate.connection.handling_mode=DELAYED_ACQUISITION_AND_HOLD` keeps one connection for the whole transaction, so `setReadOnly(true)` is applied once and stays applied. Enable wrapper trace logging to confirm the call arrives.

**Plain JDBC.** Make the intent explicit in both directions, since no framework does it for you:

```java
// Read-only transaction
conn.setReadOnly(true);        // routes to a reader while nothing is open yet
conn.setAutoCommit(false);
// ... reads ...
conn.commit();
conn.setAutoCommit(true);
conn.setReadOnly(false);       // restore after the transaction ends, never during it

// Read-write transaction
conn.setReadOnly(false);       // explicit writer intent; a no-op if already on the writer
conn.setAutoCommit(false);
// ... reads and writes ...
conn.commit();
conn.setAutoCommit(true);
```

Two ordering details matter:

- Call `setReadOnly` **before** `setAutoCommit(false)`. With autocommit already off, a switch to a reader is blocked by the transaction guard, so a read-only transaction set up in the reverse order silently runs on the writer.
- Do not call `setReadOnly` while a transaction is open. On a reader this raises `setReadOnly(false) was called on a read-only connection inside a transaction` (SQL state 25001), and the JDBC specification allows drivers to reject it outright.

With `setReadOnly(false)` called up front, a read-write transaction is routed correctly even without `assumeWriteTransaction`; the setting exists for the code paths where you cannot add that call, such as a framework-managed transaction.

## Query-level load balancing and statement rebinding

Because this plugin treats every read statement as a routing point, it pairs naturally with query-level load balancing. Set `queryLevelLoadBalancing=true` to spread consecutive `SELECT`s across different readers (use `readerHostSelectorStrategy=roundRobin` for deterministic rotation). Statement rebinding (`allowStatementRecreationOnConnectionSwitch`, on by default) then re-creates a re-executed `PreparedStatement`/`CallableStatement` on the newly selected reader — replaying its recorded settings, bound parameters, and registered OUT parameters — so re-executes follow the rotation instead of staying pinned to the reader chosen at prepare time. Writes never rotate. See [Query-level load balancing](./UsingTheReadWriteSplittingPlugin.md#query-level-load-balancing) for the full description, defaults, and fallback behavior (stream/LOB parameters and pending batches are not rebindable).

## Limitations

### Statements are bound to a connection

As with the Read/Write Splitting Plugin, a `Statement` or `ResultSet` is internally bound to the database connection that was active when it was created. If automatic routing switches the connection, statements created before the switch continue to use the previous connection unless statement rebinding applies (see [Query-level load balancing and statement rebinding](#query-level-load-balancing-and-statement-rebinding)). Otherwise, create new `Statement`/`ResultSet` objects after a routing change. See [General plugin limitations](./UsingTheReadWriteSplittingPlugin.md#general-plugin-limitations) for more detail.

### Callable and unparseable statements

If a statement's SQL cannot be parsed, no parse result is available and the plugin falls back to keeping the query on the writer to be safe. A common example is a `CallableStatement` that invokes a stored procedure: the driver sees only the call escape sequence (for example `{call get_order_summary(?)}`), not the statements executed inside the procedure, so it cannot determine whether the call only reads data. Such calls therefore run on the writer, even if the procedure is read-only. The same fallback applies to any statement that carries no parseable SQL text.

### Inherited limitations

All limitations of the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md#limitations) apply, including session-state transfer behavior and the Spring `@Transactional(readOnly = true)` considerations.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Internal connection pooling and reader selection

The plugin inherits internal connection pooling and reader selection from the Read/Write Splitting Plugin. Enabling the internal connection pool is strongly recommended, because automatic routing can switch connections frequently. See [Internal connection pooling](./UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) and [Reader Selection](./UsingTheReadWriteSplittingPlugin.md#reader-selection) for configuration details.
