# Performance Bug: AuroraConnectionTrackerPlugin executes alias queries on every getConnection() with internal pooling

## Summary

When using `HikariPooledConnectionProvider` (internal pooling) with cluster endpoints, `AuroraConnectionTrackerPlugin.connect()` executes unnecessary SQL queries (`SELECT CONCAT(@@hostname, ':', @@port)`, `SELECT @@aurora_server_id`) on **every** `getConnection()` call, even when borrowing an existing connection from the pool.

This results in 4-5 extra database round-trips per logical connection acquisition, significantly impacting latency for high-throughput applications.

## Environment

- AWS Advanced JDBC Driver version: 2.x
- JDK version: 17+
- Database: Aurora MySQL
- Connection pool: HikariPooledConnectionProvider (internal pooling)
- Plugins: `auroraConnectionTracker,failover2,efm2,customEndpoint,initialConnection`

## Reproduction

The bug manifests when:
1. Using internal pooling (`HikariPooledConnectionProvider`) - enabled by `customEndpoint` + `initialConnection` plugins
2. Connecting via cluster endpoints (writer, reader, or custom cluster endpoints where `isRdsCluster()=true`)

### Minimal reproduction

```java
// Using custom endpoint with internal pooling
Properties props = new Properties();
props.setProperty("wrapperPlugins", "auroraConnectionTracker,failover2,efm2,customEndpoint,initialConnection");
props.setProperty("user", USERNAME);
props.setProperty("password", PASSWORD);
props.setProperty("readerInitialConnectionHostSelectorStrategy", "leastConnections");

String url = "jdbc:aws-wrapper:mysql://my-cluster.cluster-custom-xyz.us-east-1.rds.amazonaws.com/mydb";

// Each getConnection() triggers alias queries, even when reusing pooled connections
for (int i = 0; i < 10; i++) {
    try (Connection conn = DriverManager.getConnection(url, props)) {
        // Observe: SELECT CONCAT(@@hostname, ':', @@port) and SELECT @@aurora_server_id
        // are executed on EVERY iteration, not just the first
    }
}
```

### Unit test demonstrating the bug

See attached test: `AuroraConnectionTrackerPluginAliasQueryBugTest.java`

The test demonstrates that calling `plugin.connect()` multiple times with the same `Connection` instance and cluster `HostSpec` triggers `fillAliases()` on every call.

## Root Cause Analysis

The bug is in `AuroraConnectionTrackerPlugin.connect()` ([source](https://github.com/aws/aws-advanced-jdbc-wrapper/blob/main/wrapper/src/main/java/software/amazon/jdbc/plugin/AuroraConnectionTrackerPlugin.java#L91-L95)):

```java
final RdsUrlType type = this.rdsHelper.identifyRdsType(hostSpec.getHost());
if (type.isRdsCluster() || type == RdsUrlType.OTHER || type == RdsUrlType.IP_ADDRESS) {
    hostSpec.resetAliases();  // Always clears cache
    this.pluginService.fillAliases(conn, hostSpec);  // Always re-queries
}
```

**Problems:**
1. `resetAliases()` is called unconditionally for cluster endpoints
2. This defeats the existing optimization in `PluginServiceImpl.fillAliases()` which checks `if (!hostSpec.getAliases().isEmpty())`
3. There is no guard to skip alias queries when reusing the same physical connection

### Why this doesn't affect standard (external) pooling

With external HikariCP pooling (wrapping `AwsWrapperDataSource`):
- The plugin chain only runs when HikariCP creates a **new physical connection**
- Subsequent pool borrows don't invoke the AWS wrapper at all
- Alias queries only run once per physical connection (expected behavior)

With internal pooling (`HikariPooledConnectionProvider`):
- The plugin chain runs on **every** `getConnection()` call
- `HikariPooledConnectionProvider.connect()` returns pooled connections but the upstream plugin chain still executes
- Alias queries run on every borrow (unexpected, wasteful)

## Impact

- **Latency**: 4-5 extra SQL queries per `getConnection()` call
- **Database load**: Unnecessary queries to Aurora instances
- **Affected configurations**: Any setup using `customEndpoint` + `initialConnection` plugins for leastConnections load balancing with custom Aurora endpoints

## Proposed Fix

### Option 1: Fix at the plugin level (recommended)

Make `AuroraConnectionTrackerPlugin.connect()` idempotent by not unconditionally resetting aliases:

```java
final RdsUrlType type = this.rdsHelper.identifyRdsType(hostSpec.getHost());
if (type.isRdsCluster() || type == RdsUrlType.OTHER || type == RdsUrlType.IP_ADDRESS) {
    // Only fill aliases if not already populated for this connection
    if (hostSpec.getAliases().isEmpty()) {
        this.pluginService.fillAliases(conn, hostSpec);
    }
}
```

Or track connection identity to detect reused physical connections.

### Option 2: Fix at the internal pooling architecture level

Change `HikariPooledConnectionProvider` so that pool borrows don't run the full plugin chain - more closely mirroring external HikariCP behavior.

## Design Question for Maintainers

This bug raises a broader architectural question:

**Is `plugin.connect()` expected to be called multiple times for the same physical connection?**

- **If yes**: Plugins must be idempotent and handle repeated calls without causing unnecessary work. The plugin-level fix (Option 1) is appropriate.
- **If no**: Internal pooling should be adjusted to avoid re-running the plugin chain on pool borrows. Option 2 would be the correct fix.

I've implemented Option 1 as the minimal, backwards-compatible fix, but I'd appreciate guidance on the intended contract between internal pooling and the plugin lifecycle.

## Workaround

Remove `auroraConnectionTracker` from the plugin list when using internal pooling with custom endpoints:

```java
props.setProperty("wrapperPlugins", "failover2,efm2,customEndpoint,initialConnection");
```

**Tradeoff**: Lose automatic stale connection cleanup after failover events. Idle connections to demoted writers remain open and may fail on use rather than being proactively closed.

## Related Files

- `AuroraConnectionTrackerPlugin.java` - Contains the bug
- `PluginServiceImpl.java` - `fillAliases()` method with isEmpty guard
- `HikariPooledConnectionProvider.java` - Internal pooling that triggers plugin chain on every borrow
- `AuroraConnectionTrackerPluginAliasQueryBugTest.java` - Unit test demonstrating the bug
