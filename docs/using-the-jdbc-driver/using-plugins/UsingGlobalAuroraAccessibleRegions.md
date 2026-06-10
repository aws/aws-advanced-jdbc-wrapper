# Restricting Aurora Global Database Access by Region

When using the AWS Advanced JDBC Wrapper against an [Aurora Global Database](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database.html), the wrapper discovers every node in every region the Global Database spans. By default it will attempt connections to any of them — for failover, topology monitoring, or read/write splitting.

Some applications cannot — or should not — reach every region. Common reasons:

- **Network reachability.** Security groups, VPC peering, or transit-gateway routes only permit traffic to a subset of regions.
- **Compliance / data residency.** Application data is permitted to flow to specific regions only.
- **Latency budget.** Cross-region connection attempts (and their TCP timeouts) are expensive enough to break SLOs even when they eventually fail.

The `gdbAccessibleRegions` connection property restricts the wrapper to a user-specified set of regions. Hosts in any other region are filtered out across the four subsystems that decide where to connect.

## Plugin Availability

The property is available since version <!-- TODO: fill in --> and applies to:

- [GDB Failover Plugin](./UsingTheGdbFailoverPlugin.md)
- [GDB Read/Write Splitting Plugin](./UsingTheGdbReadWriteSplittingPlugin.md)
- [Aurora Initial Connection Strategy Plugin](./UsingTheAuroraInitialConnectionStrategyPlugin.md)
- The Global Aurora topology monitor (used by the plugins above)

## Configuration

Set `gdbAccessibleRegions` to a comma-separated list of AWS region names:

```java
Properties props = new Properties();
props.setProperty("wrapperPlugins", "gdbFailover,gdbReadWriteSplitting");
props.setProperty("failoverHomeRegion", "us-east-1");
props.setProperty("gdbRwHomeRegion", "us-east-1");
props.setProperty("gdbAccessibleRegions", "us-east-1,us-west-2");
```

| Property | Default | Description |
|---|---|---|
| `gdbAccessibleRegions` | _none_ | Comma-separated list of AWS regions the application can reach. When unset, all regions are considered accessible (legacy behavior). Region names are case-insensitive and trimmed. |

### The `homeRegion` ⊆ `accessibleRegions` constraint

If `gdbAccessibleRegions` is set and the home region is not in it, both the GDB Failover Plugin and the GDB Read/Write Splitting Plugin throw `SQLException` at connect time with one of:

- `Home region '<region>' is not included in the list of accessible regions [...]. The home region must be accessible.`

This is intentional. The home region is where the application normally operates; an unreachable home region is a misconfiguration, not a runtime condition the wrapper should silently work around.

## What gets filtered

| Subsystem | Behavior when `gdbAccessibleRegions` excludes a host's region |
|---|---|
| Topology monitor | No node-monitoring worker is spawned for the host. The host is excluded from stable-topology checks. |
| Topology monitor — initial host | If the initial connection host is in an excluded region, the monitor throws `RuntimeException` rather than start in an unreachable region. |
| Failover (`STRICT_WRITER` mode) | If the writer is in an excluded region, throws `FailoverFailedSQLException` — the wrapper does **not** attempt a cross-region writer connection. |
| Failover (other modes) | Excluded hosts are filtered out of the candidate set before host selection. |
| RW splitting (writer) | If the writer is in an excluded region, throws `ReadWriteSplittingSQLException`. |
| RW splitting (readers) | Excluded reader regions are filtered before the home-region restriction is applied. |
| Initial connection strategy | `Dialect.filterAvailableHosts` excludes inaccessible-region hosts before `getHostSpecByStrategy` selects one. |

The filter is applied **before** any other selection logic — strategy, role preference, home-region restriction, etc. all operate on the post-filter host list.

## What happens when the writer is in an inaccessible region

This is the case that surprises users most often. Aurora Global Database can fail over the writer to any region in the global cluster — including one your application is not authorized to reach.

The wrapper's behavior is **fail loudly, not silently retry**:

- **Failover with `STRICT_WRITER`**: throws `FailoverFailedSQLException` with message `"Writer is in region '<region>' which is not in the list of accessible regions [...]"`.
- **Read/Write splitting** when switching to writer mode: throws `ReadWriteSplittingSQLException` with the same message shape.
- **Topology monitor**: continues monitoring the readers it can reach, but cannot verify a writer connection. The monitor exits panic mode via reader-observed writer changes (see "Panic mode and writer detection" below) rather than via a verified writer connection.

The application is expected to handle these exceptions explicitly — for instance, by surfacing them to an orchestration layer that can switch the application to a different cluster, page an operator, or retry once the writer fails back.

## Worked example — three regions, two accessible

Consider a Global Database with member clusters in `us-east-1` (current writer), `us-west-2`, and `eu-central-1`. The application is permitted to reach `us-east-1` and `us-west-2` only:

```
gdbAccessibleRegions = us-east-1,us-west-2
failoverHomeRegion   = us-east-1
gdbRwHomeRegion      = us-east-1
```

Steady state:
- Topology monitor maintains node-monitoring workers for hosts in `us-east-1` and `us-west-2`. No workers for `eu-central-1`.
- RW splitting with `restrictReaderToHomeRegion = false` may select a reader in either `us-east-1` or `us-west-2`. Never `eu-central-1`.
- Failover with `STRICT_WRITER` would reconnect to a new writer if it appears in `us-east-1` or `us-west-2`. If the writer fails over to `eu-central-1`, failover throws `FailoverFailedSQLException`.

If `eu-central-1` becomes the new primary region:
- The topology monitor sees readers in `us-east-1` and `us-west-2` change role to reader (they already were readers, but with a different "primary region" understanding) and observes the writer's host moving to a `eu-central-1` endpoint via reader-monitor topology updates.
- Failover surfaces `FailoverFailedSQLException` to the application — the wrapper will not attempt the cross-region connection.

## Panic mode and writer detection

The topology monitor enters "panic mode" when it loses its monitoring connection. Normally it exits panic mode when one of its node-monitoring workers connects to the new writer and verifies it via `isWriterInstance()`.

When `gdbAccessibleRegions` excludes the writer's region, no node worker can reach the writer to verify it. The wrapper handles this by:

1. Reader-observed writer change detection. When a reader's topology query reports a writer in an inaccessible region, the monitor accepts that reader's view as authoritative and exits panic mode. This is logged as `NodeMonitoringThread.writerChangeExitTriggered`.
2. Stable-reader topology fallback. If readers' topologies remain consistent for the stable-topology window, the monitor harvests the reader connections through the [monitoring connection priority](./UsingMonitoringConnectionPriority.md) handler and uses one of them as the monitoring connection — even though no writer was verified.

Both paths trade verified-writer assurance for the ability to make progress with a partial view of the cluster. This is appropriate for the accessible-regions use case — the application has explicitly opted out of reaching the inaccessible writer.

## Interaction with other plugins

- **`failoverMode`**: All modes (`strict-writer`, `strict-reader`, `reader-or-writer`, etc.) honor accessible-regions filtering before host selection. `STRICT_WRITER` is the only mode that can produce an outright failure (writer in inaccessible region); the others degrade to whatever accessible candidates remain.
- **`restrictWriterToHomeRegion` / `restrictReaderToHomeRegion`**: Composes correctly with `gdbAccessibleRegions` — accessible-regions filtering runs first, then home-region restriction. A reader in a region that is accessible but not the home region will be excluded by `restrictReaderToHomeRegion` even if `gdbAccessibleRegions` permits it.
- **`enableGwf` (Global Write Forwarding)**: `gdbAccessibleRegions` does not change GWF behavior. GWF is performed by Aurora at the database level; the wrapper does not see the cross-region traffic.

## Logging and diagnostics

Useful log strings (all at `FINE` or `FINEST`):

- `GlobalAuroraTopologyMonitor.initialHostNotInAccessibleRegion` — initial host outside accessible set; monitor refuses to start.
- `NodeMonitoringThread.writerChangeExitTriggered` — panic mode exited via reader-observed writer change rather than verified writer connection.
- `GlobalDbFailoverConnectionPlugin.writerNotInAccessibleRegion` — failover refused because the writer is in an excluded region.
- `GdbReadWriteSplittingPlugin.writerNotInAccessibleRegion` — RW splitting refused to switch to a writer in an excluded region.

When debugging an unexpected failover failure, check the topology snapshot in the exception's state-snapshot section — the `accessibleRegions` field shows what the monitor was filtering against.

## When **not** to use this property

- **Single-region clusters.** The property is a no-op for non-Global Aurora dialects. Set it only when running against an Aurora Global Database.
- **You want the wrapper to retry across regions.** This property is opt-out, not opt-in — it tells the wrapper "do not consider these regions." If you want failover to span all regions, leave it unset.
- **You have a transient connectivity issue to a region.** Use a network-level health check or DNS routing instead. `gdbAccessibleRegions` is read once at connect time and is meant to express a stable application-level constraint, not a runtime condition.
