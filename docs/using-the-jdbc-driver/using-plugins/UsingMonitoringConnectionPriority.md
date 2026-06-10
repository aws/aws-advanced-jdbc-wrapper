# Monitoring Connection Priority

The AWS Advanced JDBC Wrapper's topology monitor maintains a long-lived background connection to the cluster — the **monitoring connection** — used to query topology, verify the writer, and detect failover. By default, the monitor prefers the writer for this connection because the writer is authoritative for topology and the only host where `isWriterInstance()` is unambiguous.

For most workloads the default is correct and you should not change it. The `monitoringConnectionPriority` and `gdbMonitoringConnectionPriority` properties exist for cases where the default trade-off is wrong: a writer in a region the application would rather not connect to, a reader-only operational posture, or a Global Aurora deployment with a richer regional preference.

## Plugin Availability

The properties are available since version 4.1.0. They apply to:

- All Aurora topology-monitored deployments (`monitoringConnectionPriority`)
- Aurora Global Database deployments (`gdbMonitoringConnectionPriority`)

## How priority works

A priority list is an **ordered preference**, not a hard requirement. The monitor:

1. Opens whatever connection it can during initial discovery.
2. Records the priority index of that connection (the position of the first matching priority in the list, or "none" if no priority matches).
3. While running, **asynchronously** attempts to upgrade to a higher-priority connection if the current one isn't already at index 0.

The async upgrade is important. The monitor never drops a working connection to chase a higher priority. It opens a candidate in the background, verifies it (`isWriterInstance()` for role-checking), and only swaps the candidate in if the new index is strictly lower than the current one. A failed upgrade attempt is silent and retried on the next monitoring cycle.

This means setting `strict-reader,strict-writer` does not produce churn — the monitor will keep a working writer connection even though `strict-reader` is listed first, until a reader becomes available and the upgrade succeeds.

## Aurora — `monitoringConnectionPriority`

| Property | Default | Description |
|---|---|---|
| `monitoringConnectionPriority` | `strict-writer` | Comma-separated list of priorities, in order of preference. |

### Tokens

| Token | Matches |
|---|---|
| `strict-writer` | A writer instance |
| `strict-reader` | A reader instance |
| `writer-or-reader` | Any instance |

### Examples

```properties
# Default — prefer the writer for monitoring.
monitoringConnectionPriority=strict-writer

# Prefer a reader for monitoring; fall back to the writer if no reader is available.
# Useful when the writer's region has the highest network cost or the writer is
# load-sensitive and you want monitoring traffic on a reader.
monitoringConnectionPriority=strict-reader,strict-writer

# No preference — accept whatever the monitor establishes first.
monitoringConnectionPriority=writer-or-reader
```

## Global Aurora — `gdbMonitoringConnectionPriority`

| Property | Default | Description |
|---|---|---|
| `gdbMonitoringConnectionPriority` | `strict-writer-primary` | Comma-separated list of priorities, in order of preference. |

### Tokens

| Token | Matches |
|---|---|
| `strict-writer-primary` | The writer instance, in the current primary region |
| `strict-reader-primary` | A reader instance, in the current primary region |
| `strict-reader-secondary` | A reader instance, in any non-primary region |
| `strict-writer-<region>` | The writer instance, in the named region (e.g., `strict-writer-us-east-1`) |
| `strict-reader-<region>` | A reader instance, in the named region |
| `<region>` | Any instance in the named region |

The "primary region" is the region of whichever instance is currently the writer. After a Global Database switchover, the primary region changes; priorities defined as `*-primary` track this automatically.

Region tokens are matched case-insensitively against the region extracted from the host endpoint by `RdsUtils.getRdsRegion()`. **There is no validation against a canonical region list** — a typo (`us-eat-1`) is accepted as a region literal that will never match anything. Verify your spelling.

### Examples

```properties
# Default — prefer the writer in the current primary region.
gdbMonitoringConnectionPriority=strict-writer-primary

# Prefer a primary-region reader for monitoring traffic; fall back to the
# writer if no reader is available; finally any host in the primary region.
gdbMonitoringConnectionPriority=strict-reader-primary,strict-writer-primary,strict-reader-secondary

# Pin monitoring to a specific region regardless of which region holds the writer.
# Useful when one region has dedicated monitoring infrastructure or is closest
# to the application's monitoring/observability stack.
gdbMonitoringConnectionPriority=strict-writer-us-east-1,strict-reader-us-east-1,us-east-1
```

### Interaction with `gdbAccessibleRegions`

When [`gdbAccessibleRegions`](./UsingGlobalAuroraAccessibleRegions.md) is set, hosts outside the accessible region set are excluded **before** priority matching. A priority that names an inaccessible region will simply never match any candidate. The monitor will fall through the priority list to the first priority that matches an accessible host.

## Async upgrade behavior

The upgrade machinery is non-blocking and self-throttling:

1. Per monitoring cycle, the handler checks whether the current monitoring connection is at priority index 0. If so, no upgrade is attempted.
2. Otherwise, it checks whether a previous upgrade attempt is still in progress. If so, it waits — no new attempt is submitted.
3. If a previous attempt completed, the handler verifies the candidate's role via `isWriterInstance()`. If the verified candidate's index is strictly lower than the current index, it is swapped in and the previous monitoring connection is closed. Otherwise the candidate is closed.
4. If no attempt is in progress and the current connection is below index 0, the handler enumerates all hosts in the current topology that match higher-priority entries, shuffles each priority bucket (so different hosts within the same priority are tried first across cycles), and submits an async task to try them in order until one succeeds.

The async task notifies the main monitoring loop on success so the upgrade is processed promptly rather than waiting for the next polling interval.

## When to tune this

Concrete scenarios where the default is not the right answer:

| Scenario | Suggested setting |
|---|---|
| Writer is load-sensitive; monitoring traffic should land on a reader | `monitoringConnectionPriority=strict-reader,strict-writer` |
| Multi-region GDB, application's observability stack is in a specific region | `gdbMonitoringConnectionPriority=strict-writer-us-east-1,strict-reader-us-east-1,us-east-1` |
| GDB with `gdbAccessibleRegions` excluding the writer's region | `gdbMonitoringConnectionPriority=strict-reader-primary,strict-reader-secondary` (writer match will be impossible; this falls through to readers explicitly) |
| You want the monitor to bind to whatever it gets first and never upgrade | `monitoringConnectionPriority=writer-or-reader` |

If none of these apply, leave the property at its default. **The default is the right answer for most workloads** — the writer's authoritative view of the topology is hard to replace with a reader's view, and `strict-writer` minimizes the number of stale-topology decisions the monitor has to make.

## Caveats and limitations

- **Default priority must be reachable.** If the highest-priority entry in the list cannot be satisfied (no matching host in the topology, or excluded by `gdbAccessibleRegions`), the monitor opens a connection at the next satisfiable priority and continues to attempt upgrades on every cycle. There's no warning if priority 0 is permanently unsatisfiable; check the monitor's snapshot state to verify.
- **The async-upgrade thread is per topology monitor.** A cluster monitored by the wrapper has one upgrade thread; an application connecting to multiple clusters has one per cluster. The thread is created lazily and shut down when the monitor stops.
- **Priority changes require reconnect.** The properties are read at monitor construction time. Changing `monitoringConnectionPriority` mid-run requires a new connection (and a new topology monitor) to take effect.
- **Region tokens are not validated.** A misspelled region (`us-eat-1`) is accepted as a literal and will never match. There is no canonical AWS region list in the wrapper; rely on testing or careful copy-paste.

## Logging and diagnostics

The monitor logs (at `FINE`):

- `ClusterTopologyMonitorImpl.connectionAccepted` — new connection accepted at a given priority index.
- `ClusterTopologyMonitorImpl.connectionRejected` — offered connection was rejected because the current connection has a higher priority.
- `ClusterTopologyMonitorImpl.upgradedMonitoringConnection` — async upgrade succeeded; monitoring connection swapped to a higher-priority host.
- `ClusterTopologyMonitorImpl.upgradeAttemptFailed` — a candidate host could not be connected to; the next candidate is tried.
- `ClusterTopologyMonitorImpl.upgradeTaskSubmitted` — async upgrade task submitted; includes the candidate count.

The state snapshot (visible in extended exception messages) includes the current priority index, the priority list, and whether an upgrade is in progress — useful for "why is monitoring on this host?" investigations.
