# Failover Configuration Guide

## Tips to Keep in Mind

### Failover Time Profiles
A failover time profile refers to a specific combination of failover parameters that determine the time in which failover should be completed and define the aggressiveness of failover. Some failover parameters include `failoverTimeoutMs` and `failoverReaderConnectTimeoutMs`. Failover should be completed within 5 minutes by default. If the connection is not re-established during this time, then the failover process times out and fails. Users can configure the failover parameters to adjust the aggressiveness of the failover and fulfill the needs of their specific application. For example, a user could take a more aggressive approach and shorten the time limit on failover to promote a fail-fast approach for an application that does not tolerate database outages. Examples of normal and aggressive failover time profiles are shown below. 
<br><br>
**:warning:Note**: Aggressive failover does come with its side effects. Since the time limit on failover is shorter, it becomes more likely that a problem is caused not by a failure, but rather because of a timeout.
<br><br>
#### Example of the configuration for a normal failover time profile:
| Parameter                              | Value     |
|----------------------------------------|-----------|
| `failoverTimeoutMs`                    | `180000 ` |
| `failoverWriterReconnectIntervalMs`    | `2000`    |
| `failoverReaderConnectTimeoutMs`       | `30000`   |
| `failoverClusterTopologyRefreshRateMs` | `2000`    |

#### Example of the configuration for an aggressive failover time profile:
| Parameter                              | Value   |
|----------------------------------------|---------|
| `failoverTimeoutMs`                    | `30000` |
| `failoverWriterReconnectIntervalMs`    | `2000`  |
| `failoverReaderConnectTimeoutMs`       | `10000` |
| `failoverClusterTopologyRefreshRateMs` | `2000`  |

### Writer Cluster Endpoints After Failover
Connecting to a writer cluster endpoint after failover can result in a faulty connection because DNS causes a delay in changing the writer cluster. On the AWS DNS server, this change is updated usually between 15-20 seconds, but the other DNS servers sitting between the application and the AWS DNS server may not be updated in time. Using this stale DNS data will most likely cause problems for users, so it is important to keep this is mind.

### 2-Node Clusters
Using failover with a 2-node cluster is not beneficial because during the failover process involving one writer node and one reader node, the two nodes simply switch roles; the reader becomes the writer and the writer becomes the reader. If failover is triggered because one of the nodes has a problem, this problem will persist because there aren't any extra nodes to take the responsibility of the one that is broken. Three or more database nodes are recommended to improve the stability of the cluster.

### Node Availability
It seems as though just one node, the one triggering the failover, will be unavailable during the failover process; this is actually not true. When failover is triggered, all nodes become unavailable for a short time. This is because the control plane, which orchestrates the failover process, first shuts down all nodes, then starts the writer node, and finally starts and connects the remaining nodes to the writer. In short, failover requires each node to be reconfigured and thus, all nodes must become unavailable for a short period of time. One additional note to point out is that if your failover time is aggressive, then this may cause failover to fail because some nodes may still be unavailable by the time your failover times out.

### Monitor Failures and Investigate
If you are experiencing difficulties with the failover plugin, try the following:
- Enable logs [here](/docs/using-the-jdbc-wrapper/UsingTheJdbcWrapper.md#logging) to see the cause of the failure
  - If it is a timeout, see [this](#failover-time-configuration) and fine tune the timeout values
- For additional assistance, visit the getting help page [here](../../README.md#getting-help-and-opening-issues)