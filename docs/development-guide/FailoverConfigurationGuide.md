# Failover Configuration Guide

## When Do We Need Failover?
(TODO)
## Tips to Keep in Mind

### Separate Connection for Long Reads
(TODO):
Ideally, the user application will split up the long queries. The reason for this is that long queries often require special configuration. With separated long query statements, it will become easier to configure each connection...

### Failover Time Profiles
Failover time profiles define how aggressive your failover is. For the default time, we need to execute failover within 5 minutes. If the connection is not re-established during this time, then the failover has failed. There are a few different failover time profiles, certain time profiles will fulfill the needs of different applications. For example, for an application that does not tolerate database outages, we could shorten the time limit on failover and use an aggressive profile to promote a fail-fast approach. However, aggressive failover does come with its side effects. With an aggressive failover, the chances of a false negative increase because it becomes more likely that our failover may have just timed out instead of failed.

### Writer Clusters After Failover
Connecting to a writer cluster after failover can result in a wrong connection. In a broad sense, the reason for this is that DNS causes a delay in changing the writer cluster. On the AWS end, this change is updated usually between 15-20 seconds, but the other DNS server sitting between the application and the AWS DNS server may not be updated in time. Using this stale data may cause problems for users, so it is important to keep this is mind, and to properly handle exceptions.

### 2-Node Clusters
Using failover with a 2-node cluster is not very useful. The reason for this is that in failover with a 2-node cluster, the two nodes simply switch roles; the reader becomes the writer and the writer becomes the reader. If failover is triggered because one of the nodes has a problem, this problem will persist because there aren't any extra nodes to take the responsibility of the one that is broken.

### Node Availability
It is important to understand what happens to all the nodes when failover is triggered. At a high level, it seems as though just one node, the one triggering the failover, will be unavailable during the failover process; this is actually not true. When failover is triggered, all nodes become unavailable for a short time. This is because the control plane, which orchestrates the failover process, first shuts down all nodes, then starts the writer node, and finally starts and connects the remaining nodes to the writer. In short, failover requires each node to be reconfigured and thus, all nodes must become unavailable for a short period of time. One additional note to point out is that if your failover time profile is aggressive, then this may cause failover to fail because some nodes may still be unavailable by the time your failover times out.

### Monitor Failure Rate and Investigate
If you are experiencing difficulties with the failover plugin, look in to the logs and try and understand the problem. Try to analyze, do some fine tuning if you find that it is necessary. As developers, we should be diligent. We can't simply use a feature and close our eyes!