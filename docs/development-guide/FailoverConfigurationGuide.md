# Failover Configuration Guide

## Tips to Keep in Mind

### Failover with Different Query Lengths
It is important for the developer to understand the types of queries their application will be running. The reason for this is that failover configuration should be tailored to the length of the queries. The aggressiveness of failover should be adjusted when considering the length of the queries. Additionally, it is recommended that user applications use different connection pools with particular settings for different query lengths because there are preffered settings for long running queries that differ from short running queries. 

### Failover Time Profiles
Failover time profiles define the aggressiveness of the failover. For the default time, failover should be executed within 5 minutes. If the connection is not re-established during this time, then the failover times out and fails. Users can adjust the aggressiveness of the failover to fulfill the needs of their specific application. For example, a user could shorten the time limit on failover and use an aggressive profile to promote a fail-fast approach for an application that does not tolerate database outages. However, aggressive failover does come with its side effects. Since our timeout is shorter, it becomes more likely that a problem is cause not by a failure, but rather because of a timeout.

### Writer Clusters After Failover
Connecting to a writer cluster after failover can result in a faulty connection. This is because DNS causes a delay in changing the writer cluster. On the AWS DNS server, this change is updated usually between 15-20 seconds, but the other DNS servers sitting between the application and the AWS DNS server may not be updated in time. Using this stale data may cause problems for users, so it is important to keep this is mind, and to properly handle exceptions.

### 2-Node Clusters
Using failover with a 2-node cluster will not bring you very much benefit. This is because during the failover process with a 2-node cluster, the two nodes simply switch roles; the reader becomes the writer and the writer becomes the reader. If failover is triggered because one of the nodes has a problem, this problem will persist because there aren't any extra nodes to take the responsibility of the one that is broken. At least 3 nodes are required in order to guarantee that no data is lost during the failover process.

### Node Availability
It is important to understand what happens to all the nodes when failover is triggered. At a high level, it seems as though just one node, the one triggering the failover, will be unavailable during the failover process; this is actually not true. When failover is triggered, all nodes become unavailable for a short time. This is because the control plane, which orchestrates the failover process, first shuts down all nodes, then starts the writer node, and finally starts and connects the remaining nodes to the writer. In short, failover requires each node to be reconfigured and thus, all nodes must become unavailable for a short period of time. One additional note to point out is that if your failover time profile is aggressive, then this may cause failover to fail because some nodes may still be unavailable by the time your failover times out.

### Monitor Failure Rate and Investigate
If you are experiencing difficulties with the failover plugin, look in to the logs and try and understand the problem. Try to analyze, do some fine tuning if you find that it is necessary. As developers, we should be diligent. We can't simply use a feature and close our eyes!