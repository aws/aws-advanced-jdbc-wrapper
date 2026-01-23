# Understanding the clusterId Parameter

## Overview

The `clusterId` parameter is a critical configuration setting when using the AWS Advanced JDBC Wrapper to connect to multiple database clusters within a single application. This parameter serves as a unique identifier that enables the driver to maintain separate caches and state for each distinct database cluster your application connects to.

## What is a Cluster?

In the context of the AWS Advanced JDBC Wrapper, a **cluster** is a logical grouping of database instances that should share the same topology cache, connection pools, and monitoring services. Understanding what constitutes a cluster is crucial for correctly setting the `clusterId` parameter.

A cluster represents one writer instance (primary), zero or more reader instances (replicas), shared topology that the driver needs to track, and a single failover domain where the driver can switch between instances.

### Examples of Clusters

- Aurora DB Cluster (one writer + multiple readers)
- RDS Multi-AZ DB Cluster (one writer + two readers)
- Aurora Global Database (when supplying a global db endpoint, the driver considers them as a single cluster)


> **Rule of thumb:** If the driver should track separate topology information and perform independent failover operations, use different `clusterId` values. If instances share the same topology and failover domain, use the same `clusterId`.



## Why clusterId is Important

The AWS Advanced JDBC Wrapper uses the `clusterId` as a **key for internal caching mechanisms** to optimize performance and maintain cluster-specific state. Without proper `clusterId` configuration, your application may experience:

- Cache collisions between different clusters
- Incorrect topology information
- Connection pool conflicts
- Degraded performance due to cache invalidation

## Why Not Use AWS DB Cluster Identifiers?

Host information can take many forms:

- **IP Address Connections:** `jdbc:aws-wrapper:mysql://10.0.1.50:3306/mydb` ← No cluster info!
- **Custom Domain Names:** `jdbc:aws-wrapper:mysql://db.mycompany.com:3306/mydb` ← Custom domain
- **Custom Endpoints:** `jdbc:aws-wrapper:mysql://my-custom-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com:3306/mydb` ← Custom endpoint
- **Proxy Connections:** `jdbc:aws-wrapper:mysql://my-proxy.proxy-abc.us-east-1.rds.amazonaws.com:3306/mydb` ← Proxy, not actual cluster

In fact, all of these could reference the exact same cluster. Therefore, because the driver cannot reliably parse cluster information from all connection types, **it is up to the user to explicitly provide the `clusterId`**.

## How clusterId is Used Internally

The driver uses `clusterId` as a cache key for topology information and monitoring services. This enables multiple connections to the same cluster to share cached data and avoid redundant db meta-data.

### High-Level View

The following diagram shows how connections with the same `clusterId` share cached resources:

```mermaid
graph TB
    subgraph Application
        Conn1[Connection 1<br/>clusterId: foo]
        Conn2[Connection 2<br/>clusterId: foo]
    end
    
    subgraph "AWS JDBC Wrapper Driver"
        subgraph "Shared Resources"
            TopoCache[Topology Cache]
            MonitorCache[Monitor Threads]
        end
    end
    
    Conn1 -..-> TopoCache
    Conn2 -..-> TopoCache
    Conn1 -..-> MonitorCache
    Conn2 -..-> MonitorCache
    
    style TopoCache fill:#e1f5ff,stroke:#666,stroke-width:2px
    style MonitorCache fill:#e1f5ff,stroke:#666,stroke-width:2px
```

Topology Cache could look something like:
```json
{
  "foo": ["hostA","hostB","hostC"],
  "bar": ["barHost1"]
}
```
> Both Connection 1 and Connection 2 retrieve the **same cached value** using the key `"foo"`, avoiding redundant topology queries to the database.

**Key Points:**
- Multiple connections with the **same `clusterId`** share the same cached topology and monitors
- Shared caches reduce database queries and improve performance
- Background monitors are keyed by `clusterId` to avoid duplicate threads

### Low-Level View

Here's how the driver stores and retrieves data using `clusterId` as the cache key:

```mermaid
graph TB
    subgraph "Application Connections"
        App1[Connection 1<br/>clusterId: foo]
        App2[Connection 2<br/>clusterId: bar]
        App3[Connection 3<br/>clusterId: foo]
    end
    
    subgraph "AWS JDBC Wrapper Driver"
        subgraph "Plugin Services"
            PS1[PluginService #1<br/>clusterId: foo]
            PS2[PluginService #2<br/>clusterId: bar]
            PS3[PluginService #3<br/>clusterId: foo]
        end
        
        subgraph "Shared Cache Storage"
            subgraph "Topology Cache"
                Key1["Key: foo"]
                Value1["Value:<br/>Writer: writer-1.rds.com<br/>Reader1: reader-1.rds.com<br/>Reader2: reader-2.rds.com"]
                
                Key2["Key: bar"]
                Value2["Value:<br/>Writer: writer-2.rds.com<br/>Reader1: reader-3.rds.com"]
            end
            
            subgraph "Monitor Registry"
                Key5["Key: foo"]
                Value5["Value:<br/>ClusterTopologyMonitor<br/>Thread #1"]
                
                Key6["Key: bar"]
                Value6["Value:<br/>ClusterTopologyMonitor<br/>Thread #2"]
            end
        end
    end
    
    App1 --> PS1
    App2 --> PS2
    App3 --> PS3
    
    PS1 -->|reads| Key1
    PS3 -->|reads| Key1
    PS1 -->|creates/uses| Key5
    PS3 -->|creates/uses| Key5
    Key1 --> Value1
    Key5 --> Value5
    
    PS2 -->|reads| Key2
    PS2 -->|creates/uses| Key6
    Key2 --> Value2
    Key6 --> Value6
    
    Value5 -.polls & updates.-> Value1
    Value6 -.polls & updates.-> Value2
    
    style PS1 fill:#e1f5ff,stroke:#666,stroke-width:2px
    style PS2 fill:#ffe1f5,stroke:#666,stroke-width:2px
    style PS3 fill:#e1f5ff,stroke:#666,stroke-width:2px
    style Key1 fill:#fff4cc,stroke:#666,stroke-width:2px
    style Key2 fill:#ffe1f0,stroke:#666,stroke-width:2px
    style Key5 fill:#fff4cc,stroke:#666,stroke-width:2px
    style Key6 fill:#ffe1f0,stroke:#666,stroke-width:2px
    style Value1 fill:#fffef0,stroke:#666,stroke-width:1px
    style Value2 fill:#fff5fb,stroke:#666,stroke-width:1px
    style Value5 fill:#fffef0,stroke:#666,stroke-width:1px
    style Value6 fill:#fff5fb,stroke:#666,stroke-width:1px
```

**How it works:**
- Each connection creates its own PluginService instance.
- Connection 1 and 3 both use `clusterId: "foo"` → Their separate PluginService instances access the same shared cache entries
- Connection 2 uses `clusterId: "bar"` → Its PluginService accesses separate cache entries
- PluginService reads topology from shared cache using `clusterId` as the key
- Each cache is a Map structure: `Map<String, CacheValue>` where the key is the `clusterId`
- Monitors poll the database and update the topology cache

## When to Specify clusterId

### **Required: Multiple Clusters in One Application**

You **must** specify a unique `clusterId` when your application connects to multiple database clusters:

```java
// Application connecting to two different clusters
Properties prodProps = new Properties();
prodProps.setProperty("clusterId", "production-cluster");
prodProps.setProperty("user", "admin");
prodProps.setProperty("password", "***");
Connection prodConn = DriverManager.getConnection(
    "jdbc:aws-wrapper:mysql://prod-cluster.us-east-1.rds.amazonaws.com:3306/mydb",
    prodProps
);

Properties stagingProps = new Properties();
stagingProps.setProperty("clusterId", "staging-cluster");  // Different clusterId!
stagingProps.setProperty("user", "admin");
stagingProps.setProperty("password", "***");
Connection stagingConn = DriverManager.getConnection(
    "jdbc:aws-wrapper:mysql://staging-cluster.us-east-1.rds.amazonaws.com:3306/mydb",
    stagingProps
);
```

### **Optional: Single Cluster Applications**

If your application only connects to one cluster, you can omit `clusterId` (defaults to `"1"`):

```java
// Single cluster - clusterId defaults to "1"
Connection conn = DriverManager.getConnection(
    "jdbc:aws-wrapper:mysql://my-cluster.us-east-1.rds.amazonaws.com:3306/mydb",
    props
);
```

### **Highly Recommended: Consistent clusterId for Same Cluster**

When making multiple connections to the **same cluster**, you should use the **same `clusterId`** to benefit from shared caching:

```java
// Multiple connections to the same cluster - use same clusterId
String url = "cluster-a..."
Properties props1 = new Properties();
props1.setProperty("clusterId", "my-cluster");
Connection conn1 = DriverManager.getConnection(url, props1);

Properties props2 = new Properties();
props2.setProperty("clusterId", "my-cluster");  // Same clusterId = shared cache
Connection conn2 = DriverManager.getConnection(url, props2);
```

**Benefits of consistent `clusterId`:**
- Shared topology cache (no redundant queries)
- Shared connection pools (better resource utilization)
- Single monitoring thread per cluster (reduced overhead)


## Critical Warnings

### 🚨 **NEVER Share clusterId Between Different Clusters**

Using the same `clusterId` for different database clusters will cause serious issues:

```java
// ❌ WRONG - Same clusterId for different clusters
Properties props1 = new Properties();
props1.setProperty("clusterId", "shared-id");  // ← BAD!
Connection conn1 = DriverManager.getConnection(
    "jdbc:aws-wrapper:mysql://cluster-a.us-east-1.rds.amazonaws.com:3306/db",
    props1
);

Properties props2 = new Properties();
props2.setProperty("clusterId", "shared-id");  // ← BAD! Same ID for different cluster
Connection conn2 = DriverManager.getConnection(
    "jdbc:aws-wrapper:mysql://cluster-b.us-west-2.rds.amazonaws.com:3306/db",
    props2
);
```

**What happens with cache collision:**

```mermaid
graph TB
    subgraph Application
        Conn1[Connection to Cluster A<br/>clusterId: foobar]
        Conn2[Connection to Cluster B<br/>clusterId: foobar]
    end
    
    subgraph "Topology Cache"
        Cache["foobar: <br> Cluster A or B topology?"]
    end
    
    Conn1 --> Cache
    Conn2 --> Cache
    
    style Cache fill:#ffcccc,stroke:#666,stroke-width:3px
    style Conn1 fill:#ffe6e6,stroke:#666,stroke-width:2px
    style Conn2 fill:#ffe6e6,stroke:#666,stroke-width:2px
```

**Problems this causes:**
- Topology cache collision (cluster-b's topology could overwrite cluster-a's)
- Incorrect failover behavior (driver may try to failover to wrong cluster)
- Monitor conflicts (Only one monitor instance for both clusters will lead to undefined results)

**Correct approach:**
```java
// ✅ CORRECT - Unique clusterId for each cluster
props1.setProperty("clusterId", "cluster-a-prod");
props2.setProperty("clusterId", "cluster-b-staging");
```

**Correct architecture:**

```mermaid
graph TB
    subgraph Application
        Conn1[Connection to Cluster A<br/>clusterId: foo]
        Conn2[Connection to Cluster B<br/>clusterId: bar]
    end
    
    subgraph "Topology Cache"
        KeyA["foo: <br/>[A-reader, A-writer]"]
        KeyB["bar: <br/>[B-reader, B-writer]"]
    end
    
    Conn1 --> KeyA
    Conn2 --> KeyB
    
    style KeyA fill:#e6ffe6,stroke:#666,stroke-width:2px
    style KeyB fill:#e6ffe6,stroke:#666,stroke-width:2px
    style Conn1 fill:#f0fff0,stroke:#666,stroke-width:2px
    style Conn2 fill:#f0fff0,stroke:#666,stroke-width:2px
```

### ⚠️ **Always Use Same clusterId for Same Cluster**

Using different `clusterId` values for the same cluster reduces efficiency:

```java
// ⚠️ SUBOPTIMAL - Different clusterIds for same cluster
Properties props1 = new Properties();
props1.setProperty("clusterId", "my-cluster-1");
Connection conn1 = DriverManager.getConnection(sameClusterUrl, props1);

Properties props2 = new Properties();
props2.setProperty("clusterId", "my-cluster-2");  // Different ID for same cluster
Connection conn2 = DriverManager.getConnection(sameClusterUrl, props2);
```

**Problems this causes:**
- Duplication of caches
- Multiple monitoring threads for the same cluster

**Best practice:**
```java
// ✅ BEST - Same clusterId for same cluster
final String CLUSTER_ID = "my-production-cluster";
props1.setProperty("clusterId", CLUSTER_ID);
props2.setProperty("clusterId", CLUSTER_ID);  // Shared cache and resources
```

## Summary

The `clusterId` parameter is essential for applications connecting to multiple database clusters. It serves as a cache key for topology information, connection pools, and monitoring services. Always use unique `clusterId` values for different clusters, and consistent values for the same cluster to maximize performance and avoid conflicts.
