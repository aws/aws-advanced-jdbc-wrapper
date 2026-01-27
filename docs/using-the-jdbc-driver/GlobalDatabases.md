# Aurora Global Databases

> **Since version:** 3.0.0

The AWS Advanced JDBC Wrapper provides comprehensive support for [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), including both in-region and cross-region failover capabilities.

## Overview

Aurora Global Database is a feature that allows a single Aurora database to span multiple AWS regions. It provides fast replication across regions with minimal impact on database performance, enabling disaster recovery and serving read traffic from multiple regions.

The AWS Advanced JDBC Wrapper supports:
- In-region failover
- Cross-region planned failover and switchover
- Global Writer Endpoint recognition
- Stale DNS handling

## Configuration

The following instructions are recommended by AWS Service Teams for Aurora Global Database connections. This configuration provides writer connections with support for both in-region and cross-region failover.

### Writer Connections

**Connection String:**
Use the global cluster endpoint:
```
<global-db-name>.global-<XYZ>.global.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter | Value | Notes |
|-----------|-------|-------|
| `clusterId` | `1` | See [clusterId parameter documentation](./using-plugins/UsingTheFailover2Plugin.md#failover-plugin-v2-configuration-parameters) |
| `wrapperDialect` | `global-aurora-mysql` or `global-aurora-pg` | |
| `wrapperPlugins` | `initialConnection,failover2,efm2` | Without connection pooling |
| | `auroraConnectionTracker,initialConnection,failover2,efm2` | With connection pooling |
| `globalClusterInstanceHostPatterns` | `?.XYZ1.us-east-2.rds.amazonaws.com,?.XYZ2.us-west-2.rds.amazonaws.com` | See [documentation](./using-plugins/UsingTheFailover2Plugin.md) |

> **Note:** Add additional plugins according to the [compatibility guide](./CompatibilityCrossPlugins.md).

### Reader Connections

**Connection String:**
Use the cluster reader endpoint:
```
<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter | Value | Notes |
|-----------|-------|-------|
| `clusterId` | `1` | Use the same value as writer connections |
| `wrapperDialect` | `global-aurora-mysql` or `global-aurora-pg` | |
| `wrapperPlugins` | `initialConnection,failover2,efm2` | Without connection pooling |
| | `auroraConnectionTracker,initialConnection,failover2,efm2` | With connection pooling |
| `globalClusterInstanceHostPatterns` | Same as writer configuration | |
| `failoverMode` | `strict-reader` or `reader-or-writer` | Depending on system requirements |

> **Note:** Add additional plugins according to the [compatibility guide](./CompatibilityCrossPlugins.md).

## Example Configuration

### Java Code Example

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class GlobalDatabaseExample {
    public static void main(String[] args) throws Exception {
        // Writer connection
        String writerUrl = "jdbc:aws-wrapper:mysql://my-global-db.global-xyz.global.rds.amazonaws.com:3306/mydb";
        Properties writerProps = new Properties();
        writerProps.setProperty("user", "username");
        writerProps.setProperty("password", "password");
        writerProps.setProperty("clusterId", "1");
        writerProps.setProperty("wrapperDialect", "global-aurora-mysql");
        writerProps.setProperty("wrapperPlugins", "initialConnection,failover2,efm2");
        writerProps.setProperty("globalClusterInstanceHostPatterns", 
            "?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com");
        
        Connection writerConn = DriverManager.getConnection(writerUrl, writerProps);
        
        // Reader connection
        String readerUrl = "jdbc:aws-wrapper:mysql://my-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com:3306/mydb";
        Properties readerProps = new Properties();
        readerProps.setProperty("user", "username");
        readerProps.setProperty("password", "password");
        readerProps.setProperty("clusterId", "1");
        readerProps.setProperty("wrapperDialect", "global-aurora-mysql");
        readerProps.setProperty("wrapperPlugins", "initialConnection,failover2,efm2");
        readerProps.setProperty("globalClusterInstanceHostPatterns", 
            "?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com");
        readerProps.setProperty("failoverMode", "strict-reader");
        
        Connection readerConn = DriverManager.getConnection(readerUrl, readerProps);
    }
}
```

## Important Considerations

### Plugin Selection
- **Connection Pooling**: Include `auroraConnectionTracker` plugin when using connection pooling

### Global Cluster Instance Host Patterns
The `globalClusterInstanceHostPatterns` parameter is **required** for Aurora Global Databases. The patterns are based on
instance endpoints. It should contain:
- Comma-separated list of host patterns for each region
- Different cluster identifiers for each region (e.g., `XYZ1`, `XYZ2`)
- Proper region specification for custom domains: `[us-east-1]?.custom.com`

### Failover Behavior
- **In-region failover**: Automatic failover within the same region
- **Cross-region failover**: Planned failover to a different region
- **DNS handling**: The `initialConnection` plugin helps mitigate stale DNS issues

## Compatibility

For detailed compatibility information, see:
- [Database Types Compatibility](./CompatibilityDatabaseTypes.md)
- [Endpoint Types Compatibility](./CompatibilityEndpoints.md)
- [Cross-Plugin Compatibility](./CompatibilityCrossPlugins.md)

## Related Documentation

- [Failover Plugin v2](./using-plugins/UsingTheFailover2Plugin.md)
- [Aurora Initial Connection Strategy Plugin](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)
- [IAM Authentication Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)
- [Database Dialects](./DatabaseDialects.md)
