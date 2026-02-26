# Plugin Compatibility and Aurora/RDS Version Requirements

This document provides a comprehensive overview of plugin compatibility with different Aurora and RDS database versions, deployment types, and configurations.

## Table of Contents
- [Driver Version Requirements](#driver-version-requirements)
- [Database Version Requirements](#database-version-requirements)
- [Plugin Compatibility Matrix](#plugin-compatibility-matrix)
- [Deployment Type Support](#deployment-type-support)
- [Known Limitations](#known-limitations)

## Driver Version Requirements

### Minimum AWS Advanced JDBC Wrapper Versions for Features

| Feature/Plugin | Minimum Driver Version | Notes |
|----------------|----------------------|-------|
| **Failover Plugin v2 (failover2)** | 2.4.0 | Introduced in v2.4.0; became default plugin in v2.6.0 |
| **Limitless Connection Plugin** | 2.4.0 | Introduced in v2.4.0 for Aurora PostgreSQL Limitless |
| **Blue/Green Deployment Plugin** | 2.6.0 | Full support added in v2.6.0 (requires database version support) |
| **Host Monitoring Plugin v2 (efm2)** | 2.3.2 (experimental), 2.3.3 (default) | Experimental in v2.3.2; became default in v2.3.3 |
| **Custom Endpoint Plugin** | 2.5.0 | Introduced in v2.5.0 |
| **RDS Multi-AZ DB Cluster Support** | 2.3.0 | Enhanced support added in v2.3.0 |
| **Federated Authentication Plugin** | 2.3.2 | Introduced in v2.3.2 |
| **Okta Authentication Plugin** | 2.3.6 | Introduced in v2.3.6 |
| **Aurora Initial Connection Strategy Plugin** | 2.3.2 | Plugin code added in v2.3.2; documented in v2.3.6 |
| **Fastest Response Strategy Plugin** | 2.3.2 | Introduced in v2.3.2 |
| **Configuration Profiles & Presets** | 2.3.1 | Introduced in v2.3.1 |
| **Virtual Threading Support** | 2.4.0 | Added in v2.4.0 |

### Default Plugin Changes by Version

| Driver Version | Default Plugins | Notes |
|----------------|----------------|-------|
| **< 2.3.3** | `failover`, `efm` | Original failover and EFM plugins |
| **2.3.3 - 2.5.x** | `failover`, `efm2` | EFM v2 became default |
| **≥ 2.6.0** | `failover2`, `efm2` | Failover v2 became default |

### Breaking Changes and Migration Notes

**Version 2.6.1 (Reverted in 2.6.2):**
- Temporarily made `clusterId` a required parameter for applications using multiple database clusters
- This change was reverted in v2.6.2 and will be reintroduced in a future major version

**Version 2.6.0:**
- `failover2` plugin set as default (replacing `failover`)
- Applications using Aurora Global Database secondary clusters must explicitly specify plugins to avoid default `failover2`

**Version 2.3.3:**
- `efm2` plugin set as default (replacing `efm`)
- Original `efm` plugin still available by explicitly specifying it

## Database Version Requirements

### Aurora PostgreSQL

| Feature/Plugin | Minimum Version Required | Notes |
|----------------|-------------------------|-------|
| **Blue/Green Deployments** | Engine Release 17.5, 16.9, 15.13, 14.18, 13.21 and above | Requires specific metadata tables. Without these versions, plugin falls back to previous behavior with limitations. |
| **RDS Multi-AZ DB Clusters** | 13.12, 14.9, 15.4 or higher (starting from revision R3) | Requires manual installation of `rds_tools` extension: `CREATE EXTENSION rds_tools;` |
| **General Compatibility** | All supported Aurora PostgreSQL versions | Core driver functionality works with all versions |

### Aurora MySQL

| Feature/Plugin | Minimum Version Required | Notes |
|----------------|-------------------------|-------|
| **Blue/Green Deployments** | Engine Release 3.07 and above | Requires specific metadata tables. Without these versions, plugin falls back to previous behavior with limitations. |
| **General Compatibility** | All supported Aurora MySQL versions | Core driver functionality works with all versions |

### RDS PostgreSQL

| Feature/Plugin | Minimum Version Required | Notes |
|----------------|-------------------------|-------|
| **Blue/Green Deployments** | rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21) and above | Requires specific metadata tables. Without these versions, plugin falls back to previous behavior with limitations. |
| **RDS Multi-AZ DB Clusters** | 13.12, 14.9, 15.4 or higher (starting from revision R3) | Requires manual installation of `rds_tools` extension |
| **General Compatibility** | All supported RDS PostgreSQL versions | Core driver functionality works with all versions |

### RDS MySQL

| Feature/Plugin | Minimum Version Required | Notes |
|----------------|-------------------------|-------|
| **Blue/Green Deployments** | All versions | No specific version requirement for RDS MySQL |
| **General Compatibility** | All supported RDS MySQL versions | Core driver functionality works with all versions |

### Aurora Limitless Database (PostgreSQL Only)

| Feature/Plugin | Minimum Version Required | Notes |
|----------------|-------------------------|-------|
| **Limitless Connection Plugin** | All Aurora Limitless PostgreSQL versions | Only available for Aurora PostgreSQL Limitless deployments |
| **Authentication Plugins** | All Aurora Limitless PostgreSQL versions | IAM, Secrets Manager, and other auth plugins fully supported |
| **General Compatibility** | All Aurora Limitless PostgreSQL versions | Core driver functionality works with all versions |

### Community Databases (Tested Versions)

| Database | Version | Notes |
|----------|---------|-------|
| **MySQL** | 8.0.36 | Compatible with MySQL 5.7 and 8.0 as per Community MySQL Connector/J 8.0 Driver |
| **PostgreSQL** | 16.2 | General compatibility with PostgreSQL versions |

## Plugin Compatibility Matrix

### Core Failover Plugins

| Plugin | Aurora PostgreSQL | Aurora MySQL | RDS Multi-AZ PostgreSQL | RDS Multi-AZ MySQL | RDS PostgreSQL | RDS MySQL | Aurora Global DB | Aurora Limitless |
|--------|-------------------|--------------|-------------------------|--------------------|--------------------|-----------|------------------|------------------|
| **Failover Plugin (v1)** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ⚠️ Limited* | ❌ Not Supported |
| **Failover Plugin v2 (failover2)** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ❌ Not Supported | ❌ Not Supported |

*For Aurora Global Databases: `failover` plugin works on secondary clusters, but `failover2` does not. Primary cluster connections are fully supported.

### Monitoring Plugins

| Plugin | Aurora PostgreSQL | Aurora MySQL | RDS Multi-AZ PostgreSQL | RDS Multi-AZ MySQL | RDS PostgreSQL | RDS MySQL | Aurora Global DB | Aurora Limitless |
|--------|-------------------|--------------|-------------------------|--------------------|--------------------|-----------|------------------|------------------|
| **Host Monitoring Plugin (efm)** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ⚠️ Compatible but not recommended |
| **Host Monitoring Plugin v2 (efm2)** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ⚠️ Compatible but not recommended |
| **Aurora Connection Tracker** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ❓ Untested | ❓ Untested | ❓ Untested | ❓ Untested |

### Authentication Plugins

| Plugin | Aurora PostgreSQL | Aurora MySQL | RDS Multi-AZ PostgreSQL | RDS Multi-AZ MySQL | RDS PostgreSQL | RDS MySQL | Aurora Global DB | Aurora Limitless |
|--------|-------------------|--------------|-------------------------|--------------------|--------------------|-----------|------------------|------------------|
| **IAM Authentication** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **AWS Secrets Manager** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Federated Auth** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Okta Auth** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |

### Deployment-Specific Plugins

| Plugin | Aurora PostgreSQL | Aurora MySQL | RDS Multi-AZ PostgreSQL | RDS Multi-AZ MySQL | RDS PostgreSQL | RDS MySQL | Aurora Global DB | Aurora Limitless |
|--------|-------------------|--------------|-------------------------|--------------------|--------------------|-----------|------------------|------------------|
| **Blue/Green Deployment** | ⚠️ Version-dependent** | ⚠️ Version-dependent** | ⚠️ Version-dependent** | ⚠️ Version-dependent** | ⚠️ Version-dependent** | ⚠️ Version-dependent** | ❌ Not Supported | ❌ Not Supported |
| **Limitless Connection** | ❌ Not Applicable | ❌ Not Applicable | ❌ Not Applicable | ❌ Not Applicable | ❌ Not Applicable | ❌ Not Applicable | ❌ Not Applicable | ✅ Full |
| **Aurora Initial Connection Strategy** | ✅ Full | ✅ Full | ❓ Untested | ❓ Untested | ❌ Not Applicable | ❌ Not Applicable | ❓ Untested | ❌ Not Applicable |

**See [Database Version Requirements](#database-version-requirements) for specific version requirements. Without required versions, plugin falls back to previous behavior with limitations.

### Utility Plugins

| Plugin | Aurora PostgreSQL | Aurora MySQL | RDS Multi-AZ PostgreSQL | RDS Multi-AZ MySQL | RDS PostgreSQL | RDS MySQL | Aurora Global DB | Aurora Limitless |
|--------|-------------------|--------------|-------------------------|--------------------|--------------------|-----------|------------------|------------------|
| **Read/Write Splitting** | ✅ Full | ✅ Full | ❓ Untested | ❓ Untested | ❓ Untested | ❓ Untested | ❓ Untested | ⚠️ Compatible but not recommended |
| **Driver Metadata** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Custom Endpoint** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **KMS Encryption** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |
| **Developer Plugin** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |

**Legend:**
- ✅ Full: Fully supported and tested
- ⚠️ Limited: Supported with limitations or conditions
- ❌ Not Supported: Not compatible or not recommended
- ❓ Untested: Compatibility not verified

## Deployment Type Support

### Amazon Aurora Clusters

#### Aurora PostgreSQL
- **Supported Plugins:** All core plugins including failover, failover2, efm, efm2, IAM, Secrets Manager, Federated Authentication, Okta Authentication
- **Recommended Configuration:** Use `failover2` plugin (enabled by default) for optimal performance
- **Connection Endpoint:** Use cluster endpoint or read-only cluster endpoint
- **Special Considerations:** 
  - Blue/Green requires specific engine versions (see version table)
  - Global Database support is limited (see limitations)

#### Aurora MySQL
- **Supported Plugins:** All core plugins including failover, failover2, efm, efm2, IAM, Secrets Manager, Federated Authentication, Okta Authentication
- **Recommended Configuration:** Use `failover2` plugin (enabled by default) for optimal performance
- **Connection Endpoint:** Use cluster endpoint or read-only cluster endpoint
- **Special Considerations:**
  - Blue/Green requires Engine Release 3.07+ for full support

### RDS Multi-AZ DB Clusters

#### PostgreSQL Multi-AZ
- **Supported Plugins:** failover, failover2, efm, efm2, IAM, Secrets Manager, Federated Authentication, Okta Authentication, Aurora Connection Tracker, Host Monitoring
- **Minimum Version:** PostgreSQL 13.12, 14.9, 15.4 or higher (revision R3+)
- **Required Setup:** Must install `rds_tools` extension: `CREATE EXTENSION rds_tools;`
- **Connection Endpoint:** Use cluster writer endpoint
- **Switchover Time:** ~1 second or less with optimized configuration
- **Recommended Configuration:**
  - Set `failoverClusterTopologyRefreshRateMs` to 100ms for minimal downtime
  - Default is 2000ms

#### MySQL Multi-AZ
- **Supported Plugins:** failover, failover2, efm, efm2, IAM, Secrets Manager, Federated Authentication, Okta Authentication, Aurora Connection Tracker, Host Monitoring
- **Connection Endpoint:** Use cluster writer endpoint
- **Switchover Time:** ~1 second or less with optimized configuration
- **Recommended Configuration:**
  - Set `failoverClusterTopologyRefreshRateMs` to 100ms for minimal downtime

### Plain RDS Databases

#### RDS PostgreSQL (Single Instance)
- **Supported Plugins:** IAM, Secrets Manager, Federated Authentication, Okta Authentication
- **Not Applicable:** Failover plugins (no cluster topology), Host Monitoring, Custom Endpoint
- **Connection Endpoint:** Instance endpoint

#### RDS MySQL (Single Instance)
- **Supported Plugins:** IAM, Secrets Manager, Federated Authentication, Okta Authentication
- **Not Applicable:** Failover plugins (no cluster topology), Host Monitoring, Custom Endpoint
- **Connection Endpoint:** Instance endpoint

### Aurora Limitless Database (PostgreSQL Only)

- **Supported Plugins:** Limitless Connection Plugin, IAM, Secrets Manager, Federated Authentication, Okta Authentication
- **Database Engine:** Aurora PostgreSQL only (Aurora MySQL Limitless not yet available)
- **Not Recommended:** Failover, Host Monitoring, Read/Write Splitting (add unnecessary overhead)
- **Connection Endpoint:** DB shard group (limitless) endpoint
- **Special Features:** Client-side load balancing with load awareness
- **Recommended Configuration:**
  - Configure connection pool properties to work with load balancing
  - Consider `idleTimeout`, `maxLifetime`, `minimumIdle` for HikariCP

### Aurora Global Database

- **Supported on Primary Cluster:** All plugins work normally
- **Supported on Secondary Cluster:** Limited support
  - ✅ `failover` plugin works
  - ❌ `failover2` plugin does NOT work
  - ✅ Authentication plugins work
  - ✅ Monitoring plugins work
- **Not Supported:** Planned failover or switchover between regions
- **Important:** Must explicitly specify plugins when using Global Database to avoid default `failover2` plugin

## Known Limitations

### Blue/Green Deployments

**Service Dependency:**
Support for Blue/Green deployments requires specific metadata tables that are **not available in all RDS and Aurora service versions**.

**Version Requirements:**
- **RDS PostgreSQL:** rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21) and above
- **Aurora PostgreSQL:** Engine Release 17.5, 16.9, 15.13, 14.18, 13.21 and above
- **Aurora MySQL:** Engine Release 3.07 and above
- **RDS MySQL:** No specific version requirement

**Unsupported Configurations:**
- RDS MySQL and PostgreSQL Multi-AZ clusters
- Aurora Global Database for MySQL and PostgreSQL

**Additional Requirements:**
- AWS cluster and instance endpoints must be directly accessible
- CNAME aliases are not supported

**Fallback Behavior:**
If the metadata table does not exist, the driver will:
- Continue to work normally
- Log errors stating that relevant Blue/Green metadata cannot be found
- Fall back to previous behavior with limitations

**Limitations in Fallback Mode:**
- Post-switchover failures may occur
- Metadata inconsistencies can prevent reliable operation
- Topology detection may fail after switchover

### Aurora Global Database

**Current Limitations:**
- Does NOT support planned failover or switchover to secondary clusters
- Failing over to a secondary cluster will result in errors
- May experience unforeseen errors when working with global databases

**Supported Scenarios:**
- ✅ Connecting to primary cluster (fully supported)
- ⚠️ Connecting to secondary cluster (limited support)
  - `failover2` plugin will NOT work
  - `failover` plugin WILL work
  - Must explicitly provide plugin list to avoid default `failover2`

**Workaround:**
When working with Global Databases, explicitly specify plugins:
```
wrapperPlugins=failover,iam,efm
```

**Future Support:**
Full support for Aurora Global Databases is in the backlog, but no timeline is available.

### Host Monitoring with RDS Proxy

**Not Recommended:**
Using RDS Proxy endpoints with Enhanced Failure Monitoring (efm/efm2) is not recommended.

**Reasons:**
- RDS Proxy transparently re-routes requests to different instances
- Plugin cannot identify which instance it's monitoring
- May result in false positive failure detections

**Recommendation:**
Either disable Host Monitoring Plugin or avoid using RDS Proxy endpoints when the plugin is active.

**Note:**
The plugin will still monitor network connectivity to RDS Proxy endpoints and report outages.

### Limitless Connection Plugin

**Not Recommended with:**
- Failover plugins (not designed for Limitless architecture)
- Host Monitoring plugins (adds unnecessary overhead)
- Read/Write Splitting plugin (not applicable to Limitless)

**Connection Pool Considerations:**
Connection pools can work against client-side load balancing. Consider setting:
- `idleTimeout` - reduce idle connections
- `maxLifetime` - increase connection lifetime
- `minimumIdle` - control minimum idle connections

### Plugin Loading Order

**Important for Host Monitoring Plugin:**
The Host Monitoring Plugin (efm/efm2) must be loaded at the end or as close to the end as possible.

**When used with Failover Plugin:**
Host Monitoring must be loaded AFTER the Failover Plugin.

**Example:**
```
wrapperPlugins=failover,iam,efm
```

**Incorrect:**
```
wrapperPlugins=efm,failover,iam  # Wrong order!
```

### Timeout Configuration Requirements

**Host Monitoring Plugin:**
Always provide a non-zero socket timeout or connect timeout value.

**Reason:**
Most JDBC drivers use 0 as default timeout. Without overriding, the plugin may wait forever to establish monitoring connections when database nodes are unavailable.

**Example:**
```java
properties.setProperty("connectTimeout", "30000");
properties.setProperty("socketTimeout", "30000");
```

**Blue/Green Plugin:**
Always ensure non-zero socket timeout or connect timeout values are provided.

### IAM Authentication Prerequisites

**Required Dependencies:**
IAM Authentication requires AWS Java SDK RDS v2.x to be included separately in the classpath.

**Dependency Options:**

1. **Full SDK (Recommended):**
   - `software.amazon.awssdk:rds` (~5.4MB, 22MB with dependencies)

2. **Minimal Dependencies (Limited disk space):**
   - `software.amazon.awssdk:http-client-spi`
   - `software.amazon.awssdk:auth`
   - Total: ~300KB

**Connection Requirements:**
- Host URL must be a valid Amazon endpoint
- Cannot use custom domain or IP address
- Example: `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`

**Database Engine Limitations:**
IAM database authentication is limited to certain database engines. Review [IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for details.

## Plugin Configuration Best Practices

### Default Plugin Configuration

**Since v2.3.3:**
If `wrapperPlugins` is not specified, the following plugins are enabled by default:
- `failover2` - Failover Plugin v2
- `efm2` - Host Monitoring Plugin v2

**To Use Different Plugins:**
Explicitly specify the plugins you want:
```
wrapperPlugins=failover,iam,efm
```

### Failover Plugin Selection

**Use failover2 (default) when:**
- Working with Aurora PostgreSQL or MySQL clusters
- Working with RDS Multi-AZ DB Clusters
- Need optimal performance and resource usage
- Not using Aurora Global Database secondary clusters

**Use failover when:**
- Working with Aurora Global Database secondary clusters
- Need compatibility with older configurations
- Experiencing issues with failover2

**Never use both:**
Do not use `failover` and `failover2` plugins simultaneously for the same connection.

### RDS Multi-AZ Optimization

**For Minimal Downtime:**
Configure `failoverClusterTopologyRefreshRateMs` to 100ms:
```
failoverClusterTopologyRefreshRateMs=100
```

**Trade-offs:**
- Lower value = faster failover detection
- Lower value = increased database workload during switchover
- Default 2000ms balances performance and overhead

### Blue/Green Deployment Planning

**Recommended Steps:**

1. Create Blue/Green Deployment
2. Deploy application with `bg` plugin
3. Wait several minutes for plugin to collect deployment status
4. Initiate switchover via AWS Console/CLI/API
5. Monitor switchover completion
6. Review switchover summary in logs (set log level to `FINE`)
7. Remove `bg` plugin after switchover completes
8. Delete Blue/Green Deployment

**Logging Configuration:**
Set log level to `FINE` for:
- Package: `software.amazon.jdbc.plugin.bluegreen`
- Class: `software.amazon.jdbc.plugin.bluegreen.BlueGreenStatusProvider`

## Version Testing

The AWS Advanced JDBC Wrapper is tested against the following versions:

### Community Databases
- **MySQL:** 8.0.36
- **PostgreSQL:** 16.2

### Aurora Databases
- **Aurora MySQL:** Default version + Latest release
- **Aurora PostgreSQL:** Default version + Latest release

Check the AWS Console "Create database" page for current default versions.

### Compatibility
- MySQL: Compatible with 5.7 and 8.0 (per Community MySQL Connector/J 8.0 Driver)
- PostgreSQL: Compatible with supported PostgreSQL versions

## Additional Resources

- [Getting Started Guide](./GettingStarted.md)
- [Using the JDBC Driver](./using-the-jdbc-driver/UsingTheJdbcDriver.md)
- [Failover Configuration Guide](./using-the-jdbc-driver/FailoverConfigurationGuide.md)
- [Plugin Documentation](./using-the-jdbc-driver/using-plugins/)
- [AWS RDS Multi-AZ DB Clusters Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html)
- [Aurora Limitless Database Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/limitless.html)
- [IAM Database Authentication Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html)
