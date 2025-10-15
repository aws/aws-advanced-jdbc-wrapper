# Blue/Green Deployment Plugin

## What is Blue/Green Deployment?

The [Blue/Green Deployment](https://docs.aws.amazon.com/whitepapers/latest/blue-green-deployments/introduction.html) technique enables organizations to release applications by seamlessly shifting traffic between two identical environments running different versions of the application. This strategy effectively mitigates common risks associated with software deployment, such as downtime and limited rollback capability.

The AWS JDBC Driver leverages the Blue/Green Deployment approach by intelligently managing traffic distribution between blue and green nodes, minimizing the impact of stale DNS data and connectivity disruptions on user applications.

## Prerequisites
> [!WARNING]\
> Currently Supported Database Deployments:
> - Aurora MySQL and PostgreSQL clusters
> - RDS MySQL and PostgreSQL instances
>
> Unsupported Database Deployments and Configurations:
> - RDS MySQL and PostgreSQL Multi-AZ clusters
> - Aurora Global Database for MySQL and PostgreSQL
>
> Additional Requirements:
> - AWS cluster and instance endpoints must be directly accessible from the client side
> - :warning: If connecting with non-admin users, permissions must be granted to the users so that the blue/green metadata table/function can be properly queried. If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly. Please see the [Connecting with non-admin users](#connecting-with-non-admin-users) section below.
> - Connecting to database nodes using CNAME aliases is not supported
>
> **Blue/Green Support Behaviour and Version Compatibility:**
>
> The AWS Advanced JDBC Wrapper now includes enhanced full support for Blue/Green Deployments. This support requires a minimum database version that includes a specific metadata table. The metadata will be accessible provided the green deployment satisfies the minimum version compatibility requirements. This constraint **does not** apply to RDS MySQL.
>
> For RDS Postgres, you will also need to manually install the `rds_tools` extension using the following DDL so that the metadata required by the wrapper is available:
>
> ```sql
> CREATE EXTENSION rds_tools;
> ```
>
> If your database version does **not** support this table, the driver will automatically detect its absence and fallback to its previous behaviour. In this fallback mode, Blue/Green handling is subject to the same limitations listed above.
>
> **No action is required** if your database does not include the new metadata table -- the driver will continue to operate as before. If you have questions or encounter issues, please open an issue in this repository.
>
> Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.<br>
> Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.<br>
> Supported Aurora MySQL Versions: Engine Release `3.07` and above.


## What is Blue/Green Deployment Plugin?

During a [Blue/Green switchover](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html), several significant changes occur to your database configuration:
- Connections to blue nodes terminate at a specific point during the transition
- Node connectivity may be temporarily impacted due to reconfigurations and potential node restarts
- Cluster and instance endpoints are redirected to different database nodes
- Internal database node names undergo changes
- Internal security certificates are regenerated to accommodate the new node names


All factors mentioned above may cause application disruption. The AWS Advanced JDBC Driver aims to minimize the application disruption during Blue/Green switchover by performing the following actions:
- Actively monitors Blue/Green switchover status and implements appropriate measures to suspend, pass-through, or re-route database traffic
- Prior to Blue/Green switchover initiation, compiles a comprehensive inventory of cluster and instance endpoints for both blue and green nodes along with their corresponding IP addresses
- During the active switchover phase, temporarily suspends execution of JDBC calls to blue nodes, which helps unload database nodes and reduces transaction lag for green nodes, thereby enhancing overall switchover performance
- Substitutes provided hostnames with corresponding IP addresses when establishing new blue connections, effectively eliminating stale DNS data and ensuring connections to current blue nodes
- During the brief post-switchover period, continuously monitors DNS entries, confirms that blue endpoints have been reconfigured, and discontinues hostname-to-IP address substitution as it becomes unnecessary
- Automatically rejects new connection requests to green nodes when the switchover is completed but DNS entries for green nodes remain temporarily available
- Intelligently detects switchover failures and rollbacks to the original state, implementing appropriate connection handling measures to maintain application stability


## How do I use Blue/Green Deployment Plugin with the AWS JDBC Driver?

To enable the Blue/Green Deployment functionality, add the plugin code `bg` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) parameter value.
The Blue/Green Deployment Plugin supports the following configuration parameters:

| Parameter                     |  Value  |                           Required                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             | Example Value            | Default Value |
|-------------------------------|:-------:|:------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|---------------|
| `bgdId`                       | String  | If using multiple Blue/Green Deployments, yes; otherwise, no | This parameter is optional and defaults to `1`. When supporting multiple Blue/Green Deployments (BGDs), this parameter becomes mandatory. Each connection string must include the `bgdId` parameter with a value that can be any number or string. However, all connection strings associated with the same Blue/Green Deployment must use identical `bgdId` values, while connection strings belonging to different BGDs must specify distinct values. | `1234`, `abc-1`, `abc-2` | `1`           |
| `bgConnectTimeoutMs`          | Integer |                              No                              | Maximum waiting time (in milliseconds) for establishing new connections during a Blue/Green switchover when blue and green traffic is temporarily suspended.                                                                                                                                                                                                                                                                                            | `30000`                  | `30000`       |
| `bgBaselineMs`                | Integer |                              No                              | The baseline interval (ms) for checking Blue/Green Deployment status. It's highly recommended to keep this parameter below 900000ms (15 minutes).                                                                                                                                                                                                                                                                                                       | `60000`                  | `60000`       |
| `bgIncreasedMs`               | Integer |                              No                              | The increased interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 500-2000 milliseconds.                                                                                                                                                                                                                                                                                                              | `1000`                   | `1000`        |
| `bgHighMs`                    | Integer |                              No                              | The high-frequency interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 50-500 milliseconds.                                                                                                                                                                                                                                                                                                           | `100`                    | `100`         |
| `bgSwitchoverTimeoutMs`       | Integer |                              No                              | Maximum duration (in milliseconds) allowed for switchover completion. If the switchover process stalls or exceeds this timeframe, the driver will automatically assume completion and resume normal operations.                                                                                                                                                                                                                                         | `180000`                 | `180000`      |
| `bgSuspendNewBlueConnections` | Boolean |                              No                              | Enables Blue/Green Deployment switchover to suspend new blue connection requests while the switchover process is in progress.                                                                                                                                                                                                                                                                                                                           | `false`                  | `false`       |

The plugin establishes dedicated monitoring connections to track Blue/Green Deployment status. To apply specific configurations to these monitoring connections, add the `blue-green-monitoring-` prefix to any configuration parameter, as shown in the following example:

```java
final Properties properties = new Properties();
// Configure the timeout values for all, non-monitoring connections.
properties.setProperty("connectTimeout", "30000");
properties.setProperty("socketTimeout", "30000");
// Configure different timeout values for the Blue/Green monitoring connections.
properties.setProperty("blue-green-monitoring-connectTimeout", "10000");
properties.setProperty("blue-green-monitoring-socketTimeout", "10000");
```

> [!WARNING]\
> **Always ensure you provide a non-zero socket timeout value or a connect timeout value to the Blue/Green Deployment Plugin**
>

## Connecting with non-admin users
> [!WARNING]\
> If connecting with non-admin users, permissions must be granted to the users so that the blue/green metadata table/function can be properly queried. If the permissions are not granted, the metadata table/function will not be visible and blue/green plugin functionality will not work properly.

| Environment       | Required permission statements                                                                                        |
|-------------------|-----------------------------------------------------------------------------------------------------------------------|
| Aurora Postgresql | None                                                                                                                  |
| RDS Postgresql    | `GRANT USAGE ON SCHEMA rds_tools TO your_user;`<br>`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO your_user;` |
| Aurora MySQL      | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |
| RDS MySQL         | `GRANT SELECT ON mysql.rds_topology TO 'your_user'@'%';`<br>`FLUSH PRIVILEGES;`                                       |

## Plan your Blue/Green switchover in advance

To optimize Blue/Green switchover support with the AWS JDBC Driver, advance planning is essential. Please follow these recommended steps:

1. Create a Blue/Green Deployment for your database.
2. Configure your application by incorporating the `bg` plugin along with any additional parameters of your choice, then deploy your application to the corresponding environment.
3. The order of steps 1 and 2 is flexible and can be performed in either sequence.
4. Allow sufficient time for the deployed application with the active Blue/Green plugin to collect deployment status information. This process typically requires several minutes.
5. Initiate the Blue/Green Deployment switchover through the AWS Console, CLI, or RDS API.
6. Monitor the process until the switchover completes successfully or rolls back. This may take several minutes.
7. Review the switchover summary in the application logs. This requires setting the log level to `FINE` for either the entire package `software.amazon.jdbc.plugin.bluegreen` or specifically for the class `software.amazon.jdbc.plugin.bluegreen.BlueGreenStatusProvider`.
8. Update your application by deactivating the `bg` plugin through its removal from your application configuration. Redeploy your application afterward. Note that an active Blue/Green plugin produces no adverse effects once the switchover has been completed.
9. Delete the Blue/Green Deployment through the appropriate AWS interface.
10. The sequence of steps 8 and 9 is flexible and can be executed in either order based on your preference.

Here's an example of a switchover summary. Time zero corresponds to the beginning of the active switchover phase. Time offsets indicate the start time of each specific switchover phase.
```
[2025-04-23 17:42:26.537] [FINE   ] [pool-30-thread-1] [software.amazon.jdbc.plugin.bluegreen.BlueGreenStatusProvider logSwitchoverFinalSummary] :
----------------------------------------------------------------------------
timestamp                         time offset (ms)                     event
----------------------------------------------------------------------------
2025-04-23T17:39:23.529507Z             -46468 ms               NOT_CREATED
2025-04-23T17:39:23.795213Z             -46202 ms                   CREATED
2025-04-23T17:40:07.411020Z              -2585 ms               PREPARATION
2025-04-23T17:40:09.996344Z                  0 ms               IN_PROGRESS
2025-04-23T17:40:17.429581Z               7434 ms                      POST
2025-04-23T17:40:35.853160Z              25857 ms    Green topology changed
2025-04-23T17:40:48.537135Z              38543 ms          Blue DNS updated
2025-04-23T17:42:23.163572Z             133174 ms         Green DNS removed
2025-04-23T17:42:26.536226Z             136547 ms                 COMPLETED
----------------------------------------------------------------------------
```
