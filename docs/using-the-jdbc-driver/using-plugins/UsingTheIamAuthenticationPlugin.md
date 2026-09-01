# AWS IAM Authentication Plugin

## Plugin Availability
The plugin is available since version 1.0.0.

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and it's use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites
> [!WARNING]\
> To preserve compatibility with customers using the community driver, IAM Authentication requires the [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) to be included separately in the classpath. The AWS Java SDK RDS is a runtime dependency and must be resolved.
> 
> Note: AWS Java SDK RDS may have transitive dependencies that are also required (ex. [AWS Java SDK Core](https://central.sonatype.com/artifact/software.amazon.awssdk/aws-core/)). If you are not using a package manager such as Maven or Gradle, please refer to Maven Central to determine these transitive dependencies.
>
> Since [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) size is around 5.4Mb (22Mb including all RDS SDK dependencies), some users may experience difficulties using the plugin due to limited available disk size.
> In such cases, the [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) dependency may be replaced with just two dependencies which have a smaller footprint (around 300Kb in total)<sup>1</sup>:
> - [software.amazon.awssdk:http-client-spi](https://central.sonatype.com/artifact/software.amazon.awssdk/http-client-spi)
> - [software.amazon.awssdk:auth](https://central.sonatype.com/artifact/software.amazon.awssdk/auth)
>
> It's recommended to use [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) when it's possible.

> [!WARNING]\
> To use this plugin, you must provide valid AWS credentials. The AWS SDK relies on the AWS SDK credential provider chain to authenticate with AWS services. If you are using temporary credentials (such as those obtained through AWS STS, IAM roles, or SSO), be aware that these credentials have an expiration time. AWS SDK exceptions will occur and the plugin will not work properly if your credentials expire without being refreshed or replaced. To avoid interruptions:
> - Ensure your credential provider supports automatic refresh (most AWS SDK credential providers do this automatically)
> - Monitor credential expiration times in production environments
> - Configure appropriate session durations for temporary credentials
> - Implement proper error handling for credential-related failures
>
> For more information on configuring AWS credentials, see our [AWS credentials documentation](../AwsCredentials.md).

To enable the IAM Authentication Connection Plugin, add the plugin code `iam` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

This plugin requires valid AWS credentials. See more details at [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md)

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## AWS IAM Database Authentication
The AWS Advanced JDBC Wrapper supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`

IAM database authentication use is limited to certain database engines. For more information on limitations and recommendations, please [review the IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

## How do I use IAM with the AWS Advanced JDBC Wrapper?
1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
    1. If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
    2. If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
2. Set up an [AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication. This will be the user specified in the connection string or connection properties.
    1. Connect to your database of choice using primary logins.
        1. For a MySQL database, use the following command to create a new user:<br>
           `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`<br>
           You might also need to grant extra permissions to the IAM user when connecting to RDS Multi-AZ deployments:<br>
           ```GRANT REPLICATION CLIENT ON *.* TO example_user_name@`%`;```
        2. For a PostgreSQL database, use the following command to create a new user:<br>
           `CREATE USER db_userx;
           GRANT rds_iam TO db_userx;`
4. Add the plugin code `iam` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) parameter value.

| Parameter                    |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                 | Example Value                                       |
|------------------------------|:-------:|:--------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `iamDefaultPort`             | String  |    No    | This property will override the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for `jdbc:postgresql:` and `jdbc:mysql:`. Target drivers with different protocols will require users to provide a default port.                                                                                                                      | `1234`                                              |
| `iamHost`                    | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string. This parameter is required when users are connecting with custom endpoints.                                                                                                                                                                                                            | `database.cluster-hash.us-east-1.rds.amazonaws.com` |
| `iamRegion`                  | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string.                                                                                                                                                                                                                                                                                             | `us-east-2`                                         |
| `iamExpiration`              | Integer |    No    | This property determines how long an IAM token is kept in the driver cache before a new one is generated. The default expiration time is set to be 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                                                                                                                                                                                   | `600`                                               |
| `iamAccessTokenPropertyName` | String  |    No    | This property allows you to override the property name used for passing the IAM access token. Some underlying drivers may require a specific property name for IAM authentication. Default value is `password`.                                                                                                                                                                                                                             | `password`, `accessToken`                           |
| `allowAwsLoginSession`       | Boolean |    No    | When `true`, allows resolving AWS credentials from an `awsProfile` that authenticates via a `login_session` entry (the AWS Sign-In / browser-login flow). The connection region (see `iamRegion`) is used for the login/signin exchange so the configured profile is honored instead of falling back to the `default` profile. Requires the optional `software.amazon.awssdk:signin` dependency on the classpath. Default value is `false`.<br><br>**Note:** This property is only needed with AWS SDK for Java v2 versions **before `2.51.1`**. Starting with `2.51.1`, the SDK resolves the profile's region correctly for the `login_session` sign-in exchange, so this workaround is no longer required and the property has no effect. If you are on `2.51.1` or later you can leave this property unset. | `true`                                              |

## Sample code
[AwsIamAuthenticationPostgresqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationPostgresqlExample.java)<br>
[AwsIamAuthenticationMysqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMysqlExample.java)<br>
[AwsIamAuthenticationMariadbExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMariadbExample.java)

## Using IAM Authentication with Global Databases

When using IAM authentication with [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), the IAM user or role requires the additional `rds:DescribeGlobalClusters` permission. This permission allows the driver to resolve the Global Database endpoint to the appropriate regional cluster for IAM token generation.

Example IAM policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds-db:connect",
                "rds:DescribeGlobalClusters"
            ],
            "Resource": "*"
        }
    ]
}

```

> [!NOTE]
> [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) is **required** when using this plugin with Global databases.

---
<sup>1</sup> Note: The smaller dependencies cannot be used with Global databases, which require the full [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) dependency.

## Multi-tenant clusters: per-tenant databases or IAM users

Applications that host multiple tenants on one Aurora cluster often give each tenant its own logical database and its own IAM database user. Tenants are added and removed over time, so a tenant's database and IAM user have a shorter lifetime than the cluster and than the application process.

The driver's topology monitor is shared per [`clusterId`](../ClusterId.md) and is created by whichever connection needs topology first. It keeps that connection's `user` and `database` for its whole lifetime and uses them for every connection it opens. If the tenant that happened to create the monitor is later removed, or its IAM user loses access, the monitor can no longer connect, and connections for every other tenant sharing the `clusterId` wait for a topology refresh that cannot complete.

Note that the topology monitor is installed whenever the driver detects an Aurora dialect, even when `iam` is the only plugin you enabled explicitly.

### Give the monitor its own IAM identity

Create a dedicated IAM database user for monitoring and pin it with the `topology-monitoring-` prefix, so the monitor's context does not depend on any tenant:

```java
final Properties props = new Properties();
props.setProperty("wrapperPlugins", "iam");
props.setProperty("iamRegion", "us-east-2");

// Tenant context - differs per tenant.
props.setProperty("user", tenant.getIamUser());

// Monitoring context - identical on every connection in the process.
props.setProperty("topology-monitoring-user", "topology_monitor");
props.setProperty("topology-monitoring-database", "postgres");

final Connection conn = DriverManager.getConnection(
    "jdbc:aws-wrapper:postgresql://db-cluster.cluster-XYZ.us-east-2.rds.amazonaws.com/"
        + tenant.getDatabase(),
    props);
```

The database name in the connection URL becomes the `database` connection property, so `topology-monitoring-database` overrides it for the monitoring connections only; application connections still reach the tenant's database.

The IAM plugin reads `user` and the `iam*` properties from the properties of the connection being opened, so the prefixed values are what it uses when generating a token for a monitoring connection. If you select credentials with a custom `AwsCredentialsProviderHandler` driven by a connection property (see [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md)), prefix that property as well so the handler resolves the monitoring identity.

> [!WARNING]\
> Set these properties on **every** connection in the process, including connections opened by pooling infrastructure, health checks, and schema migrations. Any connection can be the one that creates the topology monitor, so a connection that omits the overrides can still bind the monitor to its own tenant context.

### Permissions for the monitoring principal

The topology monitor only reads cluster metadata. On Aurora PostgreSQL:

```sql
CREATE ROLE topology_monitor LOGIN;
GRANT rds_iam TO topology_monitor;
GRANT CONNECT ON DATABASE postgres TO topology_monitor;
```

No `rds_superuser`, and no schema or table grants. `EXECUTE` on functions defaults to `PUBLIC`, so a plain login role normally works as-is; if you have revoked default public privileges you may also need to grant `EXECUTE` on `pg_catalog.aurora_replica_status()` and `pg_catalog.aurora_db_instance_identifier()`.

Scope the IAM policy to the **cluster** resource id rather than to a single instance. While re-discovering the cluster the monitor connects to each instance endpoint, not only the cluster endpoint, and the cluster resource id covers every instance, including ones added later by scaling:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["rds-db:connect"],
    "Resource": [
      "arn:aws:rds-db:us-east-2:123456789012:dbuser:cluster-ABCDEFGHIJKL01234/topology_monitor"
    ]
  }]
}
```

Any database that is guaranteed never to be dropped works as the monitoring target; `postgres` is a reasonable choice.

For the general behavior of the `topology-monitoring-` prefix, see [Configuring the topology monitor's connections](./UsingTheFailover2Plugin.md#configuring-the-topology-monitors-connections).

## Telemetry Metrics

When [telemetry](../Telemetry.md) is enabled and a metrics backend is configured through `telemetryMetricsBackend`, this plugin submits the following metrics:

| Metric name            | Metric type | Description                                                                                                                                                                                                                                                        |
|------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `iam.fetchToken.count` | Counter     | Number of IAM authentication tokens generated. Incremented when no valid cached token is available (cache miss, expired token, or `iamExpiration` set to `0`), and again when a login failure with a cached token triggers a retry with a freshly generated token. |
| `iam.tokenCache.size`  | Gauge       | Number of entries currently held in the authentication token cache.                                                                                                                                                                                                |

> [!NOTE]\
> The authentication token cache is shared between the `iam`, `federatedAuth`, and `okta` plugins. The `iam.tokenCache.size`, `federatedAuth.tokenCache.size`, and `oktaAuth.tokenCache.size` gauges therefore all report the size of the same cache.

See [Monitoring](../Telemetry.md#list-of-metrics) for the metrics submitted by other plugins.
