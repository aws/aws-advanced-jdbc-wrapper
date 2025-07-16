# AWS Aurora DSQL IAM Authentication Plugin

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and it's use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites
> [!WARNING]\
> To use this plugin, you must include the runtime dependency [AWS Java SDK DSQL](https://central.sonatype.com/artifact/software.amazon.awssdk/dsql) in your project.
>
> Note: AWS Java SDK DSQL may have transitive dependencies that are also required (ex. [AWS Java SDK Core](https://central.sonatype.com/artifact/software.amazon.awssdk/aws-core/)). If you are not using a package manager such as Maven or Gradle, please refer to Maven Central to determine these transitive dependencies.

To enable the AWS Aurora DSQL IAM Authentication Plugin, add the plugin code `iamDsql` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

> [!WARNING]\
> The `iam` plugin must NOT be specified when using the `iamDsql` plugin.

This plugin requires valid AWS credentials. See more details at [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md).

## AWS Aurora DQSL IAM Database Authentication
The AWS JDBC Driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `cluster-identifier.dsql.us-east-1.on.aws`

Connections established by the `iamDsql` plugin are beholden to the [Cluster quotas and database limits in Amazon Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html). In particular, applications need to consider the maximum transaction duration, and maximum connection duration limits. Ensure connections are returned to the pool regularly, and not retained for long periods.

## How do I use IAM with Aurora DQSL and the AWS JDBC Driver?
1. Configure IAM roles for the cluster according to [Using database roles and IAM authentication](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/using-database-and-iam-roles.html).
2. Add the plugin code `iamDsql` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) parameter value.

| Parameter         |  Value  | Required | Description                                                                                                                                                                                                                                                              | Example Value                              |
|-------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `iamHost`         | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string.                                                                                                                     | `cluster-identifier.dsql.us-east-1.on.aws` |
| `iamRegion`       | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string where possible. Some connection string formats may not be supported, and the `iamRegion` must be provided in these cases. | `us-east-2`                                |
| `iamExpiration`   | Integer |    No    | This property determines how long an IAM token is kept in the driver cache before a new one is generated. The default expiration time is set to be 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                | `600`                                      |

## Sample code
[AwsAuroraDsqlIamAuthenticationExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsAuroraDsqlIamAuthenticationExample.java)<br>
[AwsAuroraDsqlIamAuthenticationDatasourceExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsAuroraDsqlIamAuthenticationDatasourceExample.java)
