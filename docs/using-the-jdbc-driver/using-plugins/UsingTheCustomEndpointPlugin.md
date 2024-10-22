# Custom Endpoint Plugin

The Custom Endpoint Plugin adds support for RDS custom endpoints. When the Custom Endpoint Plugin is in use, the driver will analyse custom endpoint information to ensure instances used in connections are part of the custom endpoint being used. This includes connections used in failover and read-write splitting.

## Prerequisites
- This plugin requires the following runtime dependencies to be registered separately in the classpath:
    - [AWS Java SDK RDS v2.7.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds)
- Note: The above dependencies may have transitive dependencies that are also required (ex. AWS Java SDK RDS requires [AWS Java SDK Core](https://central.sonatype.com/artifact/software.amazon.awssdk/aws-core/)). If you are not using a package manager such as Maven or Gradle, please refer to Maven Central to determine these transitive dependencies.

## How to use the Custom Endpoint Plugin with the AWS JDBC Driver

### Enabling the Custom Endpoint Plugin

1. If needed, create a custom endpoint using the AWS RDS Console:
    - If needed, review the documentation about [creating a custom endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-custom-endpoint-creating.html).
2. Add the plugin code `customEndpoint` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).
3. If you are using the failover plugin, set the failover parameter `failoverMode` according to the custom endpoint type. For example, if the custom endpoint you are using is of type `READER`, you can set `failoverMode` to `strict-reader`, or if it is of type `ANY`, you can set `failoverMode` to `reader-or-writer`.
4. Specify parameters that are required or specific to your case.

### Custom Endpoint Plugin Parameters

| Parameter                            |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                          | Default Value         | Example Value |
|--------------------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|---------------|
| `customEndpointRegion`               | String  |    No    | The region of the cluster's custom endpoints. If not specified, the region will be parsed from the URL.                                                                                                                                                                                                                              | `null`                | `us-west-1`   |
| `customEndpointInfoRefreshRateMs`    | Integer |    No    | Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds.                                                                                                                                                                                                                                        | `30000`               | `20000`       |
| `customEndpointMonitorExpirationMs`  | Integer |    No    | Controls how long a monitor should run without use before expiring and being removed, in milliseconds.                                                                                                                                                                                                                               | `900000` (15 minutes) | `600000`      |
| `waitForCustomEndpointInfo`          | Boolean |    No    | Controls whether to wait for custom endpoint info to become available before connecting or executing a method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used recently. Note that disabling this may result in occasional connections to instances outside of the custom endpoint. | `true`                | `true`        |
| `waitForCustomEndpointInfoTimeoutMs` | Integer |    No    | Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made available by the custom endpoint monitor, in milliseconds.                                                                                                                                                                         | `5000`                | `7000`        |
