# AWS Secrets Manager Plugin

The AWS Advanced JDBC Driver supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Enabling the AWS Secrets Manager Connection Plugin
> :warning: **Note:** To use this plugin, you must include the runtime dependencies [Jackson Databind](https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind) and [AWS Secrets Manager](https://mvnrepository.com/artifact/software.amazon.awssdk/secretsmanager) in your project. These parameters are required for the AWS JDBC Driver to pass database credentials to the underlying driver.

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `awsSecretsManager` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

## AWS Secrets Manager Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager. 

> **Note:** To use this plugin, you will need to set the following AWS Secrets Manager specific parameters.

| Parameter                | Value  | Required | Description                                             | Example     | Default Value |
|--------------------------|:------:|:--------:|:--------------------------------------------------------|:------------|---------------|
| `secretsManagerSecretId` | String |   Yes    | Set this value to be the secret name or the secret ARN. | `secretId`  | `null`        |
| `secretsManagerRegion`   | String |   Yes    | Set this value to be the region your secret is in.      | `us-east-2` | `us-east-1`   |

### Example
[AwsSecretsManagerConnectionPluginPostgresqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginPostgresqlExample.java) 
demonstrates using the AWS Advanced JDBC Driver to make a connection to a PostgreSQL database using credentials fetched from the AWS Secrets Manager:
