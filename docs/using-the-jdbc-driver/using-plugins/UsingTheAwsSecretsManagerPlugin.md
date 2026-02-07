# AWS Secrets Manager Plugin

The AWS Advanced JDBC Wrapper supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Plugin Availability
The plugin is available since version 1.0.0.


## Enabling the AWS Secrets Manager Connection Plugin
> [!WARNING]\
> To use this plugin, you must include the runtime dependencies [Jackson Databind](https://central.sonatype.com/artifact/com.fasterxml.jackson.core/jackson-databind) and [AWS Secrets Manager](https://central.sonatype.com/artifact/software.amazon.awssdk/secretsmanager) in your project. These dependencies are required for the AWS Advanced JDBC Wrapper to pass database credentials to the underlying driver.

> [!WARNING]\
> To use this plugin, you must provide valid AWS credentials. The AWS SDK relies on the AWS SDK credential provider chain to authenticate with AWS services. If you are using temporary credentials (such as those obtained through AWS STS, IAM roles, or SSO), be aware that these credentials have an expiration time. AWS SDK exceptions will occur and the plugin will not work properly if your credentials expire without being refreshed or replaced. To avoid interruptions:
> - Ensure your credential provider supports automatic refresh (most AWS SDK credential providers do this automatically)
> - Monitor credential expiration times in production environments
> - Configure appropriate session durations for temporary credentials
> - Implement proper error handling for credential-related failures
>
> For more information on configuring AWS credentials, see our [AWS credentials documentation](../AwsCredentials.md)

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `awsSecretsManager` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

This plugin requires valid AWS credentials. See more details at [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md)

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## AWS Secrets Manager Connection Plugin Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager.

> **Note:** To use this plugin, you will need to set the following AWS Secrets Manager specific parameters.

| Parameter                              |  Value  |                         Required                         | Description                                                                                                                                                                                                                      | Example                 | Default Value |
|----------------------------------------|:-------:|:--------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|---------------|
| `secretsManagerSecretId`               | String  |                           Yes                            | Set this value to be the secret name or the secret ARN.                                                                                                                                                                          | `secretId`              | `null`        |
| `secretsManagerRegion`                 | String  | Yes unless the `secretsManagerSecretId` is a Secret ARN. | Set this value to be the region your secret is in.                                                                                                                                                                               | `us-east-2`             | `us-east-1`   |
| `secretsManagerEndpoint`               | String  |                            No                            | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required. | `http://localhost:1234` | `null`        |
| `secretsManagerExpirationSec`          | Integer |                            No                            | This property sets the time in seconds that secrets are cached before it is re-fetched.                                                                                                                                          | `600`                   | `870`         |
| `secretsManagerSecretUsernameProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the username for database connection.                                                                                                                              | `writerUsername`        | `username`    |
| `secretsManagerSecretPasswordProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the password for database connection.                                                                                                                              | `readerPassword`        | `password`    |

*NOTE* A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`

## Secret Data
The secret stored in the AWS Secrets Manager should be a JSON object containing the properties `username` and `password`. If the secret contains different key names, you can specify them with the `secretsManagerSecretUsernameProperty` and `secretsManagerSecretPasswordProperty` parameters.

### Example
[AwsSecretsManagerConnectionPluginPostgresqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsSecretsManagerConnectionPluginPostgresqlExample.java)
demonstrates using the AWS Advanced JDBC Wrapper to make a connection to a PostgreSQL database using credentials fetched from the AWS Secrets Manager.
