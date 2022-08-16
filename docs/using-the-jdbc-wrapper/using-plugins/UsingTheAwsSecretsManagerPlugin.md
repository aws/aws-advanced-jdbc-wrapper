# AWS Secrets Manager Plugin

The AWS Advanced JDBC Wrapper supports usage of database credentials stored as secrets in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Connection Plugin. When you create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created with the credentials inside that secret.

## Enabling the AWS Secrets Manager Connection Plugin
> :warning: **Note:** To use this plugin, you must include the runtime dependencies [Jackson Databind](https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind) and [AWS Secrets Manager](https://mvnrepository.com/artifact/software.amazon.awssdk/secretsmanager) in your project. These parameters are required for the JDBC Wrapper to pass database credentials to the underlying driver.

To enable the AWS Secrets Manager Connection Plugin, add the plugin code `awsSecretsManager` to the [`wrapperPlugins`](../UsingTheJdbcWrapper.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcWrapper.md#connection-plugin-manager-parameters).

## AWS Secrets Manager Parameters
The following properties are required for the AWS Secrets Manager Connection Plugin to retrieve database credentials from the AWS Secrets Manager. 

> **Note:** To use this plugin, you will need to set the following AWS Secrets Manager specific parameters, as well as the [`wrapperTargetDriverUserPropertyName`](../UsingTheJdbcWrapper.md#aws-advanced-jdbc-wrapper-parameters) and [`wrapperTargetDriverPasswordPropertyName`](../UsingTheJdbcWrapper.md#aws-advanced-jdbc-wrapper-parameters) JDBC Wrapper parameters.

| Parameter                | Value  | Required | Description                                             | Example     | Default Value |
|--------------------------|:------:|:--------:|:--------------------------------------------------------|:------------|---------------|
| `secretsManagerSecretId` | String |   Yes    | Set this value to be the secret name or the secret ARN. | `secretId`  | `null`        |
| `secretsManagerRegion`   | String |   Yes    | Set this value to be the region your secret is in.      | `us-east-2` | `us-east-1`   |

### Example
The following example demonstrates using the AWS Advanced JDBC Wrapper to make a connection to a PostgreSQL database using credentials fetched from the AWS Secrets Manager:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class AwsSecretsManagerConnectionPluginPostgresqlSample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/employees";

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    // Set the AWS Secrets Manager Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();
    properties.setProperty("secretsManagerRegion", "us-east-2");
    properties.setProperty("secretsManagerSecretId", "secretId");
    properties.setProperty("wrapperTargetDriverUserPropertyName", "user");
    properties.setProperty("wrapperTargetDriverPasswordPropertyName", "password");

    // Enable the AWS Secrets Manager Connection Plugin.
    properties.setProperty(
        "wrapperPlugins",
        "awsSecretsManager");

    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
      while (rs.next()) {
        System.out.println(rs.getString("first_name"));
      }
    }
  }
}
```
