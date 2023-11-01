# Federated Authentication Connection Plugin (Proof of Concept)

## Assumptions
- AWS Aurora Database.
- IAM user set up to access database.
- IAM role is set up for access to database.
- ADFS is set up as a SAML Identity Provider for IAM.

## How to use it
- Add `federatedAuth`to the `wrapperPlugins` parameter. 
- Include the runtime dependencies, [AWS Java SDK RDS](https://search.maven.org/artifact/software.amazon.awssdk/rds) and [AWS Java SDK STS](https://search.maven.org/artifact/software.amazon.awssdk/sts).
- Specify the following parameters:

| Parameter               | Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value                            | Example Value                                          |
|-------------------------|:------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|--------------------------------------------------------|
| `wrapperPlugins`        | String |   Yes    | A comma separated list of connection plugin codes for which plugins the AWS JDBC Driver is to use.                                                                                                                                                                                                                                                                 | ``auroraConnectionTracker,failover,efm`` | `auroraConnectionTracker,failover,efm,federatedAuth`   |
| `idpHost`               | String |   Yes    | The hosting URL of the Identity Provider.                                                                                                                                                                                                                                                                                                                          | `null`                                   | `ec2amaz-ab3cdef.example.com`                          |
| `idpPort`               | String |    No    | The hosting port of Identity Provider.                                                                                                                                                                                                                                                                                                                             | `443`                                    | `1234`                                                 |
| `rpIdentifier`          | String |    No    | The relaying party identifier.                                                                                                                                                                                                                                                                                                                                     | `urn:amazon:webservices`                 |                                                        |
| `iamRoleArn`            | String |   Yes    | The ARN of the IAM Role that is to be assumed.                                                                                                                                                                                                                                                                                                                     | `null`                                   | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `iamIdpArn`             | String |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `null`                                   | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `iamRegion`             | String |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `null`                                   | `us-east-2`                                            |
| `federatedUserName`     | String |   Yes    | The federated user name.                                                                                                                                                                                                                                                                                                                                           | `null`                                   | `jimbob@example.com`                                   |
| `federatedUserPassword` | String |   Yes    | The federated user password.                                                                                                                                                                                                                                                                                                                                       | `null`                                   | `someRandomPassword`                                   |
| `user`                  | String |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `null`                                   | `some_user_name`                                       |

## Code Example

```java
public class FederatedAuthConnectionPluginExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://db-identifier.XYZ.us-east-2.rds.amazonaws.com:5432/employees";

  public static void main(String[] args) throws SQLException {
    // Set the AWS Federated Authentication Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "federatedAuth");
    properties.setProperty("idpHost", "ec2amaz-ab3cdef.example.com");
    properties.setProperty("iamRoleArn", "arn:aws:iam::123456789012:role/adfs_example_iam_role");
    properties.setProperty("iamIdpArn", "arn:aws:iam::123456789012:saml-provider/adfs_example");
    properties.setProperty("iamRegion", "us-east-2");
    properties.setProperty("federatedUserName", "someFederatedUsername@example.com");
    properties.setProperty("federatedUserPassword", "somePassword");
    properties.setProperty("user", "someIamUser");


    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery("SELECT 1")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
```
