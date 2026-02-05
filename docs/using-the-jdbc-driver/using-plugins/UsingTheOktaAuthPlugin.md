# Okta Authentication Plugin

The Okta Authentication Plugin adds support for authentication via Federated Identity and then database access via IAM.

## Plugin Availability
The plugin is available since version 2.3.6.

## What is Federated Identity
Federated Identity allows users to use the same set of credentials to access multiple services or resources across different organizations. This works by having Identity Providers (IdP) that manage and authenticate user credentials, and Service Providers (SP) that are services or resources that can be internal, external, and/or belonging to various organizations. Multiple SPs can establish trust relationships with a single IdP.

When a user wants access to a resource, it authenticates with the IdP. From this a security token generated and is passed to the SP then grants access to said resource.
In the case of AD FS, the user signs into the AD FS sign in page. This generates a SAML Assertion which acts as a security token. The user then passes the SAML Assertion to the SP when requesting access to resources. The SP verifies the SAML Assertion and grants access to the user.

## Prerequisites
- This plugin requires the following runtime dependencies to be registered separately in the classpath:
  - [AWS Java SDK RDS v2.7.x or later](https://central.sonatype.com/artifact/software.amazon.awssdk/rds)
  - [AWS Java SDK STS v2.7.x or later](https://central.sonatype.com/artifact/software.amazon.awssdk/sts)
  - [jsoup](https://central.sonatype.com/artifact/org.jsoup/jsoup)
  - [Jackson Databind](https://central.sonatype.com/artifact/com.fasterxml.jackson.core/jackson-databind)
- Note: The above dependencies may have transitive dependencies that are also required (ex. AWS Java SDK RDS requires [AWS Java SDK Core](https://central.sonatype.com/artifact/software.amazon.awssdk/aws-core/)). If you are not using a package manager such as Maven or Gradle, please refer to Maven Central to determine these transitive dependencies. 
- This plugin does not create or modify any Okta or IAM resources. Okta must be federated into to your AWS IAM account before using this plugin. You can follow Okta's [integration guide](https://help.okta.com/en-us/content/topics/deploymentguides/aws/aws-deployment.htm). In addition, all permissions and policies must be correctly configured before using this plugin. If you plan on using [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/) with this plugin, please see the [Using Okta Authentication with Global Databases](#using-okta-authentication-with-global-databases) section as well.

> [!WARNING]\
> To use this plugin, you must provide valid AWS credentials. The AWS SDK relies on the AWS SDK credential provider chain to authenticate with AWS services. If you are using temporary credentials (such as those obtained through AWS STS, IAM roles, or SSO), be aware that these credentials have an expiration time. AWS SDK exceptions will occur and the plugin will not work properly if your credentials expire without being refreshed or replaced. To avoid interruptions:
> - Ensure your credential provider supports automatic refresh (most AWS SDK credential providers do this automatically)
> - Monitor credential expiration times in production environments
> - Configure appropriate session durations for temporary credentials
> - Implement proper error handling for credential-related failures
>
> For more information on configuring AWS credentials, see our [AWS credentials documentation](../AwsCredentials.md).

> [!NOTE]\
> Since [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) size is around 5.4Mb (22Mb including all RDS SDK dependencies), some users may experience difficulties using the plugin due to limited available disk size.
> In such cases, the [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) dependency may be replaced with just two dependencies which have a smaller footprint (around 300Kb in total):
> - [software.amazon.awssdk:http-client-spi](https://central.sonatype.com/artifact/software.amazon.awssdk/http-client-spi)
> - [software.amazon.awssdk:auth](https://central.sonatype.com/artifact/software.amazon.awssdk/auth)
>
> It's recommended to use [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds) when it's possible.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## How to use the Okta Authentication Plugin with the AWS Advanced JDBC Wrapper 

### Enabling the Okta Authentication Plugin
> [!NOTE]\
> AWS IAM database authentication is needed to use the Okta Authentication Plugin. This is because after the plugin
> acquires SAML assertion from the identity provider, the SAML Assertion is then used to acquire an AWS IAM token. The AWS
> IAM token is then subsequently used to access the database.

1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
   - If needed, review the documentation about [IAM authentication for MariaDB, MySQL, and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).
2. Configure Okta as the AWS identity provider.
    - If needed, review the documentation about [Amazon Web Services Account Federation](https://help.okta.com/en-us/content/topics/deploymentguides/aws/aws-deployment.htm) on Okta's documentation.
3. Add the plugin code `okta` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).
4. Specify parameters that are required or specific to your case.

### Okta Authentication Plugin Parameters
| Parameter                  |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value            | Example Value                                          |
|----------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|--------------------------------------------------------|
| `dbUser`                   | String  |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `null`                   | `some_user_name`                                       |
| `idpUsername`              | String  |   Yes    | The user name for the `idpEndpoint` server. If this parameter is not specified, the plugin will fallback to using the `user` parameter.                                                                                                                                                                                                                            | `null`                   | `jimbob@example.com`                                   |
| `idpPassword`              | String  |   Yes    | The password associated with the `idpEndpoint` username. If this parameter is not specified, the plugin will fallback to using the `password` parameter.                                                                                                                                                                                                           | `null`                   | `someRandomPassword`                                   |
| `idpEndpoint`              | String  |   Yes    | The hosting URL for the service that you are using to authenticate into AWS Aurora.                                                                                                                                                                                                                                                                                | `null`                   | `ec2amaz-ab3cdef.example.com`                          |
| `appId`                    | String  |   Yes    | The Amazon Web Services (AWS) app [configured](https://help.okta.com/en-us/content/topics/deploymentguides/aws/aws-configure-aws-app.htm) on Okta.                                                                                                                                                                                                                 | `null`                   | `ec2amaz-ab3cdef.example.com`                          |
| `iamRoleArn`               | String  |   Yes    | The ARN of the IAM Role that is to be assumed to access AWS Aurora.                                                                                                                                                                                                                                                                                                | `null`                   | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `iamIdpArn`                | String  |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `null`                   | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `iamRegion`                | String  |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `null`                   | `us-east-2`                                            |
| `idpPort`                  | String  |    No    | The port that the host for the authentication service listens at.                                                                                                                                                                                                                                                                                                  | `urn:amazon:webservices` | `urn:amazon:webservices`                               |
| `iamHost`                  | String  |    No    | Overrides the host that is used to generate the IAM token.                                                                                                                                                                                                                                                                                                         | `null`                   | `database.cluster-hash.us-east-1.rds.amazonaws.com`    |
| `iamDefaultPort`           | String  |    No    | This property overrides the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for `jdbc:postgresql:` and `jdbc:mysql:`. Target drivers with different protocols will require users to provide a default port.                                                 | `null`                   | `1234`                                                 |
| `iamTokenExpiration`       | Integer |    No    | Overrides the default IAM token cache expiration in seconds                                                                                                                                                                                                                                                                                                        | `870`                    | `123`                                                  |
| `httpClientSocketTimeout`  | Integer |    No    | The socket timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin.                                                                                                                                                                                                                                                                            | `60000`                  | `60000`                                                |
| `httpClientConnectTimeout` | Integer |    No    | The connect timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin.                                                                                                                                                                                                                                                                           | `60000`                  | `60000`                                                |
| `sslInsecure`              | Boolean |    No    | Indicates whether or not the SSL connection is secure or not. If not, it will allow SSL connections to be made without validating the server's certificates.                                                                                                                                                                                                       | `false`                  | `true`                                                 |

## Sample code
[OktaAuthPluginExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/OktaAuthPluginExample.java)

## Using Okta Authentication with Global Databases

When using Okta authentication with [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), the IAM user or role requires the additional `rds:DescribeGlobalClusters` permission. This permission allows the driver to resolve the Global Database endpoint to the appropriate regional cluster for IAM token generation.

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
