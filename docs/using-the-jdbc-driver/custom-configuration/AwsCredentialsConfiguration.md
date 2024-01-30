# AWS Credentials Provider Configuration

### Applicable plugins: IamAuthenticationPlugin, AwsSecretsManagerPlugin

The `IamAuthenticationPlugin` and `AwsSecretsManagerPlugin` both require authentication via AWS credentials to provide the functionality they offer. In the plugin logic, the mechanism to locate your credentials is defined by passing in an `AwsCredentialsProvider` object to the applicable AWS SDK client. By default, an instance of `DefaultCredentialsProvider` will be passed, which locates your credentials using the default credential provider chain described [in this doc](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). If AWS credentials are provided by the `credentials` and `config` files ([Default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)), then it's possible to specify a profile name using the `awsProfile` configuration parameter. If no profile name is specified, a `[default]` profile is used.

If you would like to use a different `AwsCredentialsProvider` or would like to define your own mechanism for providing AWS credentials, you can do so using the methods defined in the `AwsCredentialsManager` class. To configure the plugins to use your own logic, you can call the `AwsCredentialsManager.setCustomHandler` method, passing in a lambda or `AwsCredentialsProviderHandler` that returns an `AwsCredentialsProvider`.

## Sample code
[AwsCredentialsManagerExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsCredentialsManagerExample.java)
