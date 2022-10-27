# AWS IAM Authentication Plugin

## What is IAM?
AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports granular permissions, giving you the ability to grant different permissions to different users. For more information on IAM and it's use cases, please refer to the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## AWS IAM Database Authentication
**Note:** To preserve compatibility with customers using the community driver, IAM Authentication requires the [AWS Java SDK RDS v2.x](https://mvnrepository.com/artifact/software.amazon.awssdk/rds) to be included separately in the classpath. The AWS Java SDK RDS is a runtime dependency and must be resolved.

The Advanced JDBC Wrapper supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`

IAM database authentication use is limited to certain database engines. For more information on limitations and recommendations, please [review the IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

## How do I use IAM with the AWS Advanced JDBC Driver?
1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database authentication on the AWS RDS Console:
    1. If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
    2. If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
2. Set up an [AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication:
    1. Connect to your database of choice using primary logins.
        1. For a MySQL database, use the following command to create a new user:<br>
           `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`
        2. For a PostgreSQL database, use the following command to create a new user:<br>
           `CREATE USER db_userx;
           GRANT rds_iam TO db_userx;`

| Parameter         |  Value  | Required | Description                                                                                                                                                                                                                                                                                                            | Example Value                                       |
|-------------------|:-------:|:--------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `wrapperPlugins`  | String  |    No    | Set to `"iam"` to enable AWS IAM database authentication                                                                                                                                                                                                                                                               | `iam`                                               |
| `iamDefaultPort`  | String  |    No    | This property will override the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol. For now, there is support for `jdbc:postgresql:` and `jdbc:mysql:`. Target drivers with different protocols will require users to provide a default port. | `1234`                                              |
| `iamHost`         | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string. This parameter is useful when users are connecting with custom endpoints.                                                                                         | `database.cluster-hash.us-east-1.rds.amazonaws.com` |
| `iamRegion`       | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string.                                                                                                                                                                        | `us-east-2`                                         |
| `iamExpiration`   | Integer |    No    | This property will override the default expiration time that is assigned to the generated IAM token. The default expiration time is set to be 15 minutes.                                                                                                                                                              | `600`                                               |

## Sample code
[AwsIamAuthenticationPostgresqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationPostgresqlExample.java)<br>
[AwsIamAuthenticationMysqlExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMysqlExample.java)<br>
[AwsIamAuthenticationMariadbExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/AwsIamAuthenticationMariadbExample.java)
