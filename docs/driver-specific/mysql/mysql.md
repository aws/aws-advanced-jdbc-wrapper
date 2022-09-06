# Using the MySQL JDBC Driver with AWS Advanced JDBC Wrapper

### Running the Integration Tests

First ensure you set up the needed environment variables (replace the variables with the appropriate values):

macOS:
```bash
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY  AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> AURORA_MYSQL_CLUSTER_IDENTIFIER=XYZ.us-east-2.rds.amazonaws.com AURORA_MYSQL_USERNAME=username AURORA_MYSQL_PASSWORD=password AURORA_MYSQL_DB_REGION=us-east-2```
```
Windows:
```bash
set AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY | set AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE | set AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> | set AURORA_MYSQL_CLUSTER_IDENTIFIER=XYZ.us-east-2.rds.amazonaws.com | set AURORA_MYSQL_USERNAME=username | set AURORA_MYSQL_PASSWORD=password | set AURORA_MYSQL_DB_REGION=us-east-2```
```
#### Aurora Integration Tests
The following command will run the MySQL Aurora integration test suite.

macOS:
```bash
./gradlew --no-parallel --no-daemon test-integration-aurora-mysql
```
Windows:
```bash
cmd /c gradlew --no-parallel --no-daemon test-integration-aurora-mysql
```
#### Standard Integration Tests
The following command will run the MySQL standard integration test suite.

macOS:
```bash
./gradlew --no-parallel --no-daemon test-integration-standard-mysql
```
Windows:
```bash
cmd /c gradlew --no-parallel --no-daemon test-integration-standard-mysql
``` 

### Environment Variables

#### Aurora
| Environment Variable              | Required |                                                                                                    Description                                                                                                    | Example Value                              |
|-----------------------------------|:--------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|--------------------------------------------|
| `AWS_ACCESS_KEY_ID`               |   Yes    |                                                                    An AWS access key associated with an IAM user or role with RDS permissions.                                                                    | ASIAIOSFODNN7EXAMPLE                       |
| `AWS_SECRET_ACCESS_KEY`           |   Yes    |                                                                          The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                           | wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY   |
| `AWS_SESSION_TOKEN`               |    No    | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html).. | AQoDYXdzEJr...<remainder of session token> |
| `AURORA_MYSQL_CLUSTER_IDENTIFIER` |   Yes    |                                               The database identifier for your Aurora cluster. Must be a unique value to avoid conflicting with existing clusters.                                                | db-identifier                              |
| `AURORA_MYSQL_USERNAME`           |   Yes    |                                                                                       The username to access the database.                                                                                        | username                                   |
| `AURORA_MYSQL_PASSWORD`           |   Yes    |                                                                                          The database cluster password.                                                                                           | password                                   |
| `AURORA_MYSQL_DB_REGION`          |   Yes    |                                                                                               The database region.                                                                                                | us-east-2                                  |
| `DB_CONN_SUFFIX`                  |    No    |                                                          The existing database connection suffix. Use this variable to run against an existing database.                                                          | XYZ.us-east-2.rds.amazonaws.com            |
| `AURORA_MYSQL_DB`                 |    No    |                                                             Name of the database that will be used by the tests. The default database name is `test`.                                                             | test_db_name                               |


#### Standard
| Environment Variable      | Required |                                                          Description                                                           | Example Value |
|---------------------------|:--------:|:------------------------------------------------------------------------------------------------------------------------------:|---------------|
| `STANDARD_MYSQL_USERNAME` |    No    |                                              The username to access the database.                                              | username      |
| `STANDARD_MYSQL_PASSWORD` |    No    |                                                 The database cluster password.                                                 | password      |
| `STANDARD_MYSQL_DB`       |    No    |   Name of the database that will be used by the tests. The default database name is `test`.    | test_db_name  |
