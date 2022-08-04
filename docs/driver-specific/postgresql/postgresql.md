# Using the PostgreSQL JDBC Driver with AWS Advanced JDBC Wrapper

### Prerequisites

- Docker Desktop:
  - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
  - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

##### Aurora Test Requirements
- An AWS account with:
  - RDS permissions
  - EC2 permissions so integration tests can whitelist the current IP address in the Aurora cluster's EC2 security group.
  - For more information see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

- An available Aurora PostgreSQL DB cluster is required if you're running the tests against an existing DB cluster.

### Running the Integration Tests

First ensure you set up the needed environment variables (replace the variables with the appropriate values):
macOS:
```bash
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY  AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> AURORA_POSTGRES_CLUSTER_IDENTIFIER=XYZ.us-east-2.rds.amazonaws.com AURORA_POSTGRES_USERNAME=username AURORA_POSTGRES_PASSWORD=password AURORA_POSTGRES_DB_REGION=us-east-2```
```
Windows:
```bash
set AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY | set AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE | set AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> | set AURORA_POSTGRES_CLUSTER_IDENTIFIER=XYZ.us-east-2.rds.amazonaws.com | set AURORA_POSTGRES_USERNAME=username | set AURORA_POSTGRES_PASSWORD=password | set AURORA_POSTGRES_DB_REGION=us-east-2```
```
#### Aurora Integration Tests

The Aurora integration tests are focused on testing connection strings and failover capabilities of any driver
PostgreSQL tests are currently supported, MySQL tests will be added in the future.
The tests are run in docker but make a connection to test against an Aurora cluster.

macOS:
```bash
./gradlew --no-parallel --no-daemon test-integration-aurora-postgres
```
Windows:
```bash
cmd /c gradlew --no-parallel --no-daemon test-integration-aurora-postgres
```
#### Standard Integration Tests

The Standard integration tests are focused on testing connection strings against a local database inside a docker container.
PostgreSQL and mySQL tests are currently supported.

macOS:
```bash
./gradlew --no-parallel --no-daemon test-integration-standard-postgres
```
Windows:
To run the integration tests on Windows, use the following command:
```bash
cmd /c gradlew --no-parallel --no-daemon test-integration-standard-postgres
``` 

### Environment Variables

#### Aurora
| Environment Variable | Required |                                                                                                    Description                                                                                                    | Example Value                              |
| ---------------- |:--------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|--------------------------------------------|
| `AWS_ACCESS_KEY_ID` |   Yes    |                                                                    An AWS access key associated with an IAM user or role with RDS permissions.                                                                    | ASIAIOSFODNN7EXAMPLE                       |
| `AWS_SECRET_ACCESS_KEY` |   Yes    |                                                                          The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                           | wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY   |
| `AWS_SESSION_TOKEN` |    No    | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html).. | AQoDYXdzEJr...<remainder of session token> |
| `AURORA_POSTGRES_CLUSTER_IDENTIFIER` |   Yes    |                                               The database identifier for your Aurora cluster. Must be a unique value to avoid conflicting with existing clusters.                                                | db-identifier                              |
| `AURORA_POSTGRES_USERNAME` |   Yes    |                                                                                       The username to access the database.                                                                                        | username                                   |
| `AURORA_POSTGRES_PASSWORD` |   Yes    |                                                                                          The database cluster password.                                                                                           | password                                   |
| `AURORA_POSTGRES_DB_REGION` |   Yes    |                                                                                               The database region.                                                                                                | us-east-2                                  |
| `DB_CONN_STR_SUFFIX` |    No    |                                                          The existing database connection suffix. Use this variable to run against an existing database.                                                          | XYZ.us-east-2.rds.amazonaws.com            |
| `AURORA_POSTGRES_DB` |    No    |                                                             Name of the database that will be used by the tests. The default database name is `test`.                                                             | test_db_name                               |


#### Standard
| Environment Variable         | Required |                                                          Description                                                           | Example Value |
|------------------------------|:--------:|:------------------------------------------------------------------------------------------------------------------------------:|---------------|
| `STANDARD_POSTGRES_USERNAME` |    No    |                                              The username to access the database.                                              | username      |
| `STANDARD_POSTGRES_PASSWORD` |    No    |                                                 The database cluster password.                                                 | password      |
| `STANDARD_POSTGRES_DB`       |    No    |   Name of the database that will be used by the tests. The default database name is `test`.    | test_db_name  |

