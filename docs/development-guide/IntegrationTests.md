# Integration Tests

### Prerequisites

- Docker Desktop:
    - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

##### Aurora Test Requirements
- An AWS account with:
    - RDS permissions
    - EC2 permissions so integration tests can whitelist the current IP address in the Aurora cluster's EC2 security group.
    - For more information see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

- An available Aurora PostgreSQL or MySQL DB cluster is required if you're running the tests against an existing DB cluster.

### Aurora Integration Tests

The Aurora integration tests are focused on testing connection strings and failover capabilities of any driver.
The tests are run in Docker but make a connection to test against an Aurora cluster.
PostgreSQL and MySQL tests are currently supported.

### Standard Integration Tests

The Standard integration tests are focused on testing connection strings against a local database inside a Docker container.
PostgreSQL and MySQL tests are currently supported.

### Running the Integration Tests
Please view [this](/docs/driver-specific/postgresql/postgresql.md) page for further instructions on how to run the PostgreSQL integration tests.

Please view [this](docs/driver-specific/mysql/mysql.md) page for further instructions on how to run the MySQL integration tests.

