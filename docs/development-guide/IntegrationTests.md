# Integration Tests

### Prerequisites

- Docker Desktop:
    - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Environment variables](#Environment-Variables)

##### Aurora Test Requirements
- An AWS account with:
    - RDS permissions
    - EC2 permissions so integration tests can add the current IP address in the Aurora cluster's EC2 security group.
    - For more information, see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

- An available Aurora PostgreSQL or MySQL DB cluster is required if you're running the tests against an existing DB cluster.

### Aurora Integration Tests

The Aurora integration tests are focused on testing connection strings and failover capabilities of any driver.
The tests are run in Docker but make a connection to test against an Aurora cluster.
PostgreSQL and MySQL tests are currently supported.

### Standard Integration Tests

These integration tests are focused on testing connection strings against a local database inside a Docker container.
PostgreSQL and MySQL tests are currently supported.

### Environment Variables

Every environment is provisioned for the run and deleted afterwards, so the database identifiers,
usernames and passwords are chosen by the test framework rather than configured. You need a working
Docker environment for any of the tests, because the suite itself runs inside a container; the
containers are created automatically, so no Docker commands need to be run by hand.

| Environment Variable Name | Required            | Description                                                                                                                                                                                                     | Example Value                                |
|---------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `AWS_ACCESS_KEY_ID`       | For AWS runs        | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`   | For AWS runs        | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`       | No                  | AWS Session Token for CLI, SDK, & API access. This value is for MFA credentials only. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |
| `KMS_KEY_ID`              | Encryption run only | The KMS key the client-side encryption suite uses. It is not created by the tests, since a deleted key cannot be reclaimed promptly.                                                                             | `alias/jdbc-encryption-key`                  |
| `REPEAT_TIMES`            | No                  | Overrides how many times the performance suites repeat each measurement. Leave unset for a real measurement.                                                                                                     | `1`                                          |

The region defaults to `us-east-2` and provisioned resources are named with the prefix
`test-orchestra-`. Docker-only runs need no AWS credentials at all.

### Running the Integration Tests

Each task provisions one environment, runs the in-container suite against it, and tears it down:

- `orchestra-test-docker`: databases in containers, no AWS account needed. This is the pull request gate.
- `orchestra-test-pg-aurora`: an AWS deployment - Aurora by default, and Multi-AZ or blue/green on request.
- `orchestra-test-hibernate`: Hibernate ORM's own test suite, with the wrapper as its JDBC driver.

What each task provisions and runs is chosen with `-Dorchestra-*` properties rather than by picking a
different task. The ones you are most likely to want:

| System Property                | Default          | Description                                                                                     |
|--------------------------------|------------------|-------------------------------------------------------------------------------------------------|
| `orchestra-engines`            | `postgres`       | Engines to run, comma separated: `postgres`, `mysql`. One composition per engine.                 |
| `orchestra-deployment`         | `AURORA`         | `AURORA`, `RDS_MULTI_AZ_CLUSTER` or `RDS_MULTI_AZ_INSTANCE`.                                     |
| `orchestra-instances`          | deployment default | Cluster sizes to run, comma separated.                                                          |
| `orchestra-drivers`            | every driver     | Which JDBC drivers the suite connects with: `pg`, `mysql`, `mariadb`.                             |
| `orchestra-jvms`               | one JVM          | JVMs to run the suite on, named by `TargetJvm`, for example `OPENJDK17,OPENJDK21`.                |
| `orchestra-bluegreen`          | off              | Provisions a real blue/green deployment and runs the switchover tests.                            |
| `orchestra-suite`              | the ordinary suite | Runs one suite instead: `performance`, `advanced-performance`, `autoscaling`, `encryption`, `metrics`. |
| `orchestra-caching`            | off              | Docker only. Provisions the Valkey caches and runs the caching tests instead of the rest.          |
| `orchestra-telemetry`          | on               | `none` provisions no telemetry backends.                                                          |
| `orchestra-aws-credentials-bind` | off            | Binds `~/.aws` into the test container instead of copying resolved keys. Useful for a long local run whose session token is refreshed on a timer. |

For example, to run the pull request gate against both engines:

macOS:
```bash
./gradlew --no-parallel --no-daemon orchestra-test-docker -Dorchestra-engines=pg,mysql
```

Windows:
```bash
cmd /c ./gradlew --no-parallel --no-daemon orchestra-test-docker -Dorchestra-engines=pg,mysql
```

Run one task at a time in a given checkout: every test task clears `wrapper/build/test-results`,
which a running container has bound.

Test results can be found at `wrapper/build/report/index.html`.

### Splitting a Test Run Across Several Machines (Sharding)

A full Aurora run takes hours because every test executes serially against a single cluster. The
scheduled CI workflows therefore split the work: each job pins itself to one test environment and
runs one *shard* of the integration test classes against its own database cluster.

Two system properties control this:

| System Property     | Default | Description                                                                 |
|---------------------|---------|-----------------------------------------------------------------------------|
| `test-shard-count`  | `1`     | Number of shards the integration test classes are divided into.              |
| `test-shard-index`  | `1`     | Which shard this run executes. 1-based, must be in `[1, test-shard-count]`.  |

The default of shard 1 of 1 runs every test class, so local runs and non-sharded workflows behave
exactly as before.

```bash
./gradlew --no-parallel --no-daemon orchestra-test-pg-aurora \
  -Dtest-shard-index=2 -Dtest-shard-count=4
```

Sharding only selects *test classes*. Which environment is provisioned is a separate choice, made
with the `-Dorchestra-*` properties above, so a sharded CI job usually combines both, for example
`-Dorchestra-engines=mysql -Dtest-shard-index=2 -Dtest-shard-count=4`.

To run named classes instead of a shard, use `-Dtest-classes`, which accepts a comma separated list
and simple wildcards: `-Dtest-classes=integration.container.tests.Xa*`.

Notes for maintainers:

- The shard split is computed in `wrapper/src/test/build.gradle.kts`. The list of classes is read
  from the compiled classes under `integration.container.tests`, so a newly added test class is
  automatically picked up by exactly one shard - no workflow change is needed.
- A `testClassWeightsSeconds` table in that file records the approximate cost of each class and is
  used only to keep shards evenly sized. A missing or stale entry costs some balance but can never
  drop coverage. Update it when a class's runtime changes substantially.
- Any class under `integration.container.tests` that neither ends in `Test`/`Tests` nor appears in
  `nonTestHelperClasses` fails the build rather than being silently left out of every shard.
- Running all shards of a group covers exactly the same classes as one unsharded run.

If you encounter unexplained build issues/errors, or after major project structure changes, try running the following to perform a clean build:

macOS:
```bash
./gradlew clean
```

Windows:
```bash
cmd /c ./gradlew clean
```
