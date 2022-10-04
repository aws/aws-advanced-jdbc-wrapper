# Development Guide

### Setup
Make sure you have Amazon Corretto 8+ or Java 8+ installed.

Clone the JDBC Wrapper repository:

```bash
git clone https://github.com/awslabs/aws-advanced-jdbc-wrapper.git
```

You can now make changes in the repository.

### Building the AWS Advanced JDBC Wrapper
Navigate to project root:
```bash
cd aws-advanced-jdbc-wrapper
```
To build the AWS Advanced JDBC Wrapper without running the tests:
Mac:
```bash
./gradlew build -x test
```

Windows:
```bash
gradlew build -x test
```

Mac:
```bash
./gradlew build
```

Windows:
```bash
gradlew build
```

## Running the Tests
After building the JDBC Wrapper you can now run the unit tests.
This will also validate your environment is set up correctly.

Mac:
```bash
./gradlew test
```

Windows:
```bash
./gradlew test
```

For running driver-specific tests see these links: <br />

[PostgreSQL](/docs/driver-specific/postgresql/postgresql.md)

###### Sample Code
[Connection Test Sample Code](/docs/driver-specific/postgresql/ConnectionTestSample.java)

## Architecture
For more information on how the AWS Advanced JDBC Wrapper functions and how it is structured, please visit [Architecture](./Architecture.md).
