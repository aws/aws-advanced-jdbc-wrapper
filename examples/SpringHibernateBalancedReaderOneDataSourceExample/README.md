# Tutorial: Getting Started with the AWS JDBC Driver, Spring Boot and Hibernate for load-balanced read-write and read-only connections (Single Datasource)

In this tutorial, you will set up a Spring Boot and Hibernate application with the AWS Advanced JDBC Driver, and use a single datasource to fetch and update data from an Aurora PostgreSQL database. The datasource is configured to provide a writer connection or a reader connection to off-load a writer node from read-only queries. It provides pooled connections through the AWS Advanced JDBC Driver internal connection pool configuration.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 2.7.1
>    - Hibernate
>    - AWS JDBC Driver 2.3.2
>    - Postgresql 42.5.4
>    - Gradle 7
>    - Java 11

You will progress through the following sections:
1. Create a Gradle Spring Boot project
2. Add the required Gradle dependencies
3. Configure the AWS Advanced JDBC Driver

## Pre-requisites
- This tutorial uses the Amazon Aurora PostgreSQL database.

## Step 1: Create a Gradle Project
Create a Gradle project with the following project hierarchy:

```
└───src
    └───main
        ├───java
        │   └───example
        │       ├───data
        │       │   ├───Book.java
        │       │   ├───BookRepository.java
        │       │   └───BookService.java
        │       └───spring
        │           ├───Config.java
        │           ├───ShouldRetryTransactionException.java
        │           └───SpringHibernateBalancedReaderOneDataSourceExampleApplication.java
        └───resources
                └───application.yml
```

> Note: this sample code assumes the target database contains a table named `Book` that can be generated using the SQL queries provided in `src/main/resources/books.sql`.

## Step 2: Add the required Gradle Dependencies
In your `build.gradle.kts`, add the following dependencies.

```
dependencies {
   implementation("org.springframework.boot:spring-boot-starter-jdbc")
   implementation("org.springframework.retry:spring-retry")
   implementation("org.postgresql:postgresql")
   implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Configure Spring and Hibernate
Configure Spring to use the AWS JDBC Driver as the default datasource.

1. In the `application.yml`, add new datasources for Spring:
    ```yaml
    spring:
      datasource:
        load-balanced-writer-and-reader-datasource:
          url: jdbc:aws-wrapper:postgresql://test-cluster.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres?wrapperProfileName=F0&readerHostSelectorStrategy=roundRobin
          username: dev_user
          password: dev_password
          driver-class-name: software.amazon.jdbc.Driver
          type: org.springframework.jdbc.datasource.SimpleDriverDataSource
    ```
2. The datasource mentioned above does not use Hikari datasource that is default for Spring 2+ application. The AWS JDBC Driver manages its own internal connection pool (or several connection pools, if needed), which increases overall efficiency and helps facilitate failover support. All necessary configuration parameters are defined in the `F0` configuration profile. Other configuration presets `D`, `E` and `F` can be used as well. Any configuration profile or preset specified should use the [Read/Write Splitting Plugin](../../docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md). More details are available at [Configuration Profiles](../../docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles) and [Configuration Presets](../../docs/using-the-jdbc-driver/ConfigurationPresets.md).
   <br><br>
   Including the optional configuration parameter `readerHostSelectorStrategy` in the connection string helps to set up a strategy to select a reader node. Possible values are `random`, `roundRobin` and `leastConnections`. More details are available at [Reader Selection Strategies](../../docs/using-the-jdbc-driver/ReaderSelectionStrategies.md).


3. Configure Hibernate dialect:
   ```yaml
   jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
   ```

4. [Optional] You can enable driver logging by adding the following to `application.yml`:
   ```yaml
   logging:
      level:
        software:
          amazon:
            jdbc: INFO
            jdbc.states: INFO
        example: TRACE
   ```

For detailed logs use `TRACE` for `software.amazon.jdbc` package.

Start the application by running `./gradlew :springhibernateonedatasource:bootRun` in the terminal. You should see the application making a connection to the database and fetching data from the Example table.

# Summary
This tutorial walks through the steps required to add and configure the AWS Advanced JDBC Driver to a simple Spring Boot and Hibernate application.
