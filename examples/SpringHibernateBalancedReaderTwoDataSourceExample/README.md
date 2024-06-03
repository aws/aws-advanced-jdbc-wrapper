# Tutorial: Getting Started with the AWS Advanced JDBC Driver, Spring Boot and Hibernate for load-balanced write and read-only connections (Two Datasources)

In this tutorial, you will set up a Spring Boot and Hibernate application with the AWS Advanced JDBC Driver, and use two datasources to fetch and update data from an Aurora PostgreSQL database. One datasource is configured to provide a writer connection. The other datasource is configured to provide a reader connection to off load a writer node from read-only queries. Both datasources provide pooled connections through AWS Advanced JDBC Driver internal connection pool configuration.

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
- This tutorial uses the Aurora PostgreSQL database. 

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
        │           ├───LoadBalancedReaderDataSourceContext.java
        │           ├───RoutingDataSource.java
        │           ├───ShouldRetryTransactionException.java
        │           ├───SpringHibernateBalancedReaderTwoDataSourceExampleApplication.java
        │           ├───WithLoadBalancedDataSourceInterception.java
        │           └───WithLoadBalancedReaderDataSource.java
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
        writer-datasource:
          url: jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/test_db?wrapperProfileName=SF_F0
          username: dev_user
          password: dev_password
          driver-class-name: software.amazon.jdbc.Driver
          type: org.springframework.jdbc.datasource.SimpleDriverDataSource
        load-balanced-reader-datasource:
          url: jdbc:aws-wrapper:postgresql://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:5432/test_db?wrapperProfileName=SF_F0&readerInitialConnectionHostSelectorStrategy=roundRobin
          username: dev_user
          password: dev_password
          driver-class-name: software.amazon.jdbc.Driver
          type: org.springframework.jdbc.datasource.SimpleDriverDataSource
    ```
2. The datasources mentioned above do not use Hikari datasources which are the default for Spring 2+ applications. The AWS JDBC Driver manages its own internal connection pool (or several connection pools, if needed), which increases overall efficiency and helps facilitate failover support. All necessary configuration parameters are defined in `SF_F0` configuration profile. Other configuration presets from `SF_` family can be used as well. More details are available at [Configuration Profiles](../../docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles) and [Configuration Presets](../../docs/using-the-jdbc-driver/ConfigurationPresets.md).
 <br><br>
   Optional configuration parameter `readerInitialConnectionHostSelectorStrategy` in connection string helps to setup a strategy selecting a reader node. Possible values are `random`, `roundRobin` and `leastConnections`. More details are available at [Reader Selection Strategies](../../docs/using-the-jdbc-driver/ReaderSelectionStrategies.md).


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

Start the application by running `./gradlew :springhibernatetwodatasource:bootRun` in the terminal. You should see the application making a connection to the database and fetching data from the `Book` table.

# Summary
This tutorial walks through the steps required to add and configure the AWS JDBC Driver to a simple Spring Boot and Hibernate application that load balances connections.
