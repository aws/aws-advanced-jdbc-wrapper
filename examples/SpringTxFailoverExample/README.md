# Tutorial: Getting started with Spring Boot and Failover

In this tutorial, you will set up a Spring Boot application using the AWS JDBC Driver. This sample application will contain an example of how to retry transactions interrupted by failover. This tutorial is an extension of the [Spring Boot HikariCP example](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/examples/SpringBootHikariExample/README.md) and will contain similar elements.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 2.7.0
>    - AWS JDBC Driver 2.5.6
>    - Postgresql 42.5.4
>    - Java 8

## Step 1: Create a Gradle Project

Create a Gradle Project with the following project hierarchy:
```
├───src
│   └───main
│       ├───java
│       │   └───software
│       │       └───amazon
│       │           ├───ApiController.java
│       │           ├───Example.java
│       │           ├───ExampleConfiguration.java
│       │           ├───ExampleDao.java
│       │           ├───ExampleDaoImpl.java
│       │           ├───ExampleService.java
│       │           └───SpringTxFailoverExampleApplication.java
│       └───resources
│           └───application.yml
└───────build.gradle.kts
```

When creating the `SpringTxFailoverExampleApplication.java` class, add the following code to it.

```java
package example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringTxFailoverExampleApplication {
  public static void main(String[] args) {
    SpringApplication.run(SpringTxFailoverExampleApplication.class, args);
  }
}
```

This tutorial requires an `EXAMPLE` table with two integer fields: `ID` and `STATUS`. The `Example.java` file contains a representation of an "Example" object. It should contain the following code:
```java
package example;

public class Example {

  private int id;

  private int status;

  public Example() {
    super();
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int name) {
    this.status = name;
  }

  @Override
  public String toString() {
    return String.format("Example [id=%s, status=%s]", id, status);
  }
}
```

You may also use the Spring Initializr to create the boilerplate code:
1. Go to https://start.spring.io/
2. Select the Maven project and version 2.7.9 of the Spring Boot.
3. Select Java version 8.
4. Click Dependencies and select the following:
    - Spring Web
    - Spring Data JDBC
    - PostgreSQL Driver

## Step 2: Add the required Gradle Dependencies

In the `build.gradle.kts` file, add the following dependencies.

```kotlin
dependencies {
	implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.retry:spring-retry:1.3.4")
	implementation("org.springframework:spring-aspects:5.3.29")
	implementation("org.postgresql:postgresql:42.5.4")
	implementation("software.amazon.awssdk:rds:2.29.23")
	implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Configure the Datasource

In the `application.yml` file, configure Hikari and AWS JDBC Driver as its driver.

Note that in Spring Boot 2 and 3, Hikari is the default DataSource implementation. So, a bean explicitly specifying Hikari as a Datasource is not needed.

```yaml
spring:
  datasource:
    url: jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/db
    username: jane_doe
    password: password
    driver-class-name: software.amazon.jdbc.Driver
    hikari:
      exception-override-class-name: software.amazon.jdbc.util.HikariCPSQLException
      max-lifetime: 1260000
      auto-commit: false
      maximum-pool-size: 3
      data-source-properties:
        keepSessionStateOnFailover: true
```

Please also note the use of the [`keepSessionStateOnFailover`](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#failover-parameters) property. When failover occurs, the connection's auto commit value is reset to true. When the auto commit value is set to false or transactions are used, further operations such as a rollback or commit on the same connection will cause errors. This parameter is used when connections cannot be reconfigured manually as seen in this [example](https://github.com/awslabs/aws-advanced-jdbc-wrapper/tree/main/examples/AWSDriverExample/src/main/java/software/amazon/PgFailoverSample.java).

## Step 4: Set up a data access object

Set up a simple data access object (DAO) interface and implementation. The data access object will be responsible for executing any queries. In this tutorial, only a get method will be included, but other methods are available within the sample code. 

The DAO interface:
```java
package example;

import java.util.List;
import java.util.Map;

public interface ExampleDao {
  List<Map<String, Object>> getAll();
}
```

The DAO implementation:
```java
package example;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class ExampleDaoImpl implements ExampleDao {
  @Autowired
  private DataSource dataSource;

  @Override
  public List<Map<String, Object>> getAll() {
    final String sql = "SELECT * FROM EXAMPLE";
    final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForList(sql);
  }
}
```

## Step 5: Set up a configuration class

The `ExampleConfiguration.java` file will contain a bean for the transaction manager. The autowired datasource will be configured based on the `application.yml` file contents.

```java
package example;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
public class ExampleConfiguration {

  @Autowired
  private DataSource dataSource;

  @Bean
  public DataSourceTransactionManager getDataSourceTransactionManager() {
    return new DataSourceTransactionManager(dataSource);
  }
}
```

## Step 6: Set up a service

Set up a service class, which will contain an autowired `exampleDao`.

```java
package example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ExampleService {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  private ExampleDao exampleDao;

  public List<Example> get() {
    logger.info("Retry Number : {}", RetrySynchronizationManager.getContext().getRetryCount());
    List<Map<String, Object>> rows = exampleDao.getAll();
    List<Example> examples = new ArrayList<>();
    for (Map row : rows) {
      Example obj = new Example();
      obj.setId(((Integer) row.get("ID")));
      obj.setStatus((Integer) row.get("STATUS"));
      examples.add(obj);
    }
    return examples;
  }
}
```

## Step 7: Set up a controller

Create a new `ApiController` class:

```java
package example;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;

@RestController
public class ApiController {

  @Autowired
  private ExampleService exampleService;

  @GetMapping(value = "/get")
  @Retryable(value = {FailoverSQLException.class}, maxAttempts = 3, backoff = @Backoff(delay = 5000))
  public List<Example> get() {
    return exampleService.get();
  }
}
```

The `@RestController` annotation on the class will allow methods in it to use annotations for mapping HTTP requests.
In this example, the `get()` method is annotated with `@GetMapping(value = "/get")` which will route requests with the path `/get` to that method.
Within the `get()` method, the service is called to perform other operations and return its results.

The `@EnableRetry` and `@Retryable` annotations allow methods to be retried based on the given value. In the sample above, `value = {FailoverSQLException.class}` indicates that all methods will be retried if a `FailoverSQLException` is thrown.

## Step 8: Run and call the application

Start the application by running `./gradlew :springtxfailover:bootRun` in the terminal.

Create an HTTP request to the application by running the following terminal command `curl localhost:8080/get`.
This will trigger the query statement `SELECT * FROM EXAMPLE;` and return the results.
