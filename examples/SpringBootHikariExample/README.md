# Tutorial: Getting started with Spring Boot, Hikari, and the AWS JDBC Driver

In this tutorial, you will set up a Spring Boot application using Hikari and the AWS JDBC Driver.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 2.7.0
>    - AWS JDBC Driver 2.3.5
>    - Postgresql 42.5.4
>    - Java 8


## Step 1: Create a Gradle Project
Create a Gradle Project with the following project hierarchy:
```
├───gradle
│   └───wrapper
│       ├───gradle-wrapper.jar
│       └───gradle-wrapper.properties   
├───build.gradle.kts
├───gradlew
└───src
    └───main
        ├───java
        │   └───software
        │       └───amazon
        │           └───SpringBootHikariExampleApplication.java
        └───resources
            └───application.yml
```
When creating the `SpringBootHikariExampleApplication.java` class, add the following code to it.

```java
package software.amazon.SpringBootHikariExample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootHikariExampleApplication {
  public static void main(String[] args) {
    SpringApplication.run(SpringBootHikariExampleApplication.class, args);
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
	implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
	implementation("org.postgresql:postgresql:42.5.4")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Configure the Datasource

In the `application.yml` file, configure Hikari and AWS JDBC Driver as its driver.

```yaml
spring:
  datasource:
    url: jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/database-name
    username: some_username
    password: some_password
    driver-class-name: software.amazon.jdbc.Driver
    hikari:
      data-source-properties:
        wrapperPlugins: failover,efm2
        wrapperDialect: aurora-pg
      exception-override-class-name: software.amazon.jdbc.util.HikariCPSQLException
```
Note that in Spring Boot 2 and 3, Hikari is the default DataSource implementation. So, a bean explicitly specifying Hikari as a Datasource is not needed.

Optionally, you may like to add in Hikari specific configurations like the following.
```yaml
spring:
  datasource:
    url: jdbc:aws-wrapper:postgresql://database-endpoint-url:5432/database-name
    username: some_username
    password: some_password
    driver-class-name: software.amazon.jdbc.Driver
    hikari:
      data-source-properties:
        wrapperPlugins: failover,efm2
        wrapperDialect: aurora-pg
      exception-override-class-name: software.amazon.jdbc.util.HikariCPSQLException
      max-lifetime: 840000
      minimum-idle: 10
      maximum-pool-size: 20
      idle-timeout: 900000
      read-only: true
```


## Step 4: Use JDBC Template 

Create a new `ApiController` class like the following:

```java
package software.amazon.SpringBootHikariExample;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiController {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @GetMapping(value = "/select1")
  public Integer getOne() {
    return jdbcTemplate.queryForObject("SELECT 1;", Integer.class);
  }
}
```

The `@RestController` annotation on the class will allow methods in it to use annotations for mapping HTTP requests. 
In this example, the `getOne()` method is annotated with `@GetMapping(value = "/select1")` which will route requests with the path `/select` to that method.
Within the `getOne()` method, the `JdbcTemplate` is called to execute the query `SELECT 1;` and return its results.

## Step 5: Run and call the application

Start the application by running `./gradlew run` in the terminal. 

Create an HTTP request to the application by running the following terminal command `curl http://localhost:8080/select1`.
This will trigger the query statement `SELECT 1;` and return the results.  
