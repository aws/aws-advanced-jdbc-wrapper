# Tutorial: Getting Started with the AWS Advanced JDBC Driver, Spring Boot and Hibernate

In this tutorial, you will set up a Spring Boot and Hibernate application with the AWS Advanced JDBC Driver, and use the IAM Authentication plugin to fetch some data from an Aurora PostgreSQL database.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 2.7.1
>    - Hibernate
>    - AWS JDBC Driver 2.5.6
>    - Postgresql 42.5.4
>    - Gradle 7
>    - Java 11

You will progress through the following sections:
1. Create a Gradle Spring Boot project
2. Add the required Gradle dependencies
3. Configure the AWS Advanced JDBC Driver

## Pre-requisites
- A database with IAM authentication enabled. This tutorial uses the Aurora PostgreSQL database. For information on how to enable IAM database authentication for Aurora databases, please see the [AWS documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

## Step 1: Create a Gradle Project
Create a Gradle project with the following project hierarchy:

```
└───src
    ├───main
        ├───java
        │   └───example
        |       ├───data
        |           ├───Example.java
        |           └───ExampleRepository.java
        │       └───SpringHibernateExampleApplication.java
        └───resources
                └───application.yml
```

> Note: this sample code assumes the target database contains a table named Example that can be generated using the SQL queries provided in `src/main/resources/example.sql`.

`SpringHibernateExampleApplication.java` contains the following the code:

```java
@SpringBootApplication
public class SpringHibernateExampleApplication implements CommandLineRunner {
  private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

  @Autowired
  ExampleRepository repository;

  @Override
  public void run(String... args) {
    LOGGER.info("Example -> {}", repository.findAll());
  }

  public static void main(String[] args) {
    SpringApplication.run(SpringHibernateExampleApplication.class, args);
  }
}
```

`Example.java` contains the following code:

```java
package example.data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class Example {

   @Id
   @GeneratedValue
   private int id;

   private int status;

   public Example() {
      super();
   }

   public Example(int id, int status) {
      super();
      this.id = id;
      this.status = status;
   }

   public Example(int status) {
      super();
      this.status = status;
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
Lastly, `ExampleRepository.java` contains the following code:

```java
package example.data;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ExampleRepository extends JpaRepository<Example, Integer> {

}
```

## Step 2: Add the required Gradle Dependencies
In your `build.gradle.kts`, add the following dependencies.

```
dependencies {
   implementation("org.springframework.boot:spring-boot-starter-jdbc")
   implementation("org.springframework.boot:spring-boot-starter-web")
   implementation("org.postgresql:postgresql")
   implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Configure Spring and Hibernate
Configure Spring to use the AWS Advanced JDBC Driver as the default datasource.

1. In the `application.yml`, add a new datasource for Spring:

```yaml
spring:
  datasource:
    url: jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/db
    username: jane_doe
    driver-class-name: software.amazon.jdbc.Driver
    hikari:
      data-source-properties:
        wrapperPlugins: iam,failover,efm2
        iamRegion: us-east-2
        iamExpiration: 1320
      exception-override-class-name: software.amazon.jdbc.util.HikariCPSQLException
      max-lifetime: 1260000
```
   Since Spring 2+ uses Hikari to manage datasources, to configure the driver we would need to specify the `data-source-properties` under `hikari`.
   Whenever Hikari is used, we also need to ensure failover exceptions are handled correctly so connections will not be discarded from the pool after failover has occurred. This can be done by overriding the exception handling class. For more information on this, please see [the documentation on HikariCP](../../docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#hikaricp).
   
   This example contains some very simple configurations for the IAM Authentication plugin, if you are interested in other configurations related to failover, please visit [the documentation for failover parameters](../../docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#failover-parameters)
2. Configure Hibernate dialect:

```yaml
spring:
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```
3. [Optional] You can enable driver logging by adding the following to `application.yml`:

```yaml
logging:
  level:
    software.amazon.jdbc: TRACE
```

Start the application by running `./gradlew :springhibernate:bootRun` in the terminal. You should see the application making a connection to the database and fetching data from the Example table.

# Summary
This tutorial walks through the steps required to add and configure the AWS Advanced JDBC Driver to a simple Spring Boot and Hibernate application.
