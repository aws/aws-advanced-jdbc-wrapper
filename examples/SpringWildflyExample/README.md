# Tutorial: Getting Started with the AWS JDBC Driver, Spring Boot and Wildfly

In this tutorial, you will set up a Wildfly and Spring Boot application with the AWS Advanced JDBC Driver, and use the wrapper to execute some simple database operations.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 2.7.1
>    - Wildfly 26.1.1 Final
>    - AWS JDBC Driver 2.5.6
>    - Postgresql 42.5.4
>    - Gradle 7
>    - Java 11

You will progress through the following sections:
1. Create a Gradle Spring Boot project
2. Add the required Gradle dependencies
3. Configure the AWS Advanced JDBC Wrapper in Wildfly
4. Use JDBCTemplate to perform some simple database operations

## Step 1: Create a Gradle Project
Create a Gradle project with the following project hierarchy:

```
├───build.gradle.kts
├───spring
      └───src
          ├───main
              ├───java
              │   └───example
              |       │───Example.java
              |       └───SpringWildflyExampleApplication.java
              └───resources
              │   └───application.properties
└───wildfly
      └───modules
          ├───software
              ├───amazon
              │   └───jdbc
              │       └───main
              │       │   │───module.xml
              │       │   │───postgresql-42.5.4.jar
              │       │   └───aws-advanced-jdbc-wrapper-2.5.4.jar
      └───standalone
          ├───configuration
              ├───amazon
              │   └───standalone.xml
```
> Note: The wildfly directory will contain all the files that you will download in step 3. For simplicity, the diagram above only shows the files that either need to be added or require modifications.

The file `Example.java` contains the following:
```java
package example;

public class Example {

  int status;
  int id;

  public Example(int status, int id) {
    this.status = status;
    this.id = id;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "Example{" +
        "status=" + status +
        ", id='" + id +
        '}';
  }
}
```

## Step 2: Add the required Gradle Dependencies
In your `build.gradle.kts`, add the following dependencies.

```
dependencies {
   implementation("org.springframework.boot:spring-boot-starter-jdbc")
   implementation("org.springframework.boot:spring-boot-starter-web")
   runtimeOnly("org.springframework.boot:spring-boot-devtools")
   implementation("org.postgresql:postgresql")
   implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")
}
```

Please note that the sample code inside the AWS JDBC Driver project will use the dependency `implementation(project(":aws-advanced-jdbc-wrapper"))` instead of `implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:latest")` as seen above.

## Step 3: Configure Wildfly
> Note: for simplicity, this repository does not contain the entire wildfly application, and instead only contains the modified files.

Download the Wildfly 26.1.1 Servlet-Only Distribution from the [Wildfly website](https://www.wildfly.org/downloads/).
In the Wildfly `standalone/configuration/standalone.xml` file, configure the AWS Advanced JDBC Driver as your datasource by adding the following to the `<datasources>` section.

```xml
<datasource jndi-name="java:jboss/datasources/AWSWrapper" pool-name="AWSWrapper" enabled="true" use-java-context="true" statistics-enabled="${wildfly.datasources.statistics-enabled:${wildfly.statistics-enabled:false}}">
   <connection-url>jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/db</connection-url>
   <driver>wrapper</driver>
   <security>
      <user-name>foo</user-name>
      <password>bar</password>
   </security>
</datasource>
```

If you would like to configure any additional properties for the wrapper, such as the [failover timeouts](../../docs/using-the-jdbc-driver/FailoverConfigurationGuide.md), you can do so through `<connection-property>`:
```xml
<connection-property name="failoverTimeoutMs">180000</connection-property>
<connection-property name="failoverWriterReconnectIntervalMs">2000</connection-property>
<connection-property name="failoverReaderConnectTimeoutMs">30000</connection-property>
<connection-property name="failoverClusterTopologyRefreshRateMs">2000</connection-property>
```

You also need to add a new module in the `modules` directory.
To add a new module, you need to add a `module.xml` and provide the required driver JAR files.
The folder containing the `module.xml` needs to match the module name, in this example, the module name is `software.amazon.jdbc`.
Since this example uses the PostgreSQL JDBC driver as the target driver, you need to add the AWS Advanced JDBC Driver JAR file as well as the PostgreSQL JDBC driver JAR file in the same directory as the `module.xml`.

```xml
<module xmlns="urn:jboss:module:1.1" name="software.amazon.jdbc">

  <resources>
    <resource-root path="aws-advanced-jdbc-wrapper-2.5.4.jar"/>
    <resource-root path="postgresql-42.5.4.jar"/>
  </resources>
</module>
```

## Step 4: Configure Spring to use the AWS Advanced JDBC Driver
To configure Spring to use the datasource specified in Wildfly, add an `application.properties` file in `spring/main/resources` with the `jndi-name` property:
```properties
spring.datasource.jndi-name=java:jboss/datasources/AWSWrapper
```
The `jndi-name` needs to match the JNDI name specified in the Wildfly `standalone.xml` file.

## Step 4: Use JDBCTemplate to perform some simple operations

In `spring/src/main/java/example/SpringWildflyExampleApplication.java`, use `JdbcTemplate` to query data from the database.

> Note: this sample code assumes the target database contains a table named Example that can be generated using the SQL queries provided in `src/main/resources/example.sql`.

```java
package example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class SpringWildflyExampleApplication implements CommandLineRunner {

   private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

   @Autowired
   JdbcTemplate jdbcTemplate;

   @Override
   public void run(String... args) {
      LOGGER.info("Example -> {}", jdbcTemplate.query(
              "SELECT * FROM example LIMIT 10",
              (rs, rowNum) ->
                      new Example(
                              rs.getInt("status"),
                              rs.getInt("id")
                      )
      ));
   }

   public static void main(String[] args) {
      SpringApplication.run(SpringWildflyExampleApplication.class, args);
   }
}
```

Start the application by 
1. Starting the Wildfly server with `./wildfly/bin/standalone.sh`, and
2. Running `./gradlew :springhibernate:bootRun` in the terminal.

3. You should see the Spring application making a connection to the database and fetching data from the Example table.

# Summary
This tutorial walks through the steps required to add and configure the AWS Advanced JDBC Driver to a simple Spring Boot and Wildfly application.
