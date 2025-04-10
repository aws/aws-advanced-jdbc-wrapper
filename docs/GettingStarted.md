# Getting Started

## Minimum Requirements

Before using the AWS Advanced JDBC Driver, you must install:

- Amazon Corretto 8+ or Java 8+.
- The AWS Advanced JDBC Driver.
- Your choice of underlying JDBC driver. 
  - To use the wrapper with Aurora with PostgreSQL compatibility, install the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc).
  - To use the wrapper with Aurora with MySQL compatibility, install the [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j) or [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j).

If you are using the AWS JDBC Driver as part of a Gradle project, include the wrapper and underlying driver as dependencies.  For example, to include the AWS JDBC Driver and the PostgreSQL JDBC Driver as dependencies in a Gradle project, update the ```build.gradle``` file as follows:

> **Note:** Depending on which features of the AWS JDBC Driver you use, you may have additional package requirements. Please refer to this [table](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#list-of-available-plugins) for more information.

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '2.5.6'
    implementation group: 'org.postgresql', name: 'postgresql', version: '42.5.0'
}
```

## Obtaining the AWS JDBC Driver

### Direct Download and Installation

You can use pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases) or [Maven Central](https://search.maven.org/search?q=g:software.amazon.jdbc) to install the AWS JDBC Driver. After downloading the AWS JDBC Driver, install it by including the .jar file in the application's CLASSPATH.

For example, the following command uses wget to download the wrapper:

```bash
wget https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/download/2.5.6/aws-advanced-jdbc-wrapper-2.5.6.jar
```

Then, the following command adds the AWS JDBC Driver to the CLASSPATH:

```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-advanced-jdbc-wrapper-2.5.6.jar
```

> **Note**: There is also a JAR suffixed with `-bundle-federated-auth`. It is an Uber JAR that contains the AWS JDBC Driver as well as all the dependencies needed to run the Federated Authentication Plugin. **Our general recommendation is to use the `aws-advanced-jdbc-wrapper-2.5.6.jar` for use cases unrelated to complex Federated Authentication environments**. To learn more, please check out the [Federated Authentication Plugin](./using-the-jdbc-driver/using-plugins/UsingTheFederatedAuthPlugin.md#bundled-uber-jar). 

### As a Maven Dependency

You can use [Maven's dependency management](https://search.maven.org/search?q=g:software.amazon.jdbc) to obtain the AWS JDBC Driver by adding the following configuration to the application's Project Object Model (POM) file:

```xml
<dependencies>
    <dependency>
        <groupId>software.amazon.jdbc</groupId>
        <artifactId>aws-advanced-jdbc-wrapper</artifactId>
        <version>2.5.6</version>
    </dependency>
</dependencies>
```

### As a Gradle Dependency

You can use [Gradle's dependency management](https://search.maven.org/search?q=g:software.amazon.jdbc) to obtain the AWS JDBC Driver by adding the following configuration to the application's ```build.gradle``` file:

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '2.5.6'
}
```

To add a Gradle dependency in a Kotlin syntax, use the following configuration:

```kotlin
dependencies {
    implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:2.5.6")
}
```

## Using the AWS JDBC Driver

For more detailed information about how to use and configure the AWS JDBC Driver, please visit [this page](using-the-jdbc-driver/UsingTheJdbcDriver.md).
