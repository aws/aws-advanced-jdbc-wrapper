# Getting Started

## Minimum Requirements

Before using the AWS Advanced JDBC Driver, you must install:

- Amazon Corretto 8+ or Java 8+.
- The AWS Advanced JDBC Driver.
- Your choice of underlying JDBC driver. To use the wrapper with Aurora with PostgreSQL compatibility, you need to install the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc).

If you are using the AWS JDBC Driver as part of a Gradle project, include the wrapper and underlying driver as dependencies.  For example, to include the AWS Advanced JDBC Driver and the PostgreSQL JDBC Driver as dependencies in a Gradle project, update the ```build.gradle``` file as follows:

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '1.0.0'
    implementation group: 'org.postgresql', name: 'postgresql', version: '42.5.0'
}
```

## Obtaining the AWS Advanced JDBC Driver

### Direct Download and Installation

You can use pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases) or [Maven Central](https://search.maven.org/search?q=g:software.amazon.jdbc) to install the AWS Advanced JDBC Driver. After downloading the AWS JDBC Driver, install it by including the .jar file in the application's CLASSPATH.

For example, the following command uses wget to download the wrapper:

```bash
wget https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/download/1.0.0/aws-advanced-jdbc-wrapper-1.0.0.jar
```

Then, the following command adds the AWS JDBC Driver to the CLASSPATH:

```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-advanced-jdbc-wrapper-1.0.0.jar
```

### As a Maven Dependency

You can use [Maven's dependency management](https://search.maven.org/search?q=g:software.amazon.jdbc) to obtain the AWS JDBC Driver by adding the following configuration to the application's Project Object Model (POM) file:

```xml
<dependencies>
    <dependency>
        <groupId>software.amazon.jdbc</groupId>
        <artifactId>aws-advanced-jdbc-wrapper</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

### As a Gradle Dependency

You can use [Gradle's dependency management](https://search.maven.org/search?q=g:software.amazon.jdbc) to obtain the AWS JDBC Driver by adding the following configuration to the application's ```build.gradle``` file:

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '1.0.0'
}
```

To add a Gradle dependency in a Kotlin syntax, use the following configuration:

```kotlin
dependencies {
    implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:1.0.0")
}
```

## Using the AWS Advanced JDBC Driver

For more detailed information about how to use and configure the AWS JDBC Driver, please visit [this page](using-the-jdbc-driver/UsingTheJdbcDriver.md).
