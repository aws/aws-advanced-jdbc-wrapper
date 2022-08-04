# Getting Started

## Minimum Requirements
You need to install Amazon Corretto 8+ or Java 8+ before using the AWS Advanced JDBC Wrapper.

Please note that in addition to obtaining the AWS Advanced JDBC Wrapper, you will need to include a dependency on the underlying driver of your choice to your project.

To use the AWS Advanced JDBC Wrapper for Aurora with PostgreSQL compatibility, you will need to install the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc).

For example, to use the PostgreSQL JDBC Driver as the underlying driver in a Gradle project, the ```build.gradle``` file would include the following.
```gradle
dependencies {
    implementation group: 'com.amazon.awslabs.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '1.0.0'
    implementation group: 'org.postgresql', name: 'postgresql', version: '42.4.0'
}
```

## Obtaining the AWS Advanced JDBC Wrapper

### Direct Download
The AWS Advanced JDBC Wrapper can be installed from pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases) or [Maven Central](https://search.maven.org/search?q=g:com.amazon.awslabs.jdbc). To install the AWS Advanced JDBC Wrapper, obtain the corresponding JAR file and include it in the application's CLASSPATH.

**Example - Direct Download via wget**

```bash
wget https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/download/1.0.0/aws-advanced-jdbc-wrapper-1.0.0.jar
```

**Example - Adding the JDBC Wrapper to the CLASSPATH**
```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-advanced-jdbc-wrapper-1.0.0.jar
```

### As a Maven Dependency
You can use [Maven's dependency management](https://search.maven.org/search?q=g:com.amazon.awslabs.jdbc) to obtain the JDBC Wrapper by adding the following configuration in the application's Project Object Model (POM) file:

**Example - Maven**
```xml
<dependencies>
    <dependency>
        <groupId>com.amazon.awslabs.jdbc</groupId>
        <artifactId>aws-advanced-jdbc-wrapper</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```
### As a Gradle Dependency
You can use [Gradle's dependency management](https://search.maven.org/search?q=g:com.amazon.awslabs.jdbc) to obtain the JDBC Wrapper by adding the following configuration in the application's ```build.gradle``` file:

**Example - Gradle**

```gradle
dependencies {
    implementation group: 'com.amazon.awslabs.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '1.0.0'
}
```

```kotlin
dependencies {
    implementation("com.amazon.awslabs.jdbc:aws-advanced-jdbc-wrapper:1.0.0")
}
```

## Using the AWS Advanced JDBC Wrapper
For more detailed information on how to use and configure the JDBC Wrapper, please visit [this page](./using-the-jdbc-wrapper/UsingTheJdbcWrapper.md).
