# Getting Started

## Minimum Requirements

Before using the AWS Advanced JDBC Wrapper, you must install:

- Amazon Corretto 8+ or Java 8+.
- The AWS Advanced JDBC Wrapper.
- Your choice of underlying JDBC driver. 
  - To use the wrapper with Aurora with PostgreSQL compatibility, install the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc).
  - To use the wrapper with Aurora with MySQL compatibility, install the [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j) or [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j). See [here](../README.md#mariadb) for some limitations and caveats with the MariaDB driver.

If you are using the AWS Advanced JDBC Wrapper as part of a Gradle project, include the wrapper and underlying driver as dependencies.  For example, to include the AWS Advanced JDBC Wrapper and the PostgreSQL JDBC Driver as dependencies in a Gradle project, update the ```build.gradle``` file as follows:

> **Note:** Depending on which features of the AWS Advanced JDBC Wrapper you use, you may have additional package requirements. Please refer to this [table](https://github.com/aws/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#list-of-available-plugins) for more information.

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '4.2.0'
    implementation group: 'org.postgresql', name: 'postgresql', version: '42.7.8'
}
```

## Obtaining the AWS Advanced JDBC Wrapper

### Direct Download and Installation

You can use pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/aws/aws-advanced-jdbc-wrapper/releases) or [Maven Central](https://central.sonatype.com/artifact/software.amazon.jdbc/aws-advanced-jdbc-wrapper) to install the AWS Advanced JDBC Wrapper. After downloading the AWS Advanced JDBC Wrapper, install it by including the .jar file in the application's CLASSPATH.

For example, the following command uses wget to download the wrapper:

```bash
wget https://github.com/aws/aws-advanced-jdbc-wrapper/releases/download/4.2.0/aws-advanced-jdbc-wrapper-4.2.0.jar
```

Then, the following command adds the AWS Advanced JDBC Wrapper to the CLASSPATH:

```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-advanced-jdbc-wrapper-4.2.0.jar
```

> **Note**: There is also a JAR suffixed with `-bundle-federated-auth`. It is an Uber JAR that contains the AWS Advanced JDBC Wrapper as well as all the dependencies needed to run the Federated Authentication Plugin. **Our general recommendation is to use the `aws-advanced-jdbc-wrapper-4.2.0.jar` for use cases unrelated to complex Federated Authentication environments**. To learn more, please check out the [Federated Authentication Plugin](./using-the-jdbc-driver/using-plugins/UsingTheFederatedAuthPlugin.md#bundled-uber-jar). 

### As a Maven Dependency

You can use [Maven's dependency management](https://central.sonatype.com/artifact/software.amazon.jdbc/aws-advanced-jdbc-wrapper) to obtain the AWS Advanced JDBC Wrapper by adding the following configuration to the application's Project Object Model (POM) file:

```xml
<dependencies>
    <dependency>
        <groupId>software.amazon.jdbc</groupId>
        <artifactId>aws-advanced-jdbc-wrapper</artifactId>
        <version>4.2.0</version>
    </dependency>
</dependencies>
```

### As a Gradle Dependency

You can use [Gradle's dependency management](https://central.sonatype.com/artifact/software.amazon.jdbc/aws-advanced-jdbc-wrapper) to obtain the AWS Advanced JDBC Wrapper by adding the following configuration to the application's ```build.gradle``` file:

```gradle
dependencies {
    implementation group: 'software.amazon.jdbc', name: 'aws-advanced-jdbc-wrapper', version: '4.2.0'
}
```

To add a Gradle dependency in a Kotlin syntax, use the following configuration:

```kotlin
dependencies {
    implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:4.2.0")
}
```

## Using the AWS Advanced JDBC Wrapper

For more detailed information about how to use and configure the AWS Advanced JDBC Wrapper, please visit [this page](using-the-jdbc-driver/UsingTheJdbcDriver.md).

## Configuring the Driver with AI Tools

If you use an AI coding assistant (Cursor, Claude, ChatGPT, Amazon Bedrock, GitHub Copilot, etc.), you can use the driver's configuration assistant skill to help build, review, or troubleshoot your wrapper configuration.

The skill is a single self-contained markdown file: [`JDBC-WRAPPER-CONFIGURATION-ASSISTANT.md`](./JDBC-WRAPPER-CONFIGURATION-ASSISTANT.md). It carries enough reference material — every plugin, parameter, default value, dialect, failover mode, and compatibility constraint — for the AI to answer arbitrary configuration questions without needing access to the wrapper source.

### Quick start

1. Download or copy [`JDBC-WRAPPER-CONFIGURATION-ASSISTANT.md`](./JDBC-WRAPPER-CONFIGURATION-ASSISTANT.md).
2. Add it to your AI tool as a knowledge file, project rule, or system prompt:
   - **Kiro**: place it in `.kiro/skills/` in your project workspace, or `~/.kiro/skills/` for user-level scope across every workspace. Kiro detects skills automatically.
   - **Cursor**: place it in `.cursor/rules/` of your project.
   - **Claude Projects**: upload it as a project knowledge document.
   - **ChatGPT custom GPT**: paste into the GPT's instructions or upload as a knowledge file.
   - **Amazon Bedrock agent**: attach it to the agent's knowledge base.
   - **Plain chat (ChatGPT, Claude, Gemini)**: paste it at the start of a new conversation, then ask your question.
3. Ask the AI for help. It will interview you when it needs more details.

### Example prompts

The simplest way to start:

```
Help me configure the AWS Advanced JDBC Wrapper for my application.
```

Skip the interview when you already know what you want:

```
Give me a default configuration for Aurora MySQL with Spring Boot, HikariCP, and failover support.
```

Review an existing configuration:

```
Review this wrapper configuration and tell me what to change:

<paste your application.yml or Properties block here>
```

Diagnose an error:

```
My pool drains to zero connections right after Aurora failover. I'm using HikariCP
and the failover2 plugin. What's wrong?
```

The skill targets wrapper version 4.0+ and explicitly avoids guessing — if it doesn't know something, it will say so and point you at the official docs.
