# Using the AWS Advanced JDBC Driver
The AWS Advanced JDBC Driver leverages community JDBC drivers and enables support of AWS and Aurora functionalities. Currently, the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc), [MySQL JDBC Driver](https://github.com/mysql/mysql-connector-j), and [MariaDB JDBC Driver](https://github.com/mariadb-corporation/mariadb-connector-j) are supported.
The JDBC Wrapper also supports [connection pooling](./DataSource.md#Using-the-AwsWrapperDataSource-with-Connection-Pooling-Frameworks).

## Using the AWS JDBC Driver with plain RDS databases
It is possible to use the AWS JDBC Driver with plain RDS databases, but individual features may or may not be compatible. For example, failover handling and enhanced failure monitoring are not compatible with plain RDS databases and the relevant plugins must be disabled. Plugins can be enabled or disabled as seen in the [Connection Plugin Manager Parameters](#connection-plugin-manager-parameters) section. Please note that some plugins have been enabled by default. Plugin compatibility can be verified in the [plugins table](#list-of-available-plugins).

## Wrapper Protocol
The AWS JDBC Driver uses the protocol prefix `jdbc:aws-wrapper:`. Internally, the JDBC Wrapper will replace this protocol prefix with `jdbc:`, making the final protocol `jdbc:aws-wrapper:{suffix}` where `suffix` is specific to the desired underlying protocol. For example, to connect to a PostgreSQL database, you would use the protocol `jdbc:aws-wrapper:postgresql:`, and inside the AWS JDBC Driver, the final protocol that will be used to connect to a database will be `jdbc:postgresql:`.

## Getting a Connection
To get a connection from the AWS JDBC Driver, the user application can either connect with a DriverManager or with a DataSource.

The process of getting a connection with a DriverManager will remain the same as with other JDBC Drivers; [this example](./../../examples/AWSDriverExample/src/main/java/software/amazon/PgConnectionSample.java) demonstrates establishing a connection with the PostgreSQL JDBC driver. Note that when connection properties are configured in both the connection string and with a Properties object, the connection string values will take precedence.

Establishing a connection with a DataSource may require some additional steps.
For detailed information and examples, review the [Datasource](./DataSource.md) documentation.

### Connections with Different Query Lengths
It is recommended that user applications use different settings for connections or connection pools that execute queries of varying lengths. Long and short running queries have different preferred settings. For example, if the network timeout is set to 1 minute, given an application that executes both short running (~5 seconds) and long running (~1 minute) queries, the user would be waiting a large amount of time for the short running queries to time out. Alternatively, if the timeout is set to 5 seconds, the user would experience large numbers of false negatives in which the long queries are consistently failing due to the timeout. 
<br>
**Note:** User applications with queries of varying lengths should also take into consideration any plugin configuration parameters that could be utilized to facilitate the needs of the application.

## Integration with 3rd Party Frameworks
The JDBC Wrapper can be used with different frameworks and tools. More details for some frameworks can be found [here](./Frameworks.md).

## Logging
The AWS JDBC Driver uses the Java Util Logger built-in library functionality to log information. To enable logging and see information logged by the AWS JDBC Driver:

1. Create a `.properties` file and configure the logging level.
2. Specify the `.properties` file with the `java.util.logging.config.file` option; for example, 
   `-Djava.util.logging.config.file=absolute\path\to\logging.properties`.

An example  `.properties` file is as follows:

```properties
# Possible values for log level (from most detailed to less detailed): FINEST, FINER, FINE, CONFIG, INFO, WARNING, SEVERE
.level=INFO
handlers=java.util.logging.ConsoleHandler
java.util.logging.ConsoleHandler.level=ALL
software.amazon.jdbc.Driver.level=FINER
software.amazon.jdbc.plugin.level=FINER
```

The AWS JDBC Driver also has a parameter, [`wrapperLoggerLevel`](#aws-advanced-jdbc-driver-parameters), to configure the logging level.

## AWS Advanced JDBC Driver Parameters
These parameters are applicable to any instance of the AWS JDBC Driver.

| Parameter                       | Value     | Required | Description                                                                                                                                                                          | Default Value |
|---------------------------------|-----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `wrapperLogUnclosedConnections` | `Boolean` | No       | Allows the AWS JDBC Driver to track a point in the code where connection has been opened but not closed.                                                                             | `false`       |
| `wrapperLoggerLevel`            | `String`  | No       | Logger level of the AWS JDBC Driver. <br><br/>If it is used, it must be one of the following values: `OFF`, `SEVERE`, `WARNING`, `INFO`, `CONFIG`, `FINE`, `FINER`, `FINEST`, `ALL`. | `null`        |
| `database`                      | `String`  | No       | Database name.                                                                                                                                                                       | `null`        |
| `user`                          | `String`  | No       | Database username.                                                                                                                                                                   | `null`        |
| `password`                      | `String`  | No       | Database password.                                                                                                                                                                   | `null`        |
| `wrapperDialect`                | `String`  | No       | Please see [this page on database dialects](/docs/using-the-jdbc-driver/DatabaseDialects.md), and whether you should include it.                                                     | `null`        |

## Plugins
The AWS JDBC Driver uses plugins to execute JDBC methods. You can think of a plugin as an extensible code module that adds extra logic around any JDBC method calls. The AWS JDBC Driver has a number of [built-in plugins](#list-of-available-plugins) available for use. 

Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a `String` name in the form of plugin code.

### Connection Plugin Manager Parameters

| Parameter            | Value    | Required | Description                                                                                                                                                                                          | Default Value                          |
|----------------------|----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| `wrapperPlugins`     | `String` | No       | Comma separated list of connection plugin codes. <br><br>Example: `failover,efm`                                                                                                                     | `auroraConnectionTracker,failover,efm` | 
| `wrapperProfileName` | `String` | No       | Driver configuration profile name. Instead of listing plugin codes with `wrapperPlugins`, the driver profile can be set with this parameter. <br><br> Example: See [below](#configuration-profiles). | `null`                                 |

To use a built-in plugin, specify its relevant plugin code for the `wrapperPlugins`.
The default value for `wrapperPlugins` is `auroraConnectionTracker,failover,efm`. These 3 plugins are enabled by default. To read more about these plugins, see the [List of Available Plugins](#list-of-available-plugins) section.
To override the default plugins, simply provide a new value for `wrapperPlugins`.
For instance, to use the [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md) and the [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md):

```java
properties.setProperty("wrapperPlugins", "iam,failover");
```

> :exclamation:**NOTE**: The plugins will be initialized and executed in the order they have been specified.

Provide an empty string to disable all plugins:
```java
properties.setProperty("wrapperPlugins", "");
```
The Wrapper behaves like the target driver when no plugins are used.

### Configuration Profiles
An alternative way of loading plugins is to use a configuration profile. You can create custom configuration profiles that specify which plugins the AWS JDBC Driver should load. After creating the profile, set the [`wrapperProfileName`](#connection-plugin-manager-parameters) parameter to the name of the created profile.
Although you can use this method of loading plugins, this method will most often be used by those who require custom plugins that cannot be loaded with the [`wrapperPlugins`](#connection-plugin-manager-parameters) parameter.
The following example creates and sets a configuration profile:

```java
properties.setProperty("wrapperProfileName", "testProfile");
DriverConfigurationProfiles.addOrReplaceProfile(
    "testProfile",
    Arrays.asList(
        FailoverConnectionPluginFactory.class, 
        HostMonitoringConnectionPluginFactory.class,
        CustomConnectionPluginFactory.class));
```

### List of Available Plugins
The AWS JDBC Driver has several built-in plugins that are available to use. Please visit the individual plugin page for more details.

| Plugin name                                                                                    | Plugin Code               | Database Compatibility | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Additional Required Dependencies                                                                                                                                                                              |
|------------------------------------------------------------------------------------------------|---------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                        | `failover`                | Aurora                 | Enables the failover functionality supported by Amazon Aurora clusters. Prevents opening a wrong connection to an old writer node dues to stale DNS after failover event. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                           | None                                                                                                                                                                                                          |                             
| [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)           | `efm`                     | Aurora                 | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                                                                       | None                                                                                                                                                                                                          |
| Data Cache Connection Plugin                                                                   | `dataCache`               | Any database           | Caches results from SQL queries matching the regular expression specified in the  `dataCacheTriggerCondition` configuration parameter.                                                                                                                                                                                                                                                                                                                                                                                 | None                                                                                                                                                                                                          |
| Execution Time Connection Plugin                                                               | `executionTime`           | Any database           | Logs the time taken to execute any JDBC method.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | None                                                                                                                                                                                                          |
| Log Query Connection Plugin                                                                    | `logQuery`                | Any database           | Tracks and logs the SQL statements to be executed. Sometimes SQL statements are not passed directly to the JDBC method as a parameter, such as [executeBatch()](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeBatch--). Users can set `enhancedLogQueryEnabled` to `true`, allowing the JDBC Wrapper to obtain SQL statements via Java Reflection. <br><br> :warning:**Note:** Enabling Java Reflection may cause a performance degradation.                                                | None                                                                                                                                                                                                          |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)     | `iam`                     | Any database           | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                                                               | [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds)                                                                                                                     |
| [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md)    | `awsSecretsManager`       | Any database           | Enables fetching database credentials from the AWS Secrets Manager service.                                                                                                                                                                                                                                                                                                                                                                                                                                            | [Jackson Databind](https://central.sonatype.com/artifact/com.fasterxml.jackson.core/jackson-databind) <br> [AWS Secrets Manager](https://central.sonatype.com/artifact/software.amazon.awssdk/secretsmanager) |
| Aurora Stale DNS Plugin                                                                        | `auroraStaleDns`          | Aurora                 | Prevents incorrectly opening a new connection to an old writer node when DNS records have not yet updated after a recent failover event. <br><br> :warning:**Note:** Contrary to `failover` plugin, `auroraStaleDns` plugin doesn't implement failover support itself. It helps to eliminate opening wrong connections to an old writer node after cluster failover is completed. <br><br> :warning:**Note:** This logic is already included in `failover` plugin so you can omit using both plugins at the same time. | None                                                                                                                                                                                                          |
| [Aurora Connection Tracker Plugin](./using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)   | `auroraConnectionTracker` | Aurora                 | Tracks all the opened connections. In the event of a cluster failover, the plugin will close all the impacted connections to the node. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                              | None                                                                                                                                                                                                          |
| [Driver Metadata Connection Plugin](./using-plugins/UsingTheDriverMetadataConnectionPlugin.md) | `driverMetaData`          | Any database           | Allows user application to override the return value of `DatabaseMetaData#getDriverName`                                                                                                                                                                                                                                                                                                                                                                                                                               | None                                                                                                                                                                                                          |
| [Read Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)             | `readWriteSplitting`      | Aurora                 | Enables read write splitting functionality where users can switch between database reader and writer instances.                                                                                                                                                                                                                                                                                                                                                                                                        | None                                                                                                                                                                                                          |
| [Developer Plugin](./using-plugins/UsingTheDeveloperPlugin.md)                                 | `dev`                     | Any database           | Helps developers test various everyday scenarios including rare events like network outages and database cluster failover. The plugin allows injecting and raising an expected exception, then verifying how applications handle it.                                                                                                                                                                                                                                                                                   | None                                                                                                                                                                                                          |

:exclamation: **NOTE**: As an enhancement, the wrapper is now able to automatically set the Aurora host list provider for connections to Aurora MySQL and Aurora PostgreSQL databases.
Aurora Host List Connection Plugin is deprecated. If you were using the Aurora Host List Connection Plugin, you can simply remove the plugin from the `wrapperPlugins` parameter.
However, if you choose to, you can ensure the provider is used by specifying a topology-aware dialect, for more information, see [Database Dialects](../using-the-jdbc-driver/DatabaseDialects.md).

:exclamation:**NOTE**: To see information logged by plugins such as `DataCacheConnectionPlugin` and `LogQueryConnectionPlugin`,
> see the [Logging](#logging) section.

In addition to the built-in plugins, you can also create custom plugins more suitable for your needs.
For more information, see [Custom Plugins](../development-guide/LoadablePlugins.md#using-custom-plugins).

### Using a Snapshot of the Driver
If there is an unreleased feature you would like to try, it may be available in a snapshot build of the driver. Snapshot builds can be found [here](https://aws.oss.sonatype.org/content/repositories/snapshots/software/amazon/jdbc/aws-advanced-jdbc-wrapper/). To use a snapshot, find the desired `.jar` file, which will be named `aws-advanced-jdbc-wrapper-<version>-<date>-<time>-<snapshot-number>.jar`, and add it to your project as a dependency.

#### As a Maven dependency
```xml
<dependencies>
  <dependency>
    <groupId>software.amazon.jdbc</groupId>
    <artifactId>aws-advanced-jdbc-wrapper</artifactId>
    <version>2.2.5-SNAPSHOT</version>
    <scope>system</scope>
    <systemPath>path-to-snapshot-jar</systemPath>
  </dependency>
</dependencies>
```

#### As a Gradle dependency
```gradle
dependencies {
    implementation(files("path-to-snapshot-jar"))
}
```

## AWS JDBC Driver for MySQL Migration Guide

**[The Amazon Web Services (AWS) JDBC Driver for MySQL](https://github.com/awslabs/aws-mysql-jdbc)**allows an
application to take advantage of the features of clustered MySQL databases. It is based on and can be used as a drop-in
compatible for the[MySQL Connector/J driver](https://github.com/mysql/mysql-connector-j), and is compatible with all
MySQL deployments.

The AWS JDBC Driver has the same functionalities as the AWS JDBC Driver for MySQL, as well as additional features such as support for Read/Write Splitting. This
section highlights the steps required to migrate from the AWS JDBC Driver for MySQL to the AWS JDBC Driver.

### Replacement Steps

1. Update the driver class name from `software.aws.rds.jdbc.mysql.Driver` to `software.amazon.jdbc.Driver`
2. Update the URL JDBC protocol from `jdbc:mysql:aws:` to  `jdbc:aws-wrapper:mysql`
3. Update the plugin configuration parameter from `connectionPluginFactories`
   to  [wrapperPlugins](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#connection-plugin-manager-parameters).
   See more details below.

### Plugins Configuration

In the AWS JDBC Driver for MySQL, plugins are set by providing a list of connection plugin factories:

```java
"jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/db?connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.AWSSecretsManagerPluginFactory,com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory,com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory"
```

In the AWS JDBC Driver, plugins are set by specifying the plugin codes:

```java
"jdbc:aws-wrapper:mysql://db-identifier.XYZ.us-east-2.rds.amazonaws.com:3306/db?wrapperPlugins=iam,failover"
```

To see the list of available plugins and their associated plugin code, see
the [documentation](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#list-of-available-plugins).

The AWS JDBC Driver also provides
the [Read-Write Splitting plugin](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheReadWriteSplittingPlugin.md#read-write-splitting-plugin),
this plugin allows the application to switch the connections between writer and reader instances by calling
the `Connection#setReadOnly` method.

### Example Configurations

#### Using the IAM Authentication Plugin with AWS JDBC Driver for MySQL

```java
public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("useAwsIam", "true");
    properties.setProperty("user", "foo");

    try (final Connection conn = DriverManager.getConnection(
        "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306", properties);
        final Statement statement = conn.createStatement();
        final ResultSet result = statement.executeQuery("SELECT 1")) {
      System.out.println(Util.getResult(result));
    }
  }
```

#### Using the IAM Authentication Plugin with AWS JDBC Driver

```java
public static void main(String[] args) throws SQLException {

    final Properties properties = new Properties();
    properties.setProperty("wrapperPlugins", "iam");
    properties.setProperty("user", "iam_user");

    try (Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:mysql://db-identifier.XYZ.us-east-2.rds.amazonaws.com:3306", properties);
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery("SELECT 1")) {

      System.out.println(Util.getResult(result));
    }
  }
```

The IAM Authentication Plugin in the AWS JDBC Driver has extra parameters to support custom endpoints. For more
information,
see [How do I use IAM with the AWS Advanced JDBC Driver?](https://github.com/awslabs/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheIamAuthenticationPlugin.md#how-do-i-use-iam-with-the-aws-advanced-jdbc-driver)

### Secrets Manager Plugin

The Secrets Manager Plugin in both the AWS JDBC Driver for MySQL and the AWS JDBC Driver uses the same configuration
parameters. To migrate to the AWS JDBC Driver, simply change
the `connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.AWSSecretsManagerPluginFactory` parameter
to `wrapperPlugins=awsSecretsManager`

#### Using the AWS Secrets Manager Plugin with AWS JDBC Driver for MySQL

```java
public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("connectionPluginFactories", AWSSecretsManagerPluginFactory.class.getName());
    properties.setProperty("secretsManagerSecretId", "secretId");
    properties.setProperty("secretsManagerRegion", "us-east-2");

    try (final Connection conn = DriverManager.getConnection(
        "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306", properties);
        final Statement statement = conn.createStatement();
        final ResultSet result = statement.executeQuery("SELECT 1")) {
      System.out.println(Util.getResult(result));
    }
  }
```
#### Using the AWS Secrets Manager Plugin with AWS JDBC Driver

```java
public static void main(String[] args) throws SQLException {

    final Properties properties = new Properties
    properties.setProperty("wrapperPlugins", "awsSecretsManagers");
    properties.setProperty("secretsManagerSecretId", "secretId");
    properties.setProperty("secretsManagerRegion", "us-east-2");

    try (Connection conn = DriverManager.getConnection("jdbc:aws-wrapper:mysql://db-identifier.XYZ.us-east-2.rds.amazonaws.com:3306", properties);
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery("SELECT 1")) {

      System.out.println(Util.getResult(result));
    }
  }
```

## Enable Logging
To enable logging in the AWS JDBC Driver, change the `logger=StandardLogger` parameter to `wrapperLoggerLevel=FINEST`
