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

There are multiple ways to enable logging when using the AWS JDBC Driver, some ways include:
- using the built-in Java Util Logger library
- using an external logger

### Using the Java Util Logger
To enable logging with the Java Util Logger library to log information, you can use either the `.properties` file or the `wrapperLoggerLevel` connection parameter:
#### Properties File
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
#### Connection Parameter
The AWS JDBC Driver also has a parameter, [`wrapperLoggerLevel`](#aws-advanced-jdbc-driver-parameters), to configure the logging level.
```java
 final Properties properties = new Properties();
 properties.setProperty("wrapperLoggerLevel", "finest");
 Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
```

### Using an External Logger
The two methods above are more suitable for simple Java applications that do not have a 3rd party logging library configured. If you are using the AWS JDBC Driver with Spring or Spring Boot, you can enable logging by adding the following to your configuration file.
For `application.yml`:
```yaml
logging:  
  level:  
    software.amazon.jdbc: TRACE
```

For `application.properties`:
```properties
logging.level.software.amazon.jdbc=trace
```

## AWS Advanced JDBC Driver Parameters
These parameters are applicable to any instance of the AWS JDBC Driver.

| Parameter                                         | Value     | Required | Description                                                                                                                                                                                                                                                                                                                                          | Default Value |
|---------------------------------------------------|-----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `wrapperLoggerLevel`                              | `String`  | No       | Logger level of the AWS JDBC Driver. <br><br/>If it is used, it must be one of the following values: `OFF`, `SEVERE`, `WARNING`, `INFO`, `CONFIG`, `FINE`, `FINER`, `FINEST`, `ALL`.                                                                                                                                                                 | `null`        |
| `database`                                        | `String`  | No       | Database name.                                                                                                                                                                                                                                                                                                                                       | `null`        |
| `user`                                            | `String`  | No       | Database username.                                                                                                                                                                                                                                                                                                                                   | `null`        |
| `password`                                        | `String`  | No       | Database password.                                                                                                                                                                                                                                                                                                                                   | `null`        |
| `wrapperDialect`                                  | `String`  | No       | Please see [this page on database dialects](/docs/using-the-jdbc-driver/DatabaseDialects.md), and whether you should include it.                                                                                                                                                                                                                     | `null`        |
| `wrapperLogUnclosedConnections`                   | `Boolean` | No       | Allows the AWS JDBC Driver to capture a stacktrace for each connection that is opened. If the `finalize()` method is reached without the connection being closed, the stacktrace is printed to the log. This helps developers to detect and correct the source of potential connection leaks.                                                        | `false`       |
| `loginTimeout`                                    | `Integer` | No       | Login timeout in milliseconds.                                                                                                                                                                                                                                                                                                                       | `null`        |
| `connectTimeout`                                  | `Integer` | No       | Socket connect timeout in milliseconds.                                                                                                                                                                                                                                                                                                              | `null`        |
| `socketTimeout`                                   | `Integer` | No       | Socket timeout in milliseconds.                                                                                                                                                                                                                                                                                                                      | `null`        |
| `tcpKeepAlive`                                    | `Boolean` | No       | Enable or disable TCP keep-alive probe.                                                                                                                                                                                                                                                                                                              | `false`       |
| `targetDriverAutoRegister`                        | `Boolean` | No       | Allows the AWS JDBC Driver to register a target driver based on `wrapperTargetDriverDialect` configuration parameter or, if it's missed, on a connection url protocol.                                                                                                                                                                               | `true`        |
| `transferSessionStateOnSwitch`                    | `Boolean` | No       | Enables transferring the session state to a new connection.                                                                                                                                                                                                                                                                                          | `true`        |
| `resetSessionStateOnClose`                        | `Boolean` | No       | Enables resetting the session state before closing connection.                                                                                                                                                                                                                                                                                       | `true`        |
| `rollbackOnSwitch`                                | `Boolean` | No       | Enables rolling back a current transaction, if any in effect, before switching to a new connection.                                                                                                                                                                                                                                                  | `true`        |
| `awsProfile`                                      | `String`  | No       | Allows users to specify a profile name for AWS credentials. This parameter is used by plugins that require AWS credentials, like the [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md) and the [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md).                 | `null`        |
| `enableGreenNodeReplacement`                      | `Boolean` | No       | Enables replacing a green node host name with the original host name when the green host DNS doesn't exist anymore after a blue/green switchover. Refer to [Overview of Amazon RDS Blue/Green Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-overview.html) for more details about green and blue nodes. | `false`       |
| `wrapperCaseSensitive`,<br>`wrappercasesensitive` | `Boolean` | No       | Allows the driver to change case sensitivity for parameter names in the connection string and in connection properties. Set parameter to `false` to allow case-insensitive parameter names.                                                                                                                                                          | `true`        |

## Plugins
The AWS JDBC Driver uses plugins to execute JDBC methods. You can think of a plugin as an extensible code module that adds extra logic around any JDBC method calls. The AWS JDBC Driver has a number of [built-in plugins](#list-of-available-plugins) available for use. 

Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a `String` name in the form of plugin code.

### Connection Plugin Manager Parameters

| Parameter                         | Value     | Required | Description                                                                                                                                                                                          | Default Value                           |
|-----------------------------------|-----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `wrapperPlugins`                  | `String`  | No       | Comma separated list of connection plugin codes. <br><br>Example: `failover,efm2`                                                                                                                    | `auroraConnectionTracker,failover,efm2` | 
| `autoSortWrapperPluginOrder`      | `Boolean` | No       | Allows the AWS JDBC Driver to sort connection plugins to prevent plugin misconfiguration. Allows a user to provide a custom plugin order if needed.                                                  | `true`                                  | 
| `wrapperProfileName`              | `String`  | No       | Driver configuration profile name. Instead of listing plugin codes with `wrapperPlugins`, the driver profile can be set with this parameter. <br><br> Example: See [below](#configuration-profiles). | `null`                                  |

To use a built-in plugin, specify its relevant plugin code for the `wrapperPlugins`.
The default value for `wrapperPlugins` is `auroraConnectionTracker,failover,efm2`. These 3 plugins are enabled by default. To read more about these plugins, see the [List of Available Plugins](#list-of-available-plugins) section.
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
An alternative way of loading plugins and providing configuration parameters is to use a configuration profile. You can create custom configuration profiles that specify which plugins the AWS JDBC Driver should load. After creating the profile, set the [`wrapperProfileName`](#connection-plugin-manager-parameters) parameter to the name of the created profile.
This method of loading plugins will most often be used by those who require custom plugins that cannot be loaded with the [`wrapperPlugins`](#connection-plugin-manager-parameters) parameter, or by those who are using preset configurations.

Besides a list of plugins to load and configuration properties, configuration profiles may also include the following items:
- [Database Dialect](./using-the-jdbc-driver/DatabaseDialects.md#database-dialects)
- [Target Driver Dialect](./using-the-jdbc-driver/TargetDriverDialects.md#target-driver-dialects)
- a custom exception handler
- a custom connection provider

The following example creates and sets a configuration profile:

```java
// Create a new configuration profile with name "testProfile"
ConfigurationProfileBuilder.get()
    .withName("testProfile")
    .withPluginFactories(Arrays.asList(
        FailoverConnectionPluginFactory.class,
        HostMonitoringConnectionPluginFactory.class,
        CustomConnectionPluginFactory.class))
    .buildAndSet();

// Use the configuration profile "testProfile"
properties.setProperty("wrapperProfileName", "testProfile");
```

Configuration profiles can be created based on other existing configuration profiles. Profile names are case sensitive and should be unique.

```java
// Create a new configuration profile with name "newProfile" based on "existingProfileName"
ConfigurationProfileBuilder.from("existingProfileName")
    .withName("newProfileName")
    .withDialect(new CustomDatabaseDialect())
.buildAndSet();

// Delete configuration profile "testProfile"
DriverConfigurationProfiles.remove("testProfile");
```

The AWS JDBC Driver team has gathered and analyzed various user scenarios to create commonly used configuration profiles, or presets, for users. These preset configuration profiles are optimized, profiled, verified and can be used right away. Users can create their own configuration profiles based on the built-in presets as shown above. More details could be found at the [Configuration Presets](./ConfigurationPresets.md) page. 

### Executing Custom Code When Initializing a Connection
In some use cases you may need to define a specific configuration for a new driver connection before your application can use it. For instance:
- you might need to run some initial SQL queries when a connection is established, or;
- you might need to check for some additional conditions to determine the initialization configuration required for a particular connection.

The AWS JDBC Driver allows specifying a special function that can initialize a connection. It can be done with `Driver.setConnectionInitFunc` method. The `resetConnectionInitFunc` method is also available to remove the function.

The initialization function is called for all connections, including connections opened by the internal connection pools (see [Using Read Write Splitting Plugin and Internal Connection Pooling](./using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling)). This helps user applications clean up connection sessions that have been altered by previous operations, as returning a connection to a pool will reset the state and retrieving it will call the initialization function again.

> [!WARNING]\
> Executing CPU and network intensive code in the initialization function may significantly impact the AWS JDBC Driver's overall performance.

```java
Driver.setConnectionInitFunc((connection, protocol, hostSpec, props) -> {
    // Set custom schema for connections to a test-database  
    if ("test-database".equals(props.getProperty("database"))) {
        connection.setSchema("test-database-schema");
    }
});
```


### List of Available Plugins
The AWS JDBC Driver has several built-in plugins that are available to use. Please visit the individual plugin page for more details.

| Plugin name                                                                                                       | Plugin Code               | Database Compatibility                                                                                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Additional Required Dependencies                                                                                                                                                                              |
|-------------------------------------------------------------------------------------------------------------------|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                                           | `failover`                | Aurora, RDS Multi-AZ DB Cluster                                                                                                                                                                                                                                                                                     | Enables the failover functionality supported by Amazon Aurora clusters and RDS Multi-AZ DB clusters. Prevents opening a wrong connection to an old writer node dues to stale DNS after failover event. This plugin is enabled by default.                                                                                                                                                                                                                                                                              | None                                                                                                                                                                                                          |                             
| [Failover Connection Plugin v2](./using-plugins/UsingTheFailover2Plugin.md)                                       | `failover2`               | Enables the failover functionality supported by Amazon Aurora clusters. Prevents opening a wrong connection to an old writer node due to stale DNS after failover event. It is functionally the same as the first version of the Failover Connection Plugin (`failover`) and uses similar configuration parameters. | None                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |                             
| [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)                              | `efm`                     | Aurora, RDS Multi-AZ DB Cluster                                                                                                                                                                                                                                                                                     | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                                                                       | None                                                                                                                                                                                                          |
| [Host Monitoring Connection Plugin v2](./using-plugins/UsingTheHostMonitoringPlugin.md#host-monitoring-plugin-v2) | `efm2`                    | Aurora, RDS Multi-AZ DB Cluster                                                                                                                                                                                                                                                                                     | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is an alternative implementation for host health status monitoring. It is functionally the same as the `efm` plugin and uses the same configuration parameters.                                                                                                                                                                                                                                              | None                                                                                                                                                                                                          |
| Data Cache Connection Plugin                                                                                      | `dataCache`               | Any database                                                                                                                                                                                                                                                                                                        | Caches results from SQL queries matching the regular expression specified in the  `dataCacheTriggerCondition` configuration parameter.                                                                                                                                                                                                                                                                                                                                                                                 | None                                                                                                                                                                                                          |
| Execution Time Connection Plugin                                                                                  | `executionTime`           | Any database                                                                                                                                                                                                                                                                                                        | Logs the time taken to execute any JDBC method.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | None                                                                                                                                                                                                          |
| Log Query Connection Plugin                                                                                       | `logQuery`                | Any database                                                                                                                                                                                                                                                                                                        | Tracks and logs the SQL statements to be executed. Sometimes SQL statements are not passed directly to the JDBC method as a parameter, such as [executeBatch()](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeBatch--). Users can set `enhancedLogQueryEnabled` to `true`, allowing the JDBC Wrapper to obtain SQL statements via Java Reflection. <br><br> :warning:**Note:** Enabling Java Reflection may cause a performance degradation.                                                | None                                                                                                                                                                                                          |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)                        | `iam`                     | Aurora, RDS[^1]                                                                                                                                                                                                                                                                                                     | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                                                               | [AWS Java SDK RDS v2.x](https://central.sonatype.com/artifact/software.amazon.awssdk/rds)                                                                                                                     |
| [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md)                       | `awsSecretsManager`       | Any database                                                                                                                                                                                                                                                                                                        | Enables fetching database credentials from the AWS Secrets Manager service.                                                                                                                                                                                                                                                                                                                                                                                                                                            | [Jackson Databind](https://central.sonatype.com/artifact/com.fasterxml.jackson.core/jackson-databind) <br> [AWS Secrets Manager](https://central.sonatype.com/artifact/software.amazon.awssdk/secretsmanager) |
| [Federated Authentication Plugin](./using-plugins/UsingTheFederatedAuthPlugin.md)                                 | `federatedAuth`           | Aurora, RDS[^2]                                                                                                                                                                                                                                                                                                     | Enables users to authenticate using Federated Identity and then connect to their Amazon Aurora Cluster using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                 |                                                                                                                                                                                                               |
| [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthPlugin.md)                                           | `okta`                    | Aurora, RDS[^2]                                                                                                                                                                                                                                                                                                     | Enables users to authenticate using Federated Identity and then connect to their Amazon Aurora Cluster using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                 |                                                                                                                                                                                                               |
| Aurora Stale DNS Plugin                                                                                           | `auroraStaleDns`          | Aurora                                                                                                                                                                                                                                                                                                              | Prevents incorrectly opening a new connection to an old writer node when DNS records have not yet updated after a recent failover event. <br><br> :warning:**Note:** Contrary to `failover` plugin, `auroraStaleDns` plugin doesn't implement failover support itself. It helps to eliminate opening wrong connections to an old writer node after cluster failover is completed. <br><br> :warning:**Note:** This logic is already included in `failover` plugin so you can omit using both plugins at the same time. | None                                                                                                                                                                                                          |
| [Aurora Connection Tracker Plugin](./using-plugins/UsingTheAuroraConnectionTrackerPlugin.md)                      | `auroraConnectionTracker` | Aurora, RDS Multi-AZ DB Cluster                                                                                                                                                                                                                                                                                     | Tracks all the opened connections. In the event of a cluster failover, the plugin will close all the impacted connections to the node. This plugin is enabled by default.                                                                                                                                                                                                                                                                                                                                              | None                                                                                                                                                                                                          |
| [Driver Metadata Connection Plugin](./using-plugins/UsingTheDriverMetadataConnectionPlugin.md)                    | `driverMetaData`          | Any database                                                                                                                                                                                                                                                                                                        | Allows user application to override the return value of `DatabaseMetaData#getDriverName`                                                                                                                                                                                                                                                                                                                                                                                                                               | None                                                                                                                                                                                                          |
| [Read Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)                                | `readWriteSplitting`      | Aurora                                                                                                                                                                                                                                                                                                              | Enables read write splitting functionality where users can switch between database reader and writer instances.                                                                                                                                                                                                                                                                                                                                                                                                        | None                                                                                                                                                                                                          |
| [Developer Plugin](./using-plugins/UsingTheDeveloperPlugin.md)                                                    | `dev`                     | Any database                                                                                                                                                                                                                                                                                                        | Helps developers test various everyday scenarios including rare events like network outages and database cluster failover. The plugin allows injecting and raising an expected exception, then verifying how applications handle it.                                                                                                                                                                                                                                                                                   | None                                                                                                                                                                                                          |
| [Aurora Initial Connection Strategy](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md)            | `initialConnection`       | Aurora                                                                                                                                                                                                                                                                                                              | Allows users to configure their initial connection strategy to reader cluster endpoints.                                                                                                                                                                                                                                                                                                                                                                                                                               | None                                                                                                                                                                                                          |
| [Limitless Connection Plugin](./using-plugins/UsingTheLimitlessConnectionPlugin.md)                               | `limitless`               | Aurora                                                                                                                                                                                                                                                                                                              | Enables client-side load-balancing of Transaction Routers on Amazon Aurora Limitless Databases .                                                                                                                                                                                                                                                                                                                                                                                                                       | None                                                                                                                                                                                                          |
| Fastest Response Strategy Plugin                                                                              | `fastestResponseStrategy` | Aurora                          | When read-write splitting is enabled, this plugin selects the reader to switch to based on the host with the fastest response time. The plugin achieves this by periodically monitoring the hosts' response times and storing the fastest host in a cache. **Note:** the `readerHostSelectorStrategy` parameter must be set to `fastestResponse` in the user-defined connection properties in order to enable this plugin. See [reader selection strategies](./ReaderSelectionStrategies.md).                           | None                                                                                                                           |

[^1]: [RDS Multi-AZ DB Clusters do not support IAM](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.Limitations.html).
[^2]: Federated Identity and Okta rely on IAM. Due to [^1], RDS Multi-AZ Clusters are not supported.

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
    <version>2.5.6-SNAPSHOT</version>
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

**[The Amazon Web Services (AWS) JDBC Driver for MySQL](https://github.com/awslabs/aws-mysql-jdbc)** allows an
application to take advantage of the features of clustered MySQL databases. It is based on and can be used as a drop-in
compatible for the [MySQL Connector/J driver](https://github.com/mysql/mysql-connector-j), and is compatible with all
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

    final Properties properties = new Properties();
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
