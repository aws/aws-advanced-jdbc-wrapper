# Using the AWS Advanced JDBC Wrapper
The AWS Advanced JDBC Wrapper leverages existing community JDBC drivers and enables support of AWS and Aurora functionalities. Currently, only the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc) is supported.
The JDBC Wrapper also supports [connection pooling](./DataSource.md#Using-the-AwsWrapperDataSource-with-Connection-Pooling-Frameworks).

## Wrapper Protocol
The JDBC Wrapper uses the protocol prefix `jdbc:aws-wrapper:`. Internally, the JDBC Wrapper will replace this protocol prefix with `jdbc:`, making the final protocol `jdbc:aws-wrapper:{suffix}` where `suffix` is specific to the desired underlying protocol. For example, to connect to a PostgreSQL database, you would use the protocol `jdbc:aws-wrapper:postgresql:`, and inside the JDBC Wrapper, the final protocol that will be used to connect to a database will be `jdbc:postgresql:`.

## Getting a Connection
To get a connection from the JDBC Wrapper, the user application can either connect with a DriverManager or with a DataSource. The process of getting a connection with a DriverManager will remain the same as with other JDBC Drivers, but getting a connection with a DataSource may require some additional steps. See [here](./DataSource.md) for more details.

## Logging
The JDBC Wrapper uses the Java Util Logger to log information.

To enable logging and see information logged by the driver:

1. Create a `.properties` file and configure the logging level
2. Specify the `.properties` file with the `java.util.logging.config.file` VM option when running the program
   `-Djava.util.logging.config.file=absolute\path\to\logging.properties`

An example  `.properties` file is as follows:

```properties
# Possible values for log level (from most detailed to less detailed): FINEST, FINER, FINE, CONFIG, INFO, WARNING, SEVERE
.level=INFO
handlers=java.util.logging.ConsoleHandler
java.util.logging.ConsoleHandler.level=ALL
com.amazon.awslabs.jdbc.Driver.level=FINER
com.amazon.awslabs.jdbc.plugin.level=FINER
```

The JDBC Wrapper also has a parameter, [`wrapperLoggerLevel`](#aws-advanced-jdbc-wrapper-parameters), to configure the logging level.

## AWS Advanced JDBC Wrapper Parameters
These parameters are applicable to any instance of the JDBC Wrapper.

<details>
<summary>AWS Advanced JDBC Wrapper Parameters</summary>

| Parameter                                 | Value     | Required | Description                                                                                                                                                                 | Default Value |
|-------------------------------------------|-----------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `wrapperLogUnclosedConnections`           | `Boolean` | No       | Allows the JDBC Wrapper to track a point in the code where connection has been opened and never closed after.                                                               | `false`       |
| `wrapperLoggerLevel`                      | `String`  | No       | Logger level of the driver. <br><br/>If it is used, it must be one of the following values: `OFF`, `SEVERE`, `WARNING`, `INFO`, `CONFIG`, `FINE`, `FINER`, `FINEST`, `ALL`. | `null`        |
| `wrapperUser`                             | `String`  | No       | Driver user name.                                                                                                                                                           | `null`        |
| `wrapperPassword`                         | `String`  | No       | Driver password.                                                                                                                                                            | `null`        |
| `wrapperDatabaseName`                     | `String`  | No       | Driver database name.                                                                                                                                                       | `null`        |
| `wrapperTargetDriverUserPropertyName`     | `String`  | No       | Target driver user property name.                                                                                                                                           | `null`        |
| `wrapperTargetDriverPasswordPropertyName` | `String`  | No       | Target driver password property name.                                                                                                                                       | `null`        |
</details>

## Plugins
The JDBC Wrapper uses the plugins to execute JDBC methods.
One can think of the plugins as extensible code modules that add extra logic around any JDBC method calls.
The JDBC Wrapper has a number of [built-in plugins](#list-of-available-plugins) available for use.
Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a `String` name in the form of a plugin code.

### Connection Plugin Manager Parameters
<details>
<summary>Connection Plugin Manager Parameters</summary>

| Parameter            | Value    | Required | Description                                                                                                                                                                                          | Default Value |
|----------------------|----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `wrapperPlugins`     | `String` | No       | Comma separated list of connection plugin codes. <br><br>Example: `failover,efm`                                                                                                                     | `null`        | 
| `wrapperProfileName` | `String` | No       | Driver configuration profile name. Instead of listing plugin codes with `wrapperPlugins`, the driver profile can be set with this parameter. <br><br> Example: See [below](#configuration-profiles). | `null`        |
</details>

To use a built-in plugin, specify its relevant plugin code for the `wrapperPlugins`.
For instance, to use the [FailoverConnectionPlugin](../../wrapper/src/main/java/com/amazon/awslabs/jdbc/plugin/failover/FailoverConnectionPlugin.java) and the [Host Monitoring Connection Plugin](../../wrapper/src/main/java/com/amazon/awslabs/jdbc/plugin/efm/HostMonitoringConnectionPlugin.java):

```java
properties.setProperty("wrapperPlugins", "failover,efm");
```

> :exclamation:**NOTE**: The plugins will be initialized and executed in the order they have been specified.

### Configuration Profiles
As an alternative way of loading plugins is to use a configuration profile.
Users are able to create configuration profiles that specify which plugins the JDBC Wrapper should load.
Once the profile is created, the [`wrapperProfileName`](#connection-plugin-manager-parameters) parameter can be set to the name of the created profile.
Although it's possible to use this method of loading plugins,
this will most often be used by those who require custom plugins that cannot be loaded with the [`wrapperPlugins`](#connection-plugin-manager-parameters) parameter.
See below for a sample on how to create and set a configuration profile.

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
The JDBC Wrapper has several built-in plugins that are available to use. Please visit the individual plugin page for more details.

| Plugin name                                                                                | Plugin Code      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------------------------------------------------------------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                    | `failover`       | Enables the failover functionality supported by Amazon Aurora clusters.                                                                                                                                                                                                                                                                                                                                                                                                 |
| [Host Monitoring Connection Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)       | `efm`            | Enables enhanced host connection failure monitoring, allowing faster failure detection rates.                                                                                                                                                                                                                                                                                                                                                                           |
| Aurora Host List Connection Plugin                                                         | `auroraHostList` | Retrieves Amazon Aurora clusters information. <br><br>**:warning:Note:** this plugin does not need to be explicitly loaded if the failover connection plugin is loaded.                                                                                                                                                                                                                                                                                                 |
| Data Cache Connection Plugin                                                               | `dataCache`      | Caches results from SQL queries matching the regular expression specified in the  `dataCacheTriggerCondition` configuration parameter.                                                                                                                                                                                                                                                                                                                                  |
| Execution Time Connection Plugin                                                           | `executionTime`  | Logs the time taken to execute any JDBC method.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| Log Query Connection Plugin                                                                | `logQuery`       | Tracks and logs the SQL statements to be executed. Sometimes SQL statements are not passed directly to the JDBC method as a parameter, such as [executeBatch()](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeBatch--). Users can set `enhancedLogQueryEnabled` to `true`, allowing the JDBC Wrapper to obtain SQL statements via Java Reflection. <br><br> :warning:**Note:** Enabling Java Reflection may cause a performance degradation. |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md) | `iam`            | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                                                                                                                                                                                                                                                                                |                                                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                           |                  |

> :exclamation:**NOTE**: To see information logged by plugins such as `DataCacheConnectionPlugin` and `LogQueryConnectionPlugin`,
> see the [Logging](#logging) section.

In addition to the built-in plugins, you can also create custom plugins more suitable for your needs.
For more information, see [Custom Plugins](../development-guide/LoadablePlugins.md#using-custom-plugins).
