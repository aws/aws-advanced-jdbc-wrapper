# Plugins As Loadable Modules
Plugins are loadable and extensible modules that add extra logic around JDBC method calls.

Plugins let users:
- monitor connections
- handle exceptions during executions
- log execution details, such as SQL statements executed
- cache execution results
- measure execution time
- and more

The JDBC Wrapper has several built-in plugins; you can [see the list here](../using-the-jdbc-driver/UsingTheJdbcDriver.md#list-of-available-plugins).

## Available Services

Plugins are notified by the connection plugin manager when changes to the database connection occur, and utilize the [plugin service](./PluginService.md) to establish connections and retrieve host information. 

## Using Custom Plugins

To use a custom plugin, you must:
1. Create a custom plugin.
2. Register the custom plugin to a wrapper profile.
3. Specify the custom profile name to use in `wrapperProfileName`.

### Creating Custom Plugins

There are two ways to create a custom plugin:
- implement the [ConnectionPlugin](../../wrapper/src/main/java/software/amazon/jdbc/ConnectionPlugin.java) interface directly, or
- extend the [AbstractConnectionPlugin](../../wrapper/src/main/java/software/amazon/jdbc/plugin/AbstractConnectionPlugin.java) class.

The `AbstractConnectionPlugin` class provides a simple implementation for all the methods in `ConnectionPlugin`,
as it calls the provided JDBC method without additional operations. This is helpful when the custom plugin only needs to override one (or a few) methods from the `ConnectionPlugin` interface.
See the following classes for examples:

- [IamAuthConnectionPlugin](../../wrapper/src/main/java/software/amazon/jdbc/plugin/iam/IamAuthConnectionPlugin.java)
    - The `IamAuthConnectionPlugin` class only overrides the `connect` method because the plugin is only concerned with creating
      database connections with IAM database credentials.

- [ExecutionTimeConnectionPlugin](../../wrapper/src/main/java/software/amazon/jdbc/plugin/ExecutionTimeConnectionPlugin.java)
    - The `ExecutionTimeConnectionPlugin` only overrides the `execute` method because it is only concerned with elapsed time during execution, it does not establish new connections or set up any host list provider.

A `ConnectionPluginFactory` implementation is also required for the new custom plugin. This factory class is used to register and initialize custom plugins. See [ExecutionTimeConnectionPluginFactory](../../wrapper/src/main/java/software/amazon/jdbc/plugin/ExecutionTimeConnectionPluginFactory.java) for a simple implementation example.

### Subscribed Methods

The `Set<String> getSubscribedMethods()` method specifies a set of JDBC methods a plugin is subscribed to. All plugins must implement the `Set<String> getSubscribedMethods()` method.

When executing a JDBC method, the plugin manager will only call a specific plugin method if the JDBC method is within its set of subscribed methods. For example, [LogQueryConnectionPlugin](../../wrapper/src/main/java/software/amazon/jdbc/plugin/LogQueryConnectionPlugin.java) only subscribes to JDBC methods related to query execution, such as `Statement.execute`. This plugin will not be triggered by method calls like `Connection.isValid`.

Plugins can subscribe to any of the JDBC API methods listed [here](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html); some examples are as follows:

- `Statement.executeQuery`
- `PreparedStatement.executeQuery`
- `CallableStatement.execute`

Plugins can also subscribe to the following pipelines:

| Pipeline                                                                                            | Method Name / Subscription Key |
|-----------------------------------------------------------------------------------------------------|:------------------------------:|
| [Host list provider pipeline](./Pipelines.md#host-list-provider-pipeline)                           |        initHostProvider        |
| [Connect pipeline](./Pipelines.md#connect-pipeline)                                                 |            connect             |
| [Connection changed notification pipeline](./Pipelines.md#connection-changed-notification-pipeline) |    notifyConnectionChanged     |
| [Node list changed notification pipeline](./Pipelines.md#node-list-changed-notification-pipeline)   |     notifyNodeListChanged      |                                                                      

### Tips on Creating a Custom Plugin

A custom plugin can subscribe to all JDBC methods being executed, which means it may be active in every workflow.
We recommend that you be aware of the performance impact of subscribing and performing demanding tasks for every JDBC method.

### Register the Custom Plugin
The `DriverConfigurationProfileBuilder` manages the plugin profiles.
Implement a class which implements `ConnectionPluginFactory` for your custom plugin and then 
register a new custom plugin, by calling `ConfigurationProfileBuilder.buildAndSet()` as follows:

```java
ConfigurationProfileBuilder.get()
.withName("customPluginProfile")   // this can be any name of your choosing except for existing names
.withPluginFactories(Collections.singletonList(YourCustomPluginFactory.class))
.buildAndSet();
```
In order to use the plugin to open a connection you must provide the custom profile name into the `wrapperProfileName` configuration parameter

```java
Properties props = ConnectionStringHelper.getDefaultProperties();
props.setProperty(PropertyDefinition.PROFILE_NAME.name, "customPluginProfile");

Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);
```
## What is Not Allowed in Plugins

When creating custom plugins, it is important to **avoid** the following bad practices in your plugin implementation:

1. Keeping local copies of shared information:
   - information like current connection, or the host list provider are shared across all plugins
   - shared information may be updated by any plugin at any time and should be retrieved via the plugin service when required
2. Using driver-specific properties or objects:
   - the AWS Advanced JDBC Wrapper may be used with multiple drivers, therefore plugins must ensure implementation is not restricted to a specific driver
3. Making direct connections:
   - the plugin should always call the pipeline lambdas (i.e. `JdbcCallable<Connection, SQLException> connectFunc`, `JdbcCallable<T, E> jdbcMethodFunc`)
4. Running long tasks synchronously:
   - the JDBC method calls are executed by all subscribed plugins synchronously; if one plugin runs a long task during the execution it blocks the execution for the other plugins

See the following examples for more details:

<details><summary>❌ <strong>Bad Example</strong></summary>

```java
public class BadPlugin extends AbstractConnectionPlugin {
    PluginService pluginService;
    HostListProvider hostListProvider;
    Properties props;

    BadPlugin(PluginService pluginService, Properties props) {
        this.pluginService = pluginService;
        this.props = props;

        // Bad Practice #1: keeping local copies of items
        // Plugins should not keep local copies of the host list provider, the topology or the connection.
        // Host list provider is kept in the Plugin Service and can be modified by other plugins,
        // therefore it should be retrieved by calling pluginService.getHostListProvider() when it is needed.
        this.hostListProvider = this.pluginService.getHostListProvider();
    }

    @Override
    public Set<String> getSubscribedMethods() {
        return new HashSet<>(Collections.singletonList("*"));
    }

    @Override
    public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props, boolean isInitialConnection,
            JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
        // Bad Practice #2: using driver-specific objects.
        // Not all drivers support the same configuration parameters. For instance, while MySQL Connector/J Supports "database",
        // PGJDBC uses "dbname" for database names.
        if (props.getProperty("database") == null) {
            props.setProperty("database", "defaultDatabase");
        }

        // Bad Practice #3: Making direct connections
        return DriverManager.getConnection(props.getProperty("url"), props);
    }    
}
```
</details>

<details><summary>✅ <strong>Good Example</strong></summary>

```java
public class GoodExample extends AbstractConnectionPlugin {
    PluginService pluginService;
    HostListProvider hostListProvider;
    Properties props;

    GoodExample(PluginService pluginService, Properties props) {
        this.pluginService = pluginService;
        this.props = props;
    }

    @Override
    public Set<String> getSubscribedMethods() {
        return new HashSet<>(Collections.singletonList("*"));
    }

    @Override
    public <T, E extends Exception> T execute(
        final Class<T> resultClass,
        final Class<E> exceptionClass,
        final Object methodInvokeOn,
        final String methodName,
        final JdbcCallable<T, E> jdbcMethodFunc,
        final Object[] jdbcMethodArgs)
        throws E {
      if (this.pluginService.getHosts().isEmpty()) {
        // Re-fetch host information if it is empty.
        this.pluginService.forceRefreshHostList();
      }
      return jdbcMethodFunc.call();
    }

    @Override
    public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props, boolean isInitialConnection,
            JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
        if (PropertyDefinition.USER.getString(props) == null) {
            PropertyDefinition.TARGET_DRIVER_USER_PROPERTY_NAME.set(props, "defaultUser");
        }

        // Call the pipeline lambda to connect.
        return connectFunc.call();
    }
}
```
</details>

