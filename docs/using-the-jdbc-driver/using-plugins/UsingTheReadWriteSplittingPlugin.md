# Read/Write Splitting Plugin

The read/write splitting plugin adds functionality to switch between writer/reader instances via calls to the `Connection#setReadOnly` method. Upon calling `setReadOnly(true)`, the plugin will connect to a reader instance according to a [reader selection strategy](#reader-selection-strategies) and direct subsequent queries to this instance. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the boolean argument you supply to the `setReadOnly` method.

## Loading the Read/Write Splitting Plugin

The read/write splitting plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter. If you would like to load the read/write splitting plugin alongside the failover and host monitoring plugins, the read/write splitting plugin must be listed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the read/write splitting plugin with these plugins.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm");
```

If you would like to use the read/write splitting plugin without the failover plugin, make sure you have the `readWriteSplitting` plugin in the `wrapperPlugins` property, and that the failover plugin is not part of it.
```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting");
```

> The Aurora Host List Plugin is deprecated after version 2.2.3. To use the Read Write Splitting plugin without failover with versions 2.2.3 and earlier, add the Aurora Host List Plugin to the plugin list like so: `"auroraHostList,readWriteSplitting"`.

## Supplying the connection string

When using the read/write splitting plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply just the URL for the initial instance to which you're connecting. You must also include either the failover plugin or the Aurora host list plugin in your plugin chain so that the driver knows to query Aurora for its topology. See the section on [loading the read/write splitting plugin](#loading-the-readwrite-splitting-plugin) for more info.

## Using the Read/Write Splitting Plugin against non-Aurora clusters

The read/write splitting plugin is not currently supported for non-Aurora clusters.

## Internal connection pooling

> [!WARNING]\
> If internal connection pools are enabled, database passwords may not be verified with every connection request. The initial connection request for each database instance in the cluster will verify the password, but subsequent requests may return a cached pool connection without re-verifying the password. This behavior is inherent to the nature of connection pools in general and not a bug with the driver. `ConnectionProviderManager.releaseResources` can be called to close all pools and remove all cached pool connections. See [InternalConnectionPoolPasswordWarning.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/InternalConnectionPoolPasswordWarning.java) for more details.

Whenever `setReadOnly(true)` is first called on a `Connection` object, the read/write plugin will internally open a new physical connection to a reader. After this first call, the physical reader connection will be cached for the given `Connection`. Future calls to `setReadOnly `on the same `Connection` object will not require opening a new physical connection. However, calling `setReadOnly(true)` for the first time on a new `Connection` object will require the plugin to establish another new physical connection to a reader. If your application frequently calls `setReadOnly`, you can enable internal connection pooling to improve performance. When enabled, the wrapper driver will maintain an internal connection pool for each instance in the cluster. This allows the read/write plugin to reuse connections that were established by `setReadOnly` calls on previous `Connection` objects.

> [!NOTE]\
> Initial connections to a cluster URL will not be pooled. The driver does not pool cluster URLs because it can be problematic to pool a URL that resolves to different instances over time. The main benefit of internal connection pools is when setReadOnly is called. When setReadOnly is called (regardless of the initial connection URL), an internal pool will be created for the writer/reader that the plugin switches to and connections for that instance can be reused in the future.

The wrapper driver currently uses [Hikari](https://github.com/brettwooldridge/HikariCP) to create and maintain its internal connection pools. The sample code [here](../../../examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) provides a useful example of how to enable this feature. The steps are as follows:

1.  Create an instance of `HikariPooledConnectionProvider`. The `HikariPooledConnectionProvider` constructor requires you to pass in a `HikariPoolConfigurator` function. Inside this function, you should create a `HikariConfig`, configure any desired properties on it, and return it. Note that the Hikari properties below will be set by default and will override any values you set in your function. This is done to follow desired behavior and ensure that the read/write plugin can internally establish connections to new instances.

- jdbcUrl (including the host, port, and database)
- exception override class name
- username
- password

You can optionally pass in a `HikariPoolMapping` function as a second parameter to the `HikariPooledConnectionProvider`. This allows you to decide when new connection pools should be created by defining what is included in the pool map key. A new pool will be created each time a new connection is requested with a unique key. By default, a new pool will be created for each unique instance-user combination. If you would like to define a different key system, you should pass in a `HikariPoolMapping` function defining this logic. A simple example is show below. Please see [ReadWriteSplittingPostgresExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) for the full example.

> [!WARNING]\
> If you do not include the username in your HikariPoolMapping function, connection pools may be shared between different users. As a result, an initial connection established with a privileged user may be returned to a connection request with a lower-privilege user without re-verifying credentials. This behavior is inherent to the nature of connection pools in general and not a bug with the driver. `ConnectionProviderManager.releaseResources` can be called to close all pools and remove all cached pool connections.

```java
props.setProperty("somePropertyValue", "1"); // used in getPoolKey
final HikariPooledConnectionProvider connProvider =
    new HikariPooledConnectionProvider(
        ReadWriteSplittingPostgresExample::getHikariConfig,
        ReadWriteSplittingPostgresExample::getPoolKey
    );
ConnectionProviderManager.setConnectionProvider(connProvider);

private static String getPoolKey(HostSpec hostSpec, Properties props) {
  // Include the URL, user, and somePropertyValue in the connection pool key so that a new
  // connection pool will be opened for each different instance-user-somePropertyValue
  // combination.
  final String user = props.getProperty(PropertyDefinition.USER.name);
  final String somePropertyValue = props.getProperty("somePropertyValue");
  return hostSpec.getUrl() + user + somePropertyValue;
}
```

2. Call `ConnectionProviderManager.setConnectionProvider`, passing in the `HikariPooledConnectionProvider` you created in step 1.

3. By default, the read/write plugin randomly selects a reader instance the first time that `setReadOnly(true)` is called. If you would like the plugin to select a reader based on a different selection strategy, please see the [Reader Selection Strategies](#reader-selection-strategies) section for more information.

4. Continue as normal: create connections and use them as needed.

5. When you are finished using all connections, call `ConnectionProviderManager.releaseResources`.

> [!IMPORTANT]\
> You must call `ConnectionProviderManager.releaseResources` to close the internal connection pools when you are finished using all connections. Unless `ConnectionProviderManager.releaseResources` is called, the wrapper driver will keep the pools open so that they can be shared between connections.

## Example
[ReadWriteSplittingPostgresExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) demonstrates how to enable and configure read/write splitting with the Aws Advanced JDBC Driver.

## Reader Selection Strategies
By default, the read/write plugin randomly selects a reader instance the first time that `setReadOnly(true)` is called. To balance connections to reader instances more evenly, different selection strategies can be used. The following table describes the currently available selection strategies and any relevant configuration parameters for each strategy.

To indicate which selection strategy to use, the `readerHostSelectorStrategy` configuration parameter can be set to one of the selection strategies in the table below. The following is an example of enabling the least connections strategy:

```java
props.setProperty(ReadWriteSplittingPlugin.READER_HOST_SELECTOR_STRATEGY.name, "leastConnections");
```

| Reader Selection Strategy | Configuration Parameter                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default Value |
|---------------------------|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `random`                  | This strategy does not have configuration parameters. | The random strategy is the default selection strategy. When switching to a reader connection, the reader instance will be chosen randomly from the available database instances.                                                                                                                                                                                                                                                                                                                                                                                  | N/A           |
| `leastConnections`        | This strategy does not have configuration parameters. | The least connections strategy will select reader instances based on which database instance has the least number of currently active connections. Note that this strategy is only available when internal connection pools are enabled - if you set the connection property without enabling internal pools, an exception will be thrown.                                                                                                                                                                                                                        | N/A           |
| `roundRobin`              | See the following rows for configuration parameters.  | The round robin strategy will select a reader instance by taking turns with all available database instances in a cycle. A slight addition to the round robin strategy is the weighted round robin strategy, where more connections will be passed to reader instances based on user specified connection properties.                                                                                                                                                                                                                                             | N/A           |
|                           | `roundRobinHostWeightPairs`                           | This parameter value must be a `string` type comma separated list of database host-weight pairs in the format `<host>:<weight>`. The host represents the database instance name, and the weight represents how many connections should be directed to the host in one cycle through all available hosts. For example, the value `instance-1:1,instance-2:4` means that for every connection to `instance-1`, there will be four connections to `instance-2`. <br><br> **Note:** The `<weight>` value in the string must be an integer greater than or equal to 1. | `null`        |
|                           | `roundRobinDefaultWeight`                             | This parameter value must be an integer value in the form of a `string`. This parameter represents the default weight for any hosts that have not been configured with the `roundRobinHostWeightPairs` parameter. For example, if a connection were already established and host weights were set with `roundRobinHostWeightPairs` but a new reader node was added to the database, the new reader node would use the default weight. <br><br> **Note:** This value must be an integer greater than or equal to 1.                                                | `1`           |

## Limitations

### General plugin limitations

When a Statement or ResultSet is created, it is internally bound to the database connection established at that moment. There is no standard JDBC functionality to change the internal connection used by Statement or ResultSet objects. Consequently, even if the read/write plugin switches the internal connection, any Statements/ResultSets created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after switching between the writer/reader.

### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read/write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

- autocommit value
- transaction isolation level

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.


### Limitations when using Spring Boot/Framework

#### @Transactional(readOnly = True)

> [!WARNING]\
> The use of read/write splitting with the annotation @Transactional(readOnly = True) is not recommended.

When a method with this annotation is hit, Spring calls conn.setReadOnly(true), executes the method, and then calls setReadOnly(false) to restore the connection's initial readOnly value. Consequently, every time the method is called, the plugin switches to the reader, executes the method, and then switches back to the writer. Although the reader connection will be cached after the first setReadOnly call, there is still some overhead when switching between the cached writer/reader connections. This constant switching is not an ideal use of the plugin because it is frequently incurring this overhead. The suggested approach for this scenario is to avoid loading the read/write splitting plugin and instead use the writer cluster URL for your write operations and the reader cluster URL for your read operations. By doing this you avoid the overhead of constantly switching between connections while still spreading load across the database instances in your cluster. 

#### Internal connection pools

We recommend that you do not enable internal connection pools when using Spring. This is because Spring by default uses its own external connection pool. The use of both internal and external pools is not tested and may result in problematic behavior.
