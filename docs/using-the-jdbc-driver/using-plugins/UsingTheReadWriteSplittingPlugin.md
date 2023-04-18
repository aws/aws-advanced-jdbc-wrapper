## Read-Write Splitting Plugin

The read-write splitting plugin adds functionality to switch between writer/reader instances via calls to the `Connection#setReadOnly` method. Upon calling `setReadOnly(true)`, the plugin will establish a connection to a random reader instance and direct subsequent queries to this instance. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the boolean argument you supply to the `setReadOnly` method.

### Loading the Read-Write Splitting Plugin

The read-write splitting plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter. If you would like to load the read-write splitting plugin alongside the failover and host monitoring plugins, the read-write splitting plugin must be placed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the read-write splitting plugin alongside these plugins.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm");
```

If you would like to use the read-write splitting plugin without the failover plugin against an Aurora cluster, you will need to include the Aurora host list plugin before the read-write splitting plugin. This informs the driver that it should query for Aurora's topology.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
```

### Using the Read-Write Splitting Plugin against RDS/Aurora clusters

When using the read-write splitting plugin against RDS or Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply just the URL for the initial instance to which you're connecting. You must also include either the failover plugin or the Aurora host list plugin in your plugin chain so that the driver knows to query Aurora for its topology. See the section on [loading the read-write splitting plugin](#loading-the-read-write-splitting-plugin) for more info.

### Using the Read-Write Splitting Plugin against non-RDS clusters

If you are using the read-write splitting plugin against a cluster that is not hosted on RDS or Aurora, the plugin will not be able to automatically acquire the cluster topology. Instead, you must supply the topology information in the connection string as a comma-delimited list of multiple instance URLs. If you are using a single writer instance, the first instance in the list must be the writer instance and you must enable the `singleWriterConnectionString` property:

```
properties.setProperty(ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.name, "true");
String connectionUrl = "jdbc:aws-wrapper:mysql://writer-instance-1.com,reader-instance-1.com,reader-instance-2.com/database-name"
```

Additionally, you should avoid using the Aurora host list plugin and the failover plugin in this scenario, as they are designed to specifically operate against Aurora databases.

### Internal connection pooling

Whenever `setReadOnly(true)` is first called on a `Connection` object, the read-write plugin will internally open a new physical connection to a reader. After this first call, the physical reader connection will be cached for the given `Connection`. Future calls to `setReadOnly `on the same `Connection` object will not require opening a new physical connection. However, calling `setReadOnly(true)` for the first time on a new `Connection` object will require the plugin to establish another new physical connection to a reader. If your application frequently establishes new read-only connections, you can enable internal connection pooling to improve performance. When enabled, the wrapper driver will maintain an internal connection pool for each instance in the cluster. This allows the read-write plugin to reuse reader connections that were established by previous `Connection` objects.

The wrapper driver currently uses [Hikari](https://github.com/brettwooldridge/HikariCP) to create and maintain its internal connection pools. The sample code [here](../../../examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) provides a useful example of how to enable this feature. The steps are as follows:

1.  Create an instance of `HikariPooledConnectionProvider`. The `HikariPooledConnectionProvider` constructor requires you to pass in a `HikariPoolConfigurator` function. Inside this function, you should create a `HikariConfig`, configure any desired properties on it, and return it. Note that the Hikari properties below will be set by default and will override any values you set in your function. This is done to follow desired behavior and ensure that the read-write plugin can internally establish connections to new instances.

- jdbcUrl (including the host, port, and database)
- exception override class name
- username
- password

You can optionally pass in a `HikariPoolMapping` function as a second parameter to the `HikariPooledConnectionProvider`. Internally, the connection pools used by the plugin are maintained as a map from instance URLs to connection pools. If you would like to define a different key system, you should pass in a `HikariPoolMapping` function defining this logic. This is helpful, for example, when you would like to create multiple Connection objects to the same instance with different users. In this scenario, you should pass in a `HikariPoolMapping` that incorporates the instance URL and the username from the `Properties` object into the map key.

2. Call `ConnectionProviderManager.setConnectionProvider`, passing in the `HikariPooledConnectionProvider` you created in step 1.

3. Continue as normal: create connections and use them as needed.

4. When you are finished using all connections, call `ConnectionProviderManager.releaseResources`.

> :warning: **Note:** You must call `ConnectionProviderManager.releaseResources` to close the internal connection pools when you are finished using all connections. Unless `ConnectionProviderManager.releaseResources` is called, the wrapper driver will keep the pools open so that they can be shared between connections.

### Example
[ReadWriteSplittingPostgresExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/ReadWriteSplittingPostgresExample.java) demonstrates how to enable and configure read-write splitting with the Aws Advanced JDBC Driver.

### Limitations

#### General plugin limitations

When a Statement or ResultSet is created, it is internally bound to the database connection established at that moment. There is no standard JDBC functionality to change the internal connection used by Statement or ResultSet objects. Consequently, even if the read-write plugin switches the internal connection, any Statements/ResultSets created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after switching between the writer/reader.

#### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read-write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

- autocommit value
- transaction isolation level

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.
