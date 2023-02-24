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

### Limitations

#### General plugin limitations

When a Statement or ResultSet is created, it is internally bound to the database connection established at that moment. There is no standard JDBC functionality to change the internal connection used by Statement or ResultSet objects. Consequently, even if the read-write plugin switches the internal connection, any Statements/ResultSets created before this will continue using the old database connection. This bypasses the desired functionality provided by the plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after switching between the writer/reader.

#### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read-write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

- autocommit value
- transaction isolation level

All other session state attributes will be lost when switching connections between the writer/reader.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes each time that you switch between the writer/reader.
