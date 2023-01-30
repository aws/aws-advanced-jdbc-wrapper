## Read-Write Splitting Plugin

The read-write splitting plugin adds functionality to switch between writer/reader instances via calls to the `Connection#setReadOnly` method. Upon calling `setReadOnly(true)`, the plugin will establish a connection to a random reader instance and direct subsequent queries to this instance. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the argument you supply to the `setReadOnly` method.

### Loading the Read-Write Splitting Plugin

The read-write splitting plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter:
```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "readWriteSplitting,failover,efm");
```

If you would like to load the read-write splitting plugin alongside the failover and host monitoring plugins, the read-write splitting plugin must be placed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example above to properly load the read-write splitting plugin alongside these plugins.

If you would like to use the read-write splitting plugin without the failover plugin against an Aurora cluster, you will need to include the Aurora host list plugin before the read-write splitting plugin. This informs the driver that it should query for Aurora's topology.
```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
```

### Reader load balancing

The plugin can also load balance queries among available reader instances by enabling the `loadBalanceReadOnlyTraffic` connection parameter. This parameter is disabled by default. To enable it, set the following connection parameter:
```
properties.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");
```

Once this parameter is enabled and `setReadOnly(true)` has been called, the plugin will load balance readers while autocommit is off. The reader switch will occur at each transaction boundary. The following scenarios are considered transaction boundaries:
- After calling `commit()` or `rollback()`
- After executing `COMMIT` or `ROLLBACK` as a SQL statement (see limitations [here](#multi-statement-sql-limitations-with-reader-load-balancing))

By default, the plugin will not load balance queries while autocommit is on, even if `loadBalanceReadOnlyTraffic` is enabled. To enable load balancing with autocommit on, set `loadBalanceReadOnlyTraffic` to true and `readerBalanceAutoCommitStatementLimit` to a positive value X. The plugin will then load balance after every X statements executed in read-only mode with autocommit on. If you would like to load balance after every X statements matching a custom regex, set the `readerBalanceAutoCommitStatementRegex` parameter as well.

### Using the Read-Write Splitting Plugin against RDS/Aurora clusters

When using the read-write splitting plugin against RDS or Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply just the URL for the initial instance to which you're connecting. You must also include either the failover plugin or the Aurora host list plugin in your plugin chain so that the driver knows to query Aurora for its topology. See the section on [loading the read-write splitting plugin](#loading-the-read-write-splitting-plugin) for more info.

### Using the Read-Write Splitting Plugin against non-RDS clusters

If you are using the read-write splitting plugin against a cluster that is not hosted on RDS or Aurora, the plugin will not be able to automatically acquire the cluster topology. Instead, you must supply the topology information in the connection string as a comma-delimited list of multiple instance URLs. If you are using a single writer instance, the first instance in the list must be the writer instance and you must enable the `singleWriterConnectionString` property:
```
properties.setProperty(ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.name, "true");
String connectionUrl = "jdbc:aws-wrapper:mysql://writer-instance-1.com,reader-instance-1.com,reader-instance-2.com/database-name"
```

Additionally, you should avoid using the Aurora host list plugin and the failover plugin in this scenario, as they are designed to operate against Aurora databases only.

### Limitations

#### General plugin limitations

When a Statement or ResultSet object is created, it is internally bound to the physical database connection established at that moment. There is no standard JDBC functionality to change the underlying connection used by Statement or ResultSet objects. Consequently, even if the read-write plugin switches the internal connection, any Statements/ResultSets created before this event will continue using the old database connection. This  bypasses any desired functionality provided by the read-write plugin. To prevent these scenarios, an exception will be thrown if your code uses any Statements/ResultSets created before a change in internal connection. To solve this problem, please ensure you create new Statement/ResultSet objects after each change in internal connection. See the section on [scenarios that cause a change in internal connection](#scenarios-that-cause-a-change-in-internal-connection) for more info.

#### Multi-statement SQL limitations with reader load balancing

When reader load balancing is enabled and autocommit is off, the read-write splitting plugin will analyze SQL statements executed to determine if `COMMIT` or `ROLLBACK` are executed. In addition to `commit()` and `rollback()`, these statements indicate a transaction boundary. This analysis does not support SQL strings containing multiple statements. If your SQL strings use `COMMIT` or `ROLLBACK` and they contain multiple statements, we recommend that you do not enable reader load balancing as the plugin will not detect the transaction boundary if these keywords are not the first statement.

#### Session state limitations

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read-write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

- autocommit value
- transaction isolation level

All other session state attributes will be lost when switching connections. See the section on [scenarios that cause a change in internal connection](#scenarios-that-cause-a-change-in-internal-connection) for info on when the connection is switched.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes whenever the connection is switched as described in the [scenarios that cause a change in internal connection](#scenarios-that-cause-a-change-in-internal-connection) section. Since reader load balancing frequently switches the connection, we recommend that you keep it disabled if your workflow depends on session state attributes that are not automatically transferred.

### Scenarios that cause a change in internal connection

1. If you have loaded the plugin, the connection will switch between the writer/reader when calling `setReadOnly`.
2. If you have set the `loadBalanceReadOnlyTraffic` parameter to true, the connection will also switch at transaction boundaries while read-only is true and autocommit is off. By default, the plugin will not load balance queries while autocommit is on. See the section on [reader load balancing](#reader-load-balancing) for more information.
3. If you have set the `loadBalanceReadOnlyTraffic` parameter to true and the `readerBalanceAutoCommitStatementLimit` parameter to a positive value X, the connection will also switch after every X statements executed while autocommit is on and read-only is true. You can optionally set the `readerBalanceAutoCommitStatementRegex` parameter to switch after every X statements matching a given regex. See the section on [reader load balancing](#reader-load-balancing) for more information.

### Read Write Splitting Plugin Parameters

| Parameter                               | Value   | Required | Description                                                                                                                                                                                                                                                                                                                                                                  | Default Value | Default behavior                                                   |
|-----------------------------------------|---------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------------|
| `loadBalanceReadOnlyTraffic`            | Boolean | No       | Set to `true` to load balance queries among available reader instances when the connection has been set to read-only mode and autocommit-off mode. To load balance while autocommit is on, enable this parameter and set the `readerBalanceAutoCommitStatementLimit` parameter.                                                                                              | `false`       | Reader queries are not load balanced                               |
| `readerBalanceAutoCommitStatementLimit` | Integer | No       | Set to a positive value X to load balance reader instances after every X queries while autocommit is on. To use this value, the connection must also be in read-only mode and have `loadBalanceReadOnlyTraffic` enabled. To configure the plugin to switch after every X queries matching a custom regex, set the `readerBalanceAutoCommitStatementRegex` parameter as well. | `0`           | Reader query balancing is disabled while autocommit is on          |
| `readerBalanceAutoCommitStatementRegex` | String  | No       | Set to your desired regex R to load balance reader instances after every X queries matching R while autocommit is on, where X is defined by `readerBalanceAutoCommitStatementLimit`. To use this value, the connection must also be in read-only mode and have `loadBalanceReadOnlyTraffic` enabled and `readerBalanceAutoCommitStatementLimit` set to a positive value.     | `null`        | All executes count towards `readerBalanceAutoCommitStatementLimit` |

