# Global Database (GDB) Read/Write Splitting Plugin

The GDB read/write splitting plugin extends the functionality of the [read/write splitting plugin](./UsingTheReadWriteSplittingPlugin.md) and adopts some additional settings to improve support for Global Databases.

The GDB read/write splitting plugin adds the notion of a home region and allows users to constrain new connections to this region. Such restrictions may be helpful to prevent opening new connections in environments where remote AWS regions add substantial latency that cannot be tolerated. 

Unless otherwise stated, all recommendations, configurations and code examples made for the [read/write splitting plugin](./UsingTheReadWriteSplittingPlugin.md) are applicable to the current GDB read/write splitting plugin.

## Plugin Availability
The plugin is available since version 3.2.0.

## Loading the Read/Write Splitting Plugin

The GDB read/write splitting plugin is not loaded by default. To load the plugin, include it in the `wrapperPlugins` connection parameter. If you would like to load the GDB read/write splitting plugin alongside the failover and host monitoring plugins, the read/write splitting plugin must be listed before these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the read/write splitting plugin with these plugins.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "gdbReadWriteSplitting,failover2,efm2");
```

If you would like to use the GDB read/write splitting plugin without the failover plugin, make sure you have the `gdbReadWriteSplitting` plugin in the `wrapperPlugins` property, and that the failover plugin is not part of it.
```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "gdbReadWriteSplitting");
```

> [!WARNING]
> Do not use the `readWriteSplitting`, `srw` and/or `gdbReadWriteSplitting` plugins (or their combination) at the same time for the same connection!

## Using the GDB Read/Write Splitting Plugin against non-GDB clusters

The GDB read/write splitting plugin can be used against Aurora clusters and RDS clusters. However, since these cluster types are single-region clusters, setting a home region does not make much sense. 

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Configuration Parameters

| Parameter                        |  Value  |                                                                  Required                                                                   | Description                                                                                                                                                                                                                                                                                                                                                                                            | Default Value                                                                                                                    |
|----------------------------------|:-------:|:-------------------------------------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `readerHostSelectorStrategy`     | String  |                                                                     No                                                                      | The name of the strategy that should be used to select a new reader host.                                                                                                                                                                                                                                                                                                                              | `random`                                                                                                                         |
| `cachedReaderKeepAliveTimeoutMs` | Integer |                                                                     No                                                                      | The time in milliseconds to keep a reader connection alive in the cache. Default value 0 means the plugin will keep reusing the same cached reader connection.                                                                                                                                                                                                                                         | 0                                                                                                                                | 
| `gdbRwHomeRegion`                | String  | If connecting using an IP address, a custom domain URL, Global Database endpoint or other endpoint with no region: Yes<br><br>Otherwise: No | Defines a home region.<br><br>Examples: `us-west-2`, `us-east-1`. <br><br>If this parameter is omitted, the value is parsed from the connection URL. For regional cluster endpoints and instance endpoints, it's set to the region of the provided endpoint. If the provided endpoint has no region (for example, a Global Database endpoint or IP address), the configuration parameter is mandatory. | For regional cluster endpoints and instance endpoints, it's set to the region of the provided endpoint.<br><br>Otherwise: `null` |
| `gdbRwStayHomeRegionForWriter`   | Boolean |                                                                     No                                                                      | If set to `true`, prevents following and connecting to a writer node outside the defined home region. An exception will be raised when such a connection to a writer outside the home region is requested.                                                                                                                                                                                             | `false`                                                                                                                          |
| `gdbRwStayHomeRegionForReader`   | Boolean |                                                                     No                                                                      | If set to `true`, prevents connecting to a reader node outside the defined home region. If no reader nodes in the home region are available, an exception will be raised.                                                                                                                                                                                                                              | `false`                                                                                                                          |
|


Please refer to the original [read/write splitting plugin](./UsingTheReadWriteSplittingPlugin.md) for more details about error codes, configurations, connection pooling and sample codes. 
