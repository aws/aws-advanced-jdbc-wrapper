# Driver Metadata Connection Plugin

The AWS Advanced JDBC Wrapper is meant to be used with an underlying target driver, but when the user application calls `DatabaseMetaData#getDriverName`, the wrapper will return `Amazon Web Services (AWS) Advanced JDBC Wrapper` instead of the underlying driver's name.

In some cases, user application may require the underlying driver's name for some driver-specific logic that can't be determined by specifying a dialect.
For instance, Hibernate ORM explicitly checks for the driver name to enable native handling for advanced PostgreSQL data types.
The Driver Metadata Connection Plugin allows user application to override the wrapper's name to address these scenarios.

## Enabling the Driver Metadata Connection Plugin

To enable the Driver Metadata Connection Plugin, add the plugin code `driverMetaData` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value, or to the current [driver profile](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters).

## Driver Metadata Connection Plugin Parameters

| Parameter                | Value  | Required | Description                                             | Example            | Default Value |
|--------------------------|:------:|:--------:|:--------------------------------------------------------|:-------------------|---------------|
| `wrapperDriverName` | String |   Yes    | Override this value to return a specific driver name for the DatabaseMetaData#getDriverName method. | `CustomDriverName` | `Amazon Web Services (AWS) Advanced JDBC Wrapper`        |

### Example
[DriverMetaDataConnectionPluginExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/DriverMetaDataConnectionPluginExample.java)
demonstrates how user application can override the `getDriverName` method to return the underlying driver's product name instead.
