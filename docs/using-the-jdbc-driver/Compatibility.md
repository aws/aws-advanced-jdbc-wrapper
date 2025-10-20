# Plugins compatibility

The AWS JDBC Driver uses plugins to execute JDBC methods. You can think of a plugin as an extensible code module that adds additional logic around JDBC method calls. Plugins are designed with the intention of being compatible with each other; however, there are logical constraints related to database type or database features that can make plugins inefficient in certain configurations.

For example, RDS Single-AZ Instance deployments do not support failover, so the `failover` and `failover2` plugins are marked as incompatible. If either of these plugins is included in the driver configuration, there will be no added value. However, these unnecessary plugins will function without errors and will simply consume additional resources.

The following matrices help verify plugin compatibility with other plugins and with various database types. Some plugins are sensitive to the database URL provided in the connection string, and this is also presented below.

We encourage users to verify their configurations and ensure that their configuration contains no incompatible components.

- [Database type compatibility](./CompatibilityDatabaseTypes.md)
- [Database URL type compatibility](./CompatibilityEndpoints.md)
- [Cross Plugins compatibility](./CompatibilityCrossPlugins.md)



