## Plugin Service
<div style="center"><img src="../images/plugin_service.png" alt="diagram for the plugin service design"/></div>

The plugin service retrieves and updates the current connection and its relevant host information.

It also keeps track of the host list provider in use, and notifies it to update its host list.

It is expected that the plugins do not establish a JDBC connection themselves, but rather call `PluginService.connect()`
to establish connections.

## Host List Providers

The plugin service uses the host list provider to retrieve the most recent host information or topology information about the database.

The JDBC Driver has two host list providers, the `ConnectionStringHostListProvider` and the `AuroraHostListPovider`.

The `ConnectionStringHostListProvider` is the default provider, it parses the connection string for cluster information and stores the information.
The provider supports having multiple hosts with the same port in the connection string:

| Connection String                                                    | Support            |
|----------------------------------------------------------------------| ------------------ |
| `jdbc:aws-wrapper:postgresql://hostname1,hostname2:8090/testDB`      | :white_check_mark: |
| `jdbc:aws-wrapper:postgresql://hostname1:8030,hostname2:8090/testDB` | :x:                |

The `AuroraHostListProvider` provides information of the Aurora cluster.
It uses the current connection to track the available hosts and their roles in the cluster.

The `ConnectionStringHostListProvider` is a static host list provider, whereas the `AuroraHostListProvider` is a dynamic host list provider.
A static host list provider will fetch the host list during initialization and does not update the host list afterwards,
whereas a dynamic host list provider will update the host list information based on database status.
When implementing a custom host list provider, implement either the `StaticHostListProvider` or the `DynamicHostListProvider` marker interfaces to specify its provider type.
