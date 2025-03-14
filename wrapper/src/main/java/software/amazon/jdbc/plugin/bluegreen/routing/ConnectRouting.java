package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;

public interface ConnectRouting {
  boolean isMatch(HostSpec hostSpec, BlueGreenRole hostRole);
  Connection apply(
      ConnectionPlugin plugin,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc,
      PluginService pluginService) throws SQLException;
}
