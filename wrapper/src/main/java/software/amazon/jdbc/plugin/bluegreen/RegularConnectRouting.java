package software.amazon.jdbc.plugin.bluegreen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;

public class RegularConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(RegularConnectRouting.class.getName());

  public RegularConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public Connection apply(ConnectionPlugin plugin, HostSpec hostSpec, Properties props, boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc, PluginService pluginService)
      throws SQLException {

    return connectFunc.call();
  }
}
