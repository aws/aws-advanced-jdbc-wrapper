package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;

public class RejectConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(RejectConnectRouting.class.getName());

  public RejectConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public Connection apply(ConnectionPlugin plugin, HostSpec hostSpec, Properties props, boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc, PluginService pluginService) throws SQLException {

    LOGGER.finest("Blue/Green Deployment switchover is in progress. New connection can't be opened.");
    throw new SQLException("Blue/Green Deployment switchover is in progress. New connection can't be opened.");
  }
}
