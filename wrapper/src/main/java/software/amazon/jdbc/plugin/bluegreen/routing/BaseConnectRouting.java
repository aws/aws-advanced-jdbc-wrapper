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
import software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPlugin;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;

public abstract class BaseConnectRouting extends BaseRouting implements ConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenConnectionPlugin.class.getName());

  protected final String hostAndPort;
  protected final BlueGreenRole role;

  public BaseConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    this.hostAndPort = hostAndPort == null ? null : hostAndPort.toLowerCase();
    this.role = role;
  }

  @Override
  public boolean isMatch(HostSpec hostSpec, BlueGreenRole hostRole) {
    return (this.hostAndPort == null || this.hostAndPort.equals(
                hostSpec == null ? null : hostSpec.getHostAndPort().toLowerCase()))
        && (this.role == null || this.role.equals(hostRole));
  }

  @Override
  public abstract Connection apply(
      ConnectionPlugin plugin,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc,
      PluginService pluginService)
      throws SQLException;

  @Override
  public String toString() {
    return String.format("%s [%s, %s]",
        super.toString(),
        this.hostAndPort == null ? "<null>" : this.hostAndPort,
        this.role == null ? "<null>" : this.role.toString());
  }
}
