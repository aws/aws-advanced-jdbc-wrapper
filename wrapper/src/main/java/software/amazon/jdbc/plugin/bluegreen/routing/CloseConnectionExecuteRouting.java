package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.util.WrapperUtils;

// Close current connection.
public class CloseConnectionExecuteRouting extends BaseExecuteRouting {

  private static final Logger LOGGER = Logger.getLogger(CloseConnectionExecuteRouting.class.getName());

  public CloseConnectionExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public <T, E extends Exception> Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final PluginService pluginService,
      final Properties props) throws E {

    try {
      if (pluginService.getCurrentConnection() != null
          && !pluginService.getCurrentConnection().isClosed()) {
        pluginService.getCurrentConnection().close();
      }

      throw new SQLException("Connection has been closed since Blue/Green switchover is in progress.", "08001");

    } catch (SQLException ex) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
    }
  }
}
