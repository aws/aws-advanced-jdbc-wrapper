package software.amazon.jdbc.plugin.bluegreen.routing;

import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;

// Normally execute JDBC call.
public class RegularExecuteRouting extends BaseExecuteRouting {

  private static final Logger LOGGER = Logger.getLogger(RegularExecuteRouting.class.getName());

  public RegularExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public <T, E extends Exception> @NonNull Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final PluginService pluginService,
      final Properties props) throws E {

    return Optional.of(jdbcMethodFunc.call());
  }
}
