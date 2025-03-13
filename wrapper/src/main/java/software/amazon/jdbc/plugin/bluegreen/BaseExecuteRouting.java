package software.amazon.jdbc.plugin.bluegreen;

import java.util.Optional;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;

public abstract class BaseExecuteRouting implements ExecuteRouting {

  protected final String hostAndPort;
  protected BlueGreenRole role;

  public BaseExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    this.hostAndPort = hostAndPort;
    this.role = role;
  }

  @Override
  public boolean isMatch(HostSpec hostSpec, BlueGreenRole hostRole) {
    return (this.hostAndPort == null || this.hostAndPort.equals(hostSpec == null ? null : hostSpec.getHostAndPort())
        && (this.role == null || this.role == hostRole));
  }

  @Override
  public abstract <T, E extends Exception> @NonNull Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final PluginService pluginService,
      final Properties props) throws E;

  @Override
  public String toString() {
    return String.format("%s [%s, %s]",
        super.toString(),
        this.hostAndPort == null ? "<null>" : this.hostAndPort,
        this.role == null ? "<null>" : this.role.toString());
  }
}
