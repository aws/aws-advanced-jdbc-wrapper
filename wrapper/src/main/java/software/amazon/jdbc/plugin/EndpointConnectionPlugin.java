package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSelector;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class EndpointConnectionPlugin extends AbstractConnectionPlugin {

  protected static final List<HostSpec> cachedHosts = Arrays.asList(
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("scratch")
          .port(5432)
          .role(HostRole.WRITER)
          .availability(HostAvailability.AVAILABLE)
          .weight(3)
          .build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("scratch")
          .port(5432)
          .role(HostRole.WRITER)
          .availability(HostAvailability.AVAILABLE)
          .weight(1)
          .build(),
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("scratch")
          .port(5432)
          .role(HostRole.WRITER)
          .availability(HostAvailability.AVAILABLE)
          .weight(2)
          .build()
  );

  protected final PluginService pluginService;

  private final ConnectionProviderManager connProviderManager;
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(EndpointConnectionPlugin.class);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public EndpointConnectionPlugin (final PluginService pluginService) {
    this.pluginService = pluginService;
    this.connProviderManager = new ConnectionProviderManager(pluginService.getConnectionProvider());
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, connectFunc);
  }

  @Override
  public Connection forceConnect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, forceConnectFunc);
  }

  private Connection connectInternal(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final JdbcCallable<Connection,
          SQLException> connectFunc) throws SQLException {

    HostSelector selector = new HighestWeightHostSelector();
    final HostSpec selectedHostSpec = selector.getHost(cachedHosts, HostRole.WRITER, props);
    final Connection conn = connProviderManager.getConnectionProvider(
        this.pluginService.getDriverProtocol(),
        selectedHostSpec,
        props
    ).connect(
        driverProtocol,
        this.pluginService.getDialect(),
        this.pluginService.getTargetDriverDialect(),
        selectedHostSpec,
        props
    );
    return conn;
  }
}
