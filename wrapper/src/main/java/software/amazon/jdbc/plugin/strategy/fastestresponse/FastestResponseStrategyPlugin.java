/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.strategy.fastestresponse;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.RandomHostSelector;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.CacheMap;

public class FastestResponseStrategyPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(FastestResponseStrategyPlugin.class.getName());

  public static final String FASTEST_RESPONSE_STRATEGY_NAME = "fastestResponse";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
          add("notifyNodeListChanged");
          add("acceptsStrategy");
          add("getHostSpecByStrategy");
        }
      });

  public static final AwsWrapperProperty RESPONSE_MEASUREMENT_INTERVAL_MILLIS =
      new AwsWrapperProperty(
          "responseMeasurementIntervalMs",
          "30000",
          "Interval in millis between measuring response time to a database node.");

  protected static final CacheMap<String, HostSpec> cachedFastestResponseHostByRole = new CacheMap<>();
  protected static final RandomHostSelector randomHostSelector = new RandomHostSelector();

  protected final @NonNull PluginService pluginService;
  protected final @NonNull Properties properties;
  protected final @NonNull HostResponseTimeService hostResponseTimeService;
  protected long cacheExpirationNano;

  protected List<HostSpec> hosts = new ArrayList<>();

  static {
    PropertyDefinition.registerPluginProperties(FastestResponseStrategyPlugin.class);
    PropertyDefinition.registerPluginProperties("frt-");
  }

  public FastestResponseStrategyPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService,
        properties,
        new HostResponseTimeServiceImpl(
            pluginService,
            properties,
            RESPONSE_MEASUREMENT_INTERVAL_MILLIS.getInteger(properties)));
  }

  public FastestResponseStrategyPlugin(
      final PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull HostResponseTimeService hostResponseTimeService) {

    this.pluginService = pluginService;
    this.properties = properties;
    this.hostResponseTimeService = hostResponseTimeService;
    this.cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        RESPONSE_MEASUREMENT_INTERVAL_MILLIS.getInteger(this.properties));
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    Connection conn = connectFunc.call();
    if (isInitialConnection) {
      this.hostResponseTimeService.setHosts(this.pluginService.getHosts());
    }
    return conn;
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    return FASTEST_RESPONSE_STRATEGY_NAME.equalsIgnoreCase(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(final HostRole role, final String strategy)
      throws SQLException, UnsupportedOperationException {

    if (!acceptsStrategy(role, strategy)) {
      return null;
    }

    // The cache holds a host with the fastest response time.
    // If cache doesn't have a host for a role, it's necessary to find the fastest node in the topology.
    final HostSpec fastestResponseHost = cachedFastestResponseHostByRole.get(role.name());

    if (fastestResponseHost != null) {
      // Found a fastest host. Let find it in the latest topology.
      HostSpec foundHostSpec = this.pluginService.getHosts().stream()
          .filter(x -> x.equals(fastestResponseHost))
          .findAny()
          .orElse(null);

      if (foundHostSpec != null) {
        // Found a host in the topology.
        return foundHostSpec;
      }

      // It seems that the fastest cached host isn't in the latest topology.
      // Let's ignore cached results and find the fastest host.
    }

    // Cached result isn't available. Need to find the fastest response time host.

    final HostSpec calculatedFastestResponseHost = this.pluginService.getHosts().stream()
        .filter(x -> role.equals(x.getRole()))
        .map(x -> new ResponseTimeTuple(x, this.hostResponseTimeService.getResponseTime(x)))
        .sorted(Comparator.comparingInt(x -> x.responseTime))
        .map(x -> x.hostSpec)
        .findFirst()
        .orElse(null);

    if (calculatedFastestResponseHost == null) {
      // Unable to identify the fastest response host.
      // As a last resort, let's use a random host selector.
      return randomHostSelector.getHost(this.hosts, role, properties);
    }

    cachedFastestResponseHostByRole.put(role.name(), calculatedFastestResponseHost, this.cacheExpirationNano);

    return calculatedFastestResponseHost;
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {
    this.hosts = this.pluginService.getHosts();
    this.hostResponseTimeService.setHosts(this.hosts);
  }

  public static void clearCache() {
    cachedFastestResponseHostByRole.clear();
  }

  private static class ResponseTimeTuple {
    public HostSpec hostSpec;
    public int responseTime;

    public ResponseTimeTuple(final HostSpec hostSpec, int responseTime) {
      this.hostSpec = hostSpec;
      this.responseTime = responseTime;
    }
  }
}
