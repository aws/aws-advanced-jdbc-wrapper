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

package software.amazon.jdbc.plugin.customendpoint;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class CustomEndpointPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  private static final String TELEMETRY_CUSTOM_ENDPOINT_COUNTER = "customEndpoint.endpointConnections.counter";
  private static final String TELEMETRY_WAIT_FOR_INFO_COUNTER = "customEndpoint.waitForInfo.counter";

  protected static final long CACHE_CLEANUP_RATE_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final SlidingExpirationCacheWithCleanupThread<String, CustomEndpointMonitor> monitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          CustomEndpointMonitor::shouldDispose,
          (monitor) -> {
            try {
              monitor.close();
            } catch (Exception ex) {
              // ignore
            }
          },
          CACHE_CLEANUP_RATE_NANO);

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
          add("connect");
          add("forceConnect");
        }
      });

  public static final AwsWrapperProperty CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS = new AwsWrapperProperty(
      "customEndpointInfoRefreshRateMs", "30000",
      "Controls how frequently custom endpoint monitors fetch custom endpoint info.");

  public static final AwsWrapperProperty WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS = new AwsWrapperProperty(
      "waitForCustomEndpointInfoTimeoutMs", "5000",
      "Controls the maximum amount of time that the plugin will wait for custom endpoint info to be "
          + "populated in the cache.");

  public static final AwsWrapperProperty CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS = new AwsWrapperProperty(
      "customEndpointMonitorExpirationMs", String.valueOf(TimeUnit.MINUTES.toMillis(15)),
      "Controls how long a monitor should run without use before expiring and being removed.");

  public static final AwsWrapperProperty REGION_PROPERTY = new AwsWrapperProperty(
      "customEndpointRegion", null,
      "The region of the cluster's custom endpoints.");

  static {
    PropertyDefinition.registerPluginProperties(CustomEndpointPlugin.class);
  }

  protected final PluginService pluginService;
  protected final Properties props;
  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final BiFunction<HostSpec, Region, RdsClient> rdsClientFunc;

  private final TelemetryFactory telemetryFactory;
  private final TelemetryCounter endpointConnectionsCounter;
  private final TelemetryCounter waitForInfoCounter;

  protected final int waitOnCachedInfoDurationMs;
  protected final int idleMonitorExpirationMs;
  protected HostSpec customEndpointHostSpec;

  public CustomEndpointPlugin(final PluginService pluginService, final Properties props) {
    this(
        pluginService,
        props,
        (hostSpec, region) ->
            RdsClient.builder()
                .region(region)
                .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props))
                .build());
  }

  public CustomEndpointPlugin(
      final PluginService pluginService,
      final Properties props,
      final BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    this.pluginService = pluginService;
    this.props = props;
    this.rdsClientFunc = rdsClientFunc;

    this.waitOnCachedInfoDurationMs = WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.getInteger(this.props);
    this.idleMonitorExpirationMs = CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS.getInteger(this.props);

    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.endpointConnectionsCounter = telemetryFactory.createCounter(TELEMETRY_CUSTOM_ENDPOINT_COUNTER);
    this.waitForInfoCounter = telemetryFactory.createCounter(TELEMETRY_WAIT_FOR_INFO_COUNTER);
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
    return connectInternal(hostSpec, props, isInitialConnection, connectFunc);
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(hostSpec, props, isInitialConnection, forceConnectFunc);
  }

  protected Connection connectInternal(
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    if (!this.rdsUtils.isRdsCustomClusterDns(hostSpec.getHost())) {
      return connectFunc.call();
    }

    this.customEndpointHostSpec = hostSpec;
    this.endpointConnectionsCounter.inc();
    LOGGER.finest(
        Messages.get(
            "CustomEndpointPlugin.connectionRequestToCustomEndpoint", new Object[]{ hostSpec.getHost() }));

    monitors.computeIfAbsent(
        this.customEndpointHostSpec.getHost(),
        (customEndpoint) -> new CustomEndpointMonitorImpl(
            this.pluginService,
            this.customEndpointHostSpec,
            props,
            this.rdsClientFunc
        ),
        TimeUnit.MILLISECONDS.toNanos(this.idleMonitorExpirationMs)
    );

    // If needed, wait a short time for custom endpoint info to be discovered.
    boolean isInfoInCache = waitForCustomEndpointInfo();

    if (!isInfoInCache) {
      throw new SQLException(
          Messages.get("CustomEndpointPlugin.cacheTimeout",
              new Object[]{this.waitOnCachedInfoDurationMs, this.customEndpointHostSpec.getHost()}));
    }

    return connectFunc.call();
  }

  private boolean waitForCustomEndpointInfo() {
    CustomEndpointInfo cachedInfo =
        this.pluginService.getStatus(this.customEndpointHostSpec.getHost(), CustomEndpointInfo.class, true);
    boolean isInfoInCache = cachedInfo != null;

    if (!isInfoInCache) {
      // Wait for the monitor to place the custom endpoint info in the cache. This ensures other plugins get accurate
      // custom endpoint info.
      this.waitForInfoCounter.inc();
      LOGGER.fine(
          Messages.get(
              "CustomEndpointPlugin.waitingforCustomEndpointInfo",
              new Object[]{ this.customEndpointHostSpec.getHost(), this.waitOnCachedInfoDurationMs }));
      long waitForEndpointInfoTimeoutNano =
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.waitOnCachedInfoDurationMs);

      while (!isInfoInCache && System.nanoTime() < waitForEndpointInfoTimeoutNano) {
        cachedInfo =
            this.pluginService.getStatus(this.customEndpointHostSpec.getHost(), CustomEndpointInfo.class, true);
        isInfoInCache = cachedInfo != null;
      }

      if (isInfoInCache) {
        LOGGER.fine(
            Messages.get(
                "CustomEndpointPlugin.foundInfoInCache",
                new Object[]{ this.customEndpointHostSpec.getHost(), cachedInfo }));
      }
    }

    return isInfoInCache;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {
    if (this.customEndpointHostSpec == null) {
      return jdbcMethodFunc.call();
    }

    monitors.computeIfAbsent(
        this.customEndpointHostSpec.getHost(),
        (customEndpoint) -> new CustomEndpointMonitorImpl(
            this.pluginService,
            this.customEndpointHostSpec,
            props,
            this.rdsClientFunc
        ),
        TimeUnit.MILLISECONDS.toNanos(this.idleMonitorExpirationMs)
    );

    // If needed, wait a short time for custom endpoint info to be discovered.
    boolean isInfoInCache = waitForCustomEndpointInfo();

    if (!isInfoInCache) {
      SQLException cacheTimeoutException = new SQLException(
          Messages.get("CustomEndpointPlugin.cacheTimeout",
              new Object[]{this.waitOnCachedInfoDurationMs, this.customEndpointHostSpec.getHost()}));
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, cacheTimeoutException);
    }

    return jdbcMethodFunc.call();
  }

  public static void clearCache() {
    LOGGER.info(Messages.get("CustomEndpointPlugin.closingMonitors"));

    for (CustomEndpointMonitor monitor : monitors.getEntries().values()) {
      try {
        monitor.close();
      } catch (Exception ex) {
        // ignore
      }
    }

    monitors.clear();
  }
}
