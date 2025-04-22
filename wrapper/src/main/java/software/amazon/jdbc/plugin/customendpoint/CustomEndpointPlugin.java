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
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * A plugin that analyzes custom endpoints for custom endpoint information and custom endpoint changes, such as adding
 * or removing an instance in the custom endpoint.
 */
public class CustomEndpointPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  private static final String TELEMETRY_WAIT_FOR_INFO_COUNTER = "customEndpoint.waitForInfo.counter";

  protected static final long CACHE_CLEANUP_RATE_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final RegionUtils regionUtils = new RegionUtils();
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
        }
      });

  public static final AwsWrapperProperty CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS = new AwsWrapperProperty(
      "customEndpointInfoRefreshRateMs", "30000",
      "Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds.");

  public static final AwsWrapperProperty WAIT_FOR_CUSTOM_ENDPOINT_INFO = new AwsWrapperProperty(
      "waitForCustomEndpointInfo", "true",
      "Controls whether to wait for custom endpoint info to become available before connecting or executing a "
          + "method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used "
          + "recently. Note that disabling this may result in occasional connections to instances outside of the "
          + "custom endpoint.");

  public static final AwsWrapperProperty WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS = new AwsWrapperProperty(
      "waitForCustomEndpointInfoTimeoutMs", "5000",
      "Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made "
          + "available by the custom endpoint monitor, in milliseconds.");

  public static final AwsWrapperProperty CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS = new AwsWrapperProperty(
      "customEndpointMonitorExpirationMs", String.valueOf(TimeUnit.MINUTES.toMillis(15)),
      "Controls how long a monitor should run without use before expiring and being removed, in milliseconds.");

  public static final AwsWrapperProperty REGION_PROPERTY = new AwsWrapperProperty(
      "customEndpointRegion", null,
      "The region of the cluster's custom endpoints. If not specified, the region will be parsed from the URL.");

  static {
    PropertyDefinition.registerPluginProperties(CustomEndpointPlugin.class);
  }

  protected final PluginService pluginService;
  protected final Properties props;
  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final BiFunction<HostSpec, Region, RdsClient> rdsClientFunc;

  protected final TelemetryCounter waitForInfoCounter;
  protected final boolean shouldWaitForInfo;
  protected final int waitOnCachedInfoDurationMs;
  protected final int idleMonitorExpirationMs;
  protected HostSpec customEndpointHostSpec;
  protected String customEndpointId;
  protected Region region;

  /**
   * Constructs a new CustomEndpointPlugin instance.
   *
   * @param pluginService The plugin service that the custom endpoint plugin should use.
   * @param props         The properties that the custom endpoint plugin should use.
   */
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

  /**
   * Constructs a new CustomEndpointPlugin instance.
   *
   * @param pluginService The plugin service that the custom endpoint plugin should use.
   * @param props         The properties that the custom endpoint plugin should use.
   * @param rdsClientFunc The function to call to obtain an {@link RdsClient} instance.
   */
  public CustomEndpointPlugin(
      final PluginService pluginService,
      final Properties props,
      final BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    this.pluginService = pluginService;
    this.props = props;
    this.rdsClientFunc = rdsClientFunc;

    this.shouldWaitForInfo = WAIT_FOR_CUSTOM_ENDPOINT_INFO.getBoolean(this.props);
    this.waitOnCachedInfoDurationMs = WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.getInteger(this.props);
    this.idleMonitorExpirationMs = CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS.getInteger(this.props);

    TelemetryFactory telemetryFactory = pluginService.getTelemetryFactory();
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
    if (!this.rdsUtils.isRdsCustomClusterDns(hostSpec.getHost())) {
      return connectFunc.call();
    }

    this.customEndpointHostSpec = hostSpec;
    LOGGER.finest(
        Messages.get(
            "CustomEndpointPlugin.connectionRequestToCustomEndpoint", new Object[]{ hostSpec.getHost() }));

    this.customEndpointId = this.rdsUtils.getRdsClusterId(customEndpointHostSpec.getHost());
    if (StringUtils.isNullOrEmpty(customEndpointId)) {
      throw new SQLException(
          Messages.get(
              "CustomEndpointPlugin.errorParsingEndpointIdentifier",
              new Object[] {customEndpointHostSpec.getHost()}));
    }

    this.region = regionUtils.getRegion(this.customEndpointHostSpec.getHost(), props, REGION_PROPERTY.name);
    if (this.region == null) {
      throw new SQLException(
          Messages.get(
              "CustomEndpointPlugin.unableToDetermineRegion",
              new Object[] {REGION_PROPERTY.name}));
    }

    CustomEndpointMonitor monitor = createMonitorIfAbsent(props);

    if (this.shouldWaitForInfo) {
      // If needed, wait a short time for custom endpoint info to be discovered.
      waitForCustomEndpointInfo(monitor);
    }

    return connectFunc.call();
  }

  /**
   * Creates a monitor for the custom endpoint if it does not already exist.
   *
   * @param props The connection properties.
   * @return {@link CustomEndpointMonitor}
   */
  protected CustomEndpointMonitor createMonitorIfAbsent(Properties props) {
    return monitors.computeIfAbsent(
        this.customEndpointHostSpec.getHost(),
        (customEndpoint) -> new CustomEndpointMonitorImpl(
            this.pluginService,
            this.customEndpointHostSpec,
            this.customEndpointId,
            this.region,
            TimeUnit.MILLISECONDS.toNanos(CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS.getLong(props)),
            this.rdsClientFunc
        ),
        TimeUnit.MILLISECONDS.toNanos(this.idleMonitorExpirationMs)
    );
  }



  /**
   * If custom endpoint info does not exist for the current custom endpoint, waits a short time for the info to be
   * made available by the custom endpoint monitor. This is necessary so that other plugins can rely on accurate custom
   * endpoint info. Since custom endpoint monitors and information are shared, we should not have to wait often.
   *
   * @param monitor A {@link CustomEndpointMonitor} monitor.
   */
  protected void waitForCustomEndpointInfo(CustomEndpointMonitor monitor) throws SQLException {
    boolean hasCustomEndpointInfo = monitor.hasCustomEndpointInfo();

    if (!hasCustomEndpointInfo) {
      // Wait for the monitor to place the custom endpoint info in the cache. This ensures other plugins get accurate
      // custom endpoint info.
      this.waitForInfoCounter.inc();
      LOGGER.fine(
          Messages.get(
              "CustomEndpointPlugin.waitingForCustomEndpointInfo",
              new Object[]{ this.customEndpointHostSpec.getHost(), this.waitOnCachedInfoDurationMs }));
      long waitForEndpointInfoTimeoutNano =
          System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.waitOnCachedInfoDurationMs);

      try {
        while (!hasCustomEndpointInfo && System.nanoTime() < waitForEndpointInfoTimeoutNano) {
          TimeUnit.MILLISECONDS.sleep(100);
          hasCustomEndpointInfo = monitor.hasCustomEndpointInfo();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException(
            Messages.get(
                "CustomEndpointPlugin.interruptedThread",
                new Object[]{ this.customEndpointHostSpec.getHost() }));
      }

      if (!hasCustomEndpointInfo) {
        throw new SQLException(
            Messages.get("CustomEndpointPlugin.timedOutWaitingForCustomEndpointInfo",
                new Object[]{this.waitOnCachedInfoDurationMs, this.customEndpointHostSpec.getHost()}));
      }
    }
  }

  /**
   * Executes the given method via a pipeline of plugins. If a custom endpoint is being used, a monitor for that custom
   * endpoint will be created if it does not already exist.
   *
   * @param resultClass    The class of the object returned by the {@code jdbcMethodFunc}.
   * @param exceptionClass The desired exception class for any exceptions that occur while executing the
   *                       {@code jdbcMethodFunc}.
   * @param methodInvokeOn The object that the {@code jdbcMethodFunc} is being invoked on.
   * @param methodName     The name of the method being invoked.
   * @param jdbcMethodFunc The execute pipeline to call to invoke the method.
   * @param jdbcMethodArgs The arguments to the method being invoked.
   * @param <T>            The type of the result returned by the method.
   * @param <E>            The desired type for any exceptions that occur while executing the {@code jdbcMethodFunc}.
   * @return The result of the method invocation.
   * @throws E If an exception occurs, either directly in this method, or while executing the {@code jdbcMethodFunc}.
   */
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

    try {
      CustomEndpointMonitor monitor = createMonitorIfAbsent(this.props);
      if (this.shouldWaitForInfo) {
        // If needed, wait a short time for custom endpoint info to be discovered.
        waitForCustomEndpointInfo(monitor);
      }
    } catch (Exception e) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
    }

    return jdbcMethodFunc.call();
  }

  /**
   * Closes all active custom endpoint monitors.
   */
  public static void closeMonitors() {
    LOGGER.info(Messages.get("CustomEndpointPlugin.closeMonitors"));
    // The clear call automatically calls close() on all monitors.
    monitors.clear();
  }
}
