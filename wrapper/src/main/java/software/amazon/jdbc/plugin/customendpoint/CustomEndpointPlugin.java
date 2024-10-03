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

public class CustomEndpointPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  protected static final long CACHE_CLEANUP_RATE_NANO = TimeUnit.MINUTES.toNanos(1);
  protected static final SlidingExpirationCacheWithCleanupThread<String, CustomEndpointMonitor> monitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          CustomEndpointMonitor::shouldDispose,
          (monitor) -> {
            try {
              System.out.println("asdf about to call monitor.close");
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

  // TODO: is 15 minutes a good value?
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
    if (!isInitialConnection || !this.rdsUtils.isRdsCustomClusterDns(hostSpec.getHost())) {
      return connectFunc.call();
    }

    // If we get here we are making an initial connection to a custom cluster URL
    this.customEndpointHostSpec = hostSpec;
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

    // If needed, wait a short time for the monitor to place the custom endpoint info in the cache. This ensures other
    // plugins get accurate custom endpoint info.
    long waitForEndpointInfoTimeoutNano =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.waitOnCachedInfoDurationMs);
    boolean isInfoInCache = false;
    while (!isInfoInCache && System.nanoTime() < waitForEndpointInfoTimeoutNano) {
      CustomEndpointInfo cachedInfo =
          this.pluginService.getStatus(this.customEndpointHostSpec.getHost(), CustomEndpointInfo.class, true);
      isInfoInCache = cachedInfo != null;
    }

    final Connection conn = connectFunc.call();

    if (!isInfoInCache) {
      // TODO: should we throw an exception or just log a warning? If we only log a warning, other plugins may not
      //  receive custom endpoint info and will instead rely on the complete topology list.
      throw new SQLException(
          Messages.get("CustomEndpointPlugin.cacheTimeout",
              new Object[]{this.waitOnCachedInfoDurationMs, this.customEndpointHostSpec.getHost()}));
    }

    return conn;
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

    // If needed, wait a short time for the monitor to place the custom endpoint info in the cache. This ensures other
    // plugins get accurate custom endpoint info.
    long waitForEndpointInfoTimeoutNano =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.waitOnCachedInfoDurationMs);
    boolean isInfoInCache = false;
    while (!isInfoInCache && System.nanoTime() < waitForEndpointInfoTimeoutNano) {
      CustomEndpointInfo cachedInfo =
          this.pluginService.getStatus(this.customEndpointHostSpec.getHost(), CustomEndpointInfo.class, true);
      isInfoInCache = cachedInfo != null;
    }

    if (!isInfoInCache) {
      // TODO: should we throw an exception or just log a warning? If we only log a warning, other plugins may not
      //  receive custom endpoint info and will instead rely on the complete topology list.
      SQLException cacheTimeoutException = new SQLException(
          Messages.get("CustomEndpointPlugin.cacheTimeout",
              new Object[]{this.waitOnCachedInfoDurationMs, this.customEndpointHostSpec.getHost()}));
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, cacheTimeoutException);
    }

    return jdbcMethodFunc.call();
  }

  public static void clearCache() {
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
