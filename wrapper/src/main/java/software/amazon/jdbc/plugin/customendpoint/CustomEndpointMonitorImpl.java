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

import static software.amazon.jdbc.plugin.customendpoint.MemberListType.STATIC_LIST;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * The default custom endpoint monitor implementation. This class uses a background thread to monitor a given custom
 * endpoint for custom endpoint information and future changes to the custom endpoint.
 */
public class CustomEndpointMonitorImpl implements CustomEndpointMonitor {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  private static final String TELEMETRY_ENDPOINT_INFO_CHANGED = "customEndpoint.infoChanged.counter";

  // Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
  protected static final CacheMap<String, CustomEndpointInfo> customEndpointInfoCache = new CacheMap<>();
  protected static final long CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(5);

  protected final AtomicBoolean stop = new AtomicBoolean(false);
  protected final RdsClient rdsClient;
  protected final HostSpec customEndpointHostSpec;
  protected final String endpointIdentifier;
  protected final Region region;
  protected final long refreshRateNano;

  protected final PluginService pluginService;
  protected final ExecutorService monitorExecutor = Executors.newSingleThreadExecutor(runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(monitoringThread.getName())) {
      monitoringThread.setName(monitoringThread.getName() + "-cem");
    }
    return monitoringThread;
  });

  private final TelemetryCounter infoChangedCounter;

  /**
   * Constructs a CustomEndpointMonitorImpl instance for the host specified by {@code customEndpointHostSpec}.
   *
   * @param pluginService          The plugin service to use to update the set of allowed/blocked hosts according to
   *                               the custom endpoint info.
   * @param customEndpointHostSpec The host information for the custom endpoint to be monitored.
   * @param endpointIdentifier An endpoint identifier.
   * @param region                 The region of the custom endpoint to be monitored.
   * @param refreshRateNano        Controls how often the custom endpoint information should be fetched and analyzed for
   *                               changes. The value specified should be in nanoseconds.
   * @param rdsClientFunc          The function to call to create the RDS client that will fetch custom endpoint
   *                               information.
   */
  public CustomEndpointMonitorImpl(
      PluginService pluginService,
      HostSpec customEndpointHostSpec,
      String endpointIdentifier,
      Region region,
      long refreshRateNano,
      BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    this.pluginService = pluginService;
    this.customEndpointHostSpec = customEndpointHostSpec;
    this.endpointIdentifier = endpointIdentifier;
    this.region = region;
    this.refreshRateNano = refreshRateNano;
    this.rdsClient = rdsClientFunc.apply(customEndpointHostSpec, this.region);

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    this.infoChangedCounter = telemetryFactory.createCounter(TELEMETRY_ENDPOINT_INFO_CHANGED);

    this.monitorExecutor.submit(this);
    this.monitorExecutor.shutdown();
  }

  /**
   * Analyzes a given custom endpoint for changes to custom endpoint information.
   */
  @Override
  public void run() {
    LOGGER.fine(
        Messages.get(
            "CustomEndpointMonitorImpl.startingMonitor",
            new Object[] { this.customEndpointHostSpec.getHost() }));

    try {
      while (!this.stop.get() && !Thread.currentThread().isInterrupted()) {
        try {
          long start = System.nanoTime();

          final Filter customEndpointFilter =
              Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
          final DescribeDbClusterEndpointsResponse endpointsResponse =
              this.rdsClient.describeDBClusterEndpoints(
                  (builder) ->
                      builder.dbClusterEndpointIdentifier(this.endpointIdentifier).filters(customEndpointFilter));

          List<DBClusterEndpoint> endpoints = endpointsResponse.dbClusterEndpoints();
          if (endpoints.size() != 1) {
            List<String> endpointURLs =
                endpoints.stream().map(DBClusterEndpoint::endpoint).collect(Collectors.toList());
            LOGGER.warning(
                Messages.get("CustomEndpointMonitorImpl.unexpectedNumberOfEndpoints",
                    new Object[] {
                        this.endpointIdentifier,
                        this.region.id(),
                        endpoints.size(),
                        endpointURLs
                    }
                ));

            TimeUnit.NANOSECONDS.sleep(this.refreshRateNano);
            continue;
          }

          CustomEndpointInfo endpointInfo = CustomEndpointInfo.fromDBClusterEndpoint(endpoints.get(0));
          CustomEndpointInfo cachedEndpointInfo = customEndpointInfoCache.get(this.customEndpointHostSpec.getHost());
          if (cachedEndpointInfo != null && cachedEndpointInfo.equals(endpointInfo)) {
            long elapsedTime = System.nanoTime() - start;
            long sleepDuration = Math.max(0, this.refreshRateNano - elapsedTime);
            TimeUnit.NANOSECONDS.sleep(sleepDuration);
            continue;
          }

          LOGGER.fine(
              Messages.get(
                  "CustomEndpointMonitorImpl.detectedChangeInCustomEndpointInfo",
                  new Object[] {this.customEndpointHostSpec.getHost(), endpointInfo}));

          // The custom endpoint info has changed, so we need to update the set of allowed/blocked hosts.
          AllowedAndBlockedHosts allowedAndBlockedHosts;
          if (STATIC_LIST.equals(endpointInfo.getMemberListType())) {
            allowedAndBlockedHosts = new AllowedAndBlockedHosts(endpointInfo.getStaticMembers(), null);
          } else {
            allowedAndBlockedHosts = new AllowedAndBlockedHosts(null, endpointInfo.getExcludedMembers());
          }

          this.pluginService.setAllowedAndBlockedHosts(allowedAndBlockedHosts);
          customEndpointInfoCache.put(
              this.customEndpointHostSpec.getHost(), endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO);
          this.infoChangedCounter.inc();

          long elapsedTime = System.nanoTime() - start;
          long sleepDuration = Math.max(0, this.refreshRateNano - elapsedTime);
          TimeUnit.NANOSECONDS.sleep(sleepDuration);
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          // If the exception is not an InterruptedException, log it and continue monitoring.
          LOGGER.log(Level.SEVERE,
              Messages.get(
                  "CustomEndpointMonitorImpl.exception",
                  new Object[]{this.customEndpointHostSpec.getHost()}), e);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.fine(
          Messages.get(
              "CustomEndpointMonitorImpl.interrupted",
              new Object[]{ this.customEndpointHostSpec.getHost() }));
      Thread.currentThread().interrupt();
    } finally {
      customEndpointInfoCache.remove(this.customEndpointHostSpec.getHost());
      this.rdsClient.close();
      LOGGER.fine(
          Messages.get(
              "CustomEndpointMonitorImpl.stoppedMonitor",
              new Object[]{ this.customEndpointHostSpec.getHost() }));
    }
  }

  public boolean hasCustomEndpointInfo() {
    return customEndpointInfoCache.get(this.customEndpointHostSpec.getHost()) != null;
  }

  @Override
  public boolean shouldDispose() {
    return true;
  }

  /**
   * Stops the custom endpoint monitor.
   */
  @Override
  public void close() {
    LOGGER.fine(
        Messages.get(
            "CustomEndpointMonitorImpl.stoppingMonitor",
            new Object[]{ this.customEndpointHostSpec.getHost() }));

    this.stop.set(true);

    try {
      int terminationTimeoutSec = 5;
      if (!this.monitorExecutor.awaitTermination(terminationTimeoutSec, TimeUnit.SECONDS)) {
        LOGGER.info(
            Messages.get(
                "CustomEndpointMonitorImpl.monitorTerminationTimeout",
                new Object[]{ terminationTimeoutSec, this.customEndpointHostSpec.getHost() }));

        this.monitorExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOGGER.info(
          Messages.get(
              "CustomEndpointMonitorImpl.interruptedWhileTerminating",
              new Object[]{ this.customEndpointHostSpec.getHost() }));

      Thread.currentThread().interrupt();
      this.monitorExecutor.shutdownNow();
    } finally {
      customEndpointInfoCache.remove(this.customEndpointHostSpec.getHost());
      this.rdsClient.close();
    }
  }

  /**
   * Clears the shared custom endpoint information cache.
   */
  public static void clearCache() {
    LOGGER.info(Messages.get("CustomEndpointMonitorImpl.clearCache"));
    customEndpointInfoCache.clear();
  }
}
