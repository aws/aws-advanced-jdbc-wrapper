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

import static software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS;
import static software.amazon.jdbc.plugin.customendpoint.CustomEndpointPlugin.REGION_PROPERTY;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class CustomEndpointMonitorImpl implements CustomEndpointMonitor {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  private static final String TELEMETRY_ENDPOINT_INFO_CHANGED = "customEndpoint.infoChanged.counter";

  // Keys are custom cluster endpoint identifiers, values are information objects for the associated custom cluster.
  protected static final CacheMap<String, CustomEndpointInfo> customEndpointInfoCache = new CacheMap<>();
  protected static final long CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(5);

  protected final AtomicBoolean stop = new AtomicBoolean(false);
  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final RdsClient rdsClient;
  protected final HostSpec customEndpointHostSpec;
  protected final String endpointIdentifier;
  protected final Region region;
  protected final long refreshRateNano;

  protected final PluginService pluginService;
  // TODO: is it problematic for each monitor thread to have its own executor service?
  protected final ExecutorService monitorExecutor = Executors.newSingleThreadExecutor(runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(monitoringThread.getName())) {
      monitoringThread.setName(monitoringThread.getName() + "-CustomEndpointMonitor");
    }
    return monitoringThread;
  });

  private final TelemetryFactory telemetryFactory;
  private final TelemetryCounter infoChangedCounter;

  public CustomEndpointMonitorImpl(
      PluginService pluginService,
      HostSpec customEndpointHostSpec,
      Properties props,
      BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    this.pluginService = pluginService;
    this.customEndpointHostSpec = customEndpointHostSpec;
    this.region = getRegion(customEndpointHostSpec, props);
    this.rdsClient = rdsClientFunc.apply(customEndpointHostSpec, this.region);

    this.refreshRateNano = TimeUnit.MILLISECONDS.toNanos(CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS.getLong(props));

    this.endpointIdentifier = this.rdsUtils.getRdsClusterId(customEndpointHostSpec.getHost());
    if (StringUtils.isNullOrEmpty(this.endpointIdentifier)) {
      throw new RuntimeException(
          Messages.get(
              "CustomEndpointMonitorImpl.errorParsingEndpointIdentifier",
              new Object[] {customEndpointHostSpec.getHost()}));
    }

    this.telemetryFactory = this.pluginService.getTelemetryFactory();
    this.infoChangedCounter = this.telemetryFactory.createCounter(TELEMETRY_ENDPOINT_INFO_CHANGED);

    this.monitorExecutor.submit(this);
    this.monitorExecutor.shutdown();
  }

  protected Region getRegion(HostSpec hostSpec, Properties props) {
    String regionString = REGION_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(regionString)) {
      regionString = rdsUtils.getRdsRegion(hostSpec.getHost());
    }

    if (StringUtils.isNullOrEmpty(regionString)) {
      throw new
          RuntimeException(
          Messages.get(
              "CustomEndpointMonitorImpl.missingRequiredConfigParameter",
              new Object[] {REGION_PROPERTY.name}));
    }

    final Region region = Region.of(regionString);
    if (!Region.regions().contains(region)) {
      throw new RuntimeException(
          Messages.get(
              "AwsSdk.unsupportedRegion",
              new Object[] {regionString}));
    }

    return region;
  }

  @Override
  public void run() {
    LOGGER.fine(
        Messages.get(
            "CustomEndpointMonitorImpl.startingMonitor",
            new Object[] { this.customEndpointHostSpec.getHost() }));

    try {
      while (!this.stop.get() && !Thread.currentThread().isInterrupted()) {
        long start = System.nanoTime();

        final Filter customEndpointFilter =
            Filter.builder().name("db-cluster-endpoint-type").values("custom").build();
        final DescribeDbClusterEndpointsResponse endpointsResponse =
            this.rdsClient.describeDBClusterEndpoints(
                (builder) ->
                    builder.dbClusterEndpointIdentifier(this.endpointIdentifier).filters(customEndpointFilter));

        List<DBClusterEndpoint> endpoints = endpointsResponse.dbClusterEndpoints();
        if (endpoints.size() != 1) {
          List<String> endpointURLs = endpoints.stream().map(DBClusterEndpoint::endpoint).collect(Collectors.toList());
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
        CustomEndpointInfo cachedEndpointInfo = customEndpointInfoCache.get(this.endpointIdentifier);
        if (cachedEndpointInfo != null && cachedEndpointInfo.equals(endpointInfo)) {
          long elapsedTime = System.nanoTime() - start;
          long sleepDuration = Math.min(0, this.refreshRateNano - elapsedTime);
          TimeUnit.NANOSECONDS.sleep(sleepDuration);
          continue;
        }

        LOGGER.fine(
            Messages.get(
                "CustomEndpointMonitorImpl.detectedChangeInCustomEndpointInfo",
                new Object[]{ this.customEndpointHostSpec.getHost(), endpointInfo }));

        // The custom endpoint info has changed, so we need to update the info in the registered plugin services.
        customEndpointInfoCache.put(this.endpointIdentifier, endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO);
        this.pluginService.setStatus(this.customEndpointHostSpec.getHost(), endpointInfo, true);
        this.infoChangedCounter.inc();

        long elapsedTime = System.nanoTime() - start;
        long sleepDuration = Math.min(0, this.refreshRateNano - elapsedTime);
        TimeUnit.NANOSECONDS.sleep(sleepDuration);
      }
    } catch (InterruptedException e) {
      LOGGER.info(Messages.get("CustomEndpointMonitorImpl.interrupted", new Object[]{ this.customEndpointHostSpec }));
      Thread.currentThread().interrupt();
    } finally {
      this.rdsClient.close();
      LOGGER.fine(
          Messages.get("CustomEndpointMonitorImpl.stoppedMonitor", new Object[]{ this.customEndpointHostSpec }));
    }
  }

  @Override
  public boolean shouldDispose() {
    return true;
  }

  @Override
  public void close() {
    LOGGER.fine(
        Messages.get(
            "CustomEndpointMonitorImpl.stoppingMonitor",
            new Object[]{ this.customEndpointHostSpec.getHost() }));

    this.stop.set(true);

    try {
      // TODO: the run loop takes 30s by default but could take more depending on the user setting. Should we keep 30s
      //  as the maximum wait time here or is that too short/long?
      int terminationTimeoutSec = 30;
      if (!this.monitorExecutor.awaitTermination(terminationTimeoutSec, TimeUnit.SECONDS)) {
        LOGGER.info(
            Messages.get(
                "CustomEndpointMonitorImpl.monitorTerminationTimeout",
                new Object[]{ terminationTimeoutSec, this.customEndpointHostSpec.getHost() }));

        this.monitorExecutor.shutdownNow();
        this.rdsClient.close();
      }
    } catch (InterruptedException e) {
      LOGGER.info(
          Messages.get(
              "CustomEndpointMonitorImpl.interruptedWhileTerminating",
              new Object[]{ this.customEndpointHostSpec.getHost() }));

      Thread.currentThread().interrupt();
      this.monitorExecutor.shutdownNow();
      this.rdsClient.close();
    }
  }

  public static void clearCache() {
    LOGGER.info(Messages.get("CustomEndpointMonitorImpl.clearCache"));
    customEndpointInfoCache.clear();
  }
}
