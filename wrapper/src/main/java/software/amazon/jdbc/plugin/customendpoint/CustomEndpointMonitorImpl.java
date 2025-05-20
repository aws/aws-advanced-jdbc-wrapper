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
import java.util.concurrent.TimeUnit;
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
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.monitoring.CoreMonitorService;
import software.amazon.jdbc.util.storage.CacheMap;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * The default custom endpoint monitor implementation. This class uses a background thread to monitor a given custom
 * endpoint for custom endpoint information and future changes to the custom endpoint.
 */
public class CustomEndpointMonitorImpl extends AbstractMonitor implements CustomEndpointMonitor {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointPlugin.class.getName());
  private static final String TELEMETRY_ENDPOINT_INFO_CHANGED = "customEndpoint.infoChanged.counter";

  // Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
  protected static final CacheMap<String, CustomEndpointInfo> customEndpointInfoCache = new CacheMap<>();
  protected static final long CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(5);

  protected final RdsClient rdsClient;
  protected final HostSpec customEndpointHostSpec;
  protected final String endpointIdentifier;
  protected final Region region;
  protected final long refreshRateNano;
  protected final StorageService storageService;

  private final TelemetryCounter infoChangedCounter;

  /**
   * Constructs a CustomEndpointMonitorImpl instance for the host specified by {@code customEndpointHostSpec}.
   *
   * @param monitorService         The monitorService used to submit this monitor.
   * @param storageService         The storage service used to store the set of allowed/blocked hosts according to the
   *                               custom endpoint info.
   * @param telemetryFactory       The telemetry factory used to create telemetry data.
   * @param customEndpointHostSpec The host information for the custom endpoint to be monitored.
   * @param endpointIdentifier     An endpoint identifier.
   * @param region                 The region of the custom endpoint to be monitored.
   * @param refreshRateNano        Controls how often the custom endpoint information should be fetched and analyzed for
   *                               changes. The value specified should be in nanoseconds.
   * @param rdsClientFunc          The function to call to create the RDS client that will fetch custom endpoint
   *                               information.
   */
  public CustomEndpointMonitorImpl(
      CoreMonitorService monitorService,
      StorageService storageService,
      TelemetryFactory telemetryFactory,
      HostSpec customEndpointHostSpec,
      String endpointIdentifier,
      Region region,
      long refreshRateNano,
      BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    super(30);
    this.storageService = storageService;
    this.customEndpointHostSpec = customEndpointHostSpec;
    this.endpointIdentifier = endpointIdentifier;
    this.region = region;
    this.refreshRateNano = refreshRateNano;
    this.rdsClient = rdsClientFunc.apply(customEndpointHostSpec, this.region);

    this.infoChangedCounter = telemetryFactory.createCounter(TELEMETRY_ENDPOINT_INFO_CHANGED);
  }

  /**
   * Analyzes a given custom endpoint for changes to custom endpoint information.
   */
  @Override
  public void monitor() {
    LOGGER.fine(
        Messages.get(
            "CustomEndpointMonitorImpl.startingMonitor",
            new Object[] {this.customEndpointHostSpec.getUrl()}));

    try {
      while (!this.stop.get() && !Thread.currentThread().isInterrupted()) {
        try {
          long start = System.nanoTime();
          this.lastActivityTimestampNanos = System.nanoTime();

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
          CustomEndpointInfo cachedEndpointInfo = customEndpointInfoCache.get(this.customEndpointHostSpec.getUrl());
          if (cachedEndpointInfo != null && cachedEndpointInfo.equals(endpointInfo)) {
            long elapsedTime = System.nanoTime() - start;
            long sleepDuration = Math.max(0, this.refreshRateNano - elapsedTime);
            TimeUnit.NANOSECONDS.sleep(sleepDuration);
            continue;
          }

          LOGGER.fine(
              Messages.get(
                  "CustomEndpointMonitorImpl.detectedChangeInCustomEndpointInfo",
                  new Object[] {this.customEndpointHostSpec.getUrl(), endpointInfo}));

          // The custom endpoint info has changed, so we need to update the set of allowed/blocked hosts.
          AllowedAndBlockedHosts allowedAndBlockedHosts;
          if (STATIC_LIST.equals(endpointInfo.getMemberListType())) {
            allowedAndBlockedHosts = new AllowedAndBlockedHosts(endpointInfo.getStaticMembers(), null);
          } else {
            allowedAndBlockedHosts = new AllowedAndBlockedHosts(null, endpointInfo.getExcludedMembers());
          }

          this.storageService.set(this.customEndpointHostSpec.getUrl(), allowedAndBlockedHosts);
          customEndpointInfoCache.put(
              this.customEndpointHostSpec.getUrl(), endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO);
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
                  new Object[] {this.customEndpointHostSpec.getUrl()}), e);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.fine(
          Messages.get(
              "CustomEndpointMonitorImpl.interrupted",
              new Object[] {this.customEndpointHostSpec.getUrl()}));
      Thread.currentThread().interrupt();
    } finally {
      customEndpointInfoCache.remove(this.customEndpointHostSpec.getUrl());
      this.rdsClient.close();
      LOGGER.fine(
          Messages.get(
              "CustomEndpointMonitorImpl.stoppedMonitor",
              new Object[] {this.customEndpointHostSpec.getUrl()}));
    }
  }

  public boolean hasCustomEndpointInfo() {
    return customEndpointInfoCache.get(this.customEndpointHostSpec.getUrl()) != null;
  }

  @Override
  public void close() {
    customEndpointInfoCache.remove(this.customEndpointHostSpec.getUrl());
    this.rdsClient.close();
  }

  /**
   * Clears the shared custom endpoint information cache.
   */
  public static void clearCache() {
    LOGGER.info(Messages.get("CustomEndpointMonitorImpl.clearCache"));
    customEndpointInfoCache.clear();
  }
}
