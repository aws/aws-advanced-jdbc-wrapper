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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.storage.CacheMap;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

/**
 * The default custom endpoint monitor implementation. This class uses a background thread to monitor a given custom
 * endpoint for custom endpoint information and future changes to the custom endpoint.
 */
public class CustomEndpointMonitorImpl extends AbstractMonitor implements CustomEndpointMonitor {
  private static final Logger LOGGER = Logger.getLogger(CustomEndpointMonitorImpl.class.getName());
  private static final String TELEMETRY_ENDPOINT_INFO_CHANGED = "customEndpoint.infoChanged.counter";

  // Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
  protected static final CacheMap<String, CustomEndpointInfo> customEndpointInfoCache = new CacheMap<>();
  protected static final long CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(5);
  protected static final long UNAUTHORIZED_SLEEP_NANO = TimeUnit.MINUTES.toNanos(5);
  protected static final long MONITOR_TERMINATION_TIMEOUT_SEC = 30;

  protected final AtomicBoolean refreshRequired = new AtomicBoolean(false);
  protected final RdsClient rdsClient;
  protected final HostSpec customEndpointHostSpec;
  protected final String endpointIdentifier;
  protected final Region region;
  protected long minRefreshRateNano;
  protected long maxRefreshRateNano;
  protected long refreshRateNano;
  protected int refreshRateBackoffFactor;
  protected final StorageService storageService;

  private final TelemetryCounter infoChangedCounter;

  /**
   * Constructs a CustomEndpointMonitorImpl instance for the host specified by {@code customEndpointHostSpec}.
   *
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
      StorageService storageService,
      TelemetryFactory telemetryFactory,
      HostSpec customEndpointHostSpec,
      String endpointIdentifier,
      Region region,
      long refreshRateNano,
      int refreshRateBackoffFactor,
      long maxRefreshRateNano,
      BiFunction<HostSpec, Region, RdsClient> rdsClientFunc) {
    super(MONITOR_TERMINATION_TIMEOUT_SEC);
    this.storageService = storageService;
    this.customEndpointHostSpec = customEndpointHostSpec;
    this.endpointIdentifier = endpointIdentifier;
    this.region = region;
    this.refreshRateNano = refreshRateNano;
    this.refreshRateBackoffFactor = refreshRateBackoffFactor;
    this.minRefreshRateNano = refreshRateNano;
    this.maxRefreshRateNano = maxRefreshRateNano;
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
          this.lastActivityTimestampNanos.set(System.nanoTime());

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

            this.sleep(this.refreshRateNano);
            continue;
          }

          CustomEndpointInfo endpointInfo = CustomEndpointInfo.fromDBClusterEndpoint(endpoints.get(0));
          CustomEndpointInfo cachedEndpointInfo = customEndpointInfoCache.get(this.customEndpointHostSpec.getUrl());
          if (cachedEndpointInfo != null && cachedEndpointInfo.equals(endpointInfo)) {
            long elapsedTime = System.nanoTime() - start;
            long sleepDuration = Math.max(0, this.refreshRateNano - elapsedTime);
            this.sleep(sleepDuration);
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
          this.refreshRequired.set(false);
          if (this.infoChangedCounter != null) {
            this.infoChangedCounter.inc();
          }

          speedupRefreshRate();

          long elapsedTime = System.nanoTime() - start;
          long sleepDuration = Math.max(0, this.refreshRateNano - elapsedTime);
          this.sleep(sleepDuration);
        } catch (InterruptedException e) {
          throw e;
        } catch (RdsException ex) {
          LOGGER.log(Level.SEVERE,
              Messages.get(
                  "CustomEndpointMonitorImpl.exception",
                  new Object[] {this.customEndpointHostSpec.getUrl()}), ex);

          if (ex.isThrottlingException()) {
            slowdownRefreshRate();
            this.sleep(this.refreshRateNano);
          } else if (ex.statusCode() == HttpStatusCode.UNAUTHORIZED || ex.statusCode() == HttpStatusCode.FORBIDDEN) {
            // User has no permissions to get custom endpoint details.
            // Reduce the refresh rate.
            this.sleep(UNAUTHORIZED_SLEEP_NANO);
          } else {
            this.sleep(this.refreshRateNano);
          }
        } catch (Exception e) {
          // If the exception is not an InterruptedException, log it and continue monitoring.
          LOGGER.log(Level.SEVERE,
              Messages.get(
                  "CustomEndpointMonitorImpl.exception",
                  new Object[] {this.customEndpointHostSpec.getUrl()}), e);

          this.sleep(this.refreshRateNano);
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

  protected void speedupRefreshRate() {
    if (this.refreshRateNano > this.minRefreshRateNano) {
      this.refreshRateNano /= this.refreshRateBackoffFactor; // Increase the refresh rate
      if (this.refreshRateNano < this.minRefreshRateNano) {
        this.refreshRateNano = this.minRefreshRateNano;
      }
    }
  }

  protected void slowdownRefreshRate() {
    if (this.refreshRateNano < this.maxRefreshRateNano) {
      this.refreshRateNano *= this.refreshRateBackoffFactor; // Reduce the refresh rate.
      if (this.refreshRateNano > this.maxRefreshRateNano) {
        this.refreshRateNano = this.maxRefreshRateNano;
      }
    }
  }

  protected void sleep(long durationNano) throws InterruptedException {
    long endNano = System.nanoTime() + durationNano;
    // Choose the minimum between 500ms and the durationNano passed in, in case durationNano is less than 500ms.
    long waitDurationMs = Math.min(500, TimeUnit.NANOSECONDS.toMillis(durationNano));
    while (!this.refreshRequired.get() && System.nanoTime() < endNano && !this.stop.get()) {
      synchronized (this.refreshRequired) {
        this.refreshRequired.wait(waitDurationMs);
      }
    }
  }

  public boolean hasCustomEndpointInfo() {
    CustomEndpointInfo customEndpointInfo = customEndpointInfoCache.get(this.customEndpointHostSpec.getUrl());
    if (customEndpointInfo == null && !this.refreshRequired.get()) {
      // There is no custom endpoint info, probably because the cache entry has expired. We use notifyAll below to
      // wake up the custom endpoint monitor if it is sleeping.
      synchronized (this.refreshRequired) {
        this.refreshRequired.set(true);
        this.refreshRequired.notifyAll();
      }
    }

    return customEndpointInfo != null;
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
