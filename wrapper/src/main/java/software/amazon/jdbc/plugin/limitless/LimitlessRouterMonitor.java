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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.connection.ConnectionService;
import software.amazon.jdbc.util.monitoring.AbstractMonitor;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class LimitlessRouterMonitor extends AbstractMonitor {

  private static final Logger LOGGER =
      Logger.getLogger(LimitlessRouterMonitor.class.getName());

  protected static final String MONITORING_PROPERTY_PREFIX = "limitless-router-monitor-";
  protected static final long TERMINATION_TIMEOUT_SEC = 5;
  protected final int intervalMs;
  protected final @NonNull HostSpec hostSpec;
  protected final @NonNull StorageService storageService;
  protected final @NonNull String limitlessRouterCacheKey;
  protected final @NonNull Properties props;
  protected final @NonNull ConnectionService connectionService;
  protected final @NonNull LimitlessQueryHelper queryHelper;
  protected final @NonNull TelemetryFactory telemetryFactory;
  protected Connection monitoringConn = null;

  public LimitlessRouterMonitor(
      final @NonNull PluginService pluginService,
      final @NonNull ConnectionService connectionService,
      final @NonNull TelemetryFactory telemetryFactory,
      final @NonNull HostSpec hostSpec,
      final @NonNull StorageService storageService,
      final @NonNull String limitlessRouterCacheKey,
      final @NonNull Properties props,
      final int intervalMs) {
    super(TERMINATION_TIMEOUT_SEC);
    this.connectionService = connectionService;
    this.storageService = storageService;
    this.telemetryFactory = telemetryFactory;
    this.hostSpec = hostSpec;
    this.limitlessRouterCacheKey = limitlessRouterCacheKey;
    this.props = PropertyUtils.copyProperties(props);
    props.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              this.props.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.props.getProperty(p));
              this.props.remove(p);
            });
    this.props.setProperty(LimitlessConnectionPlugin.WAIT_FOR_ROUTER_INFO.name, "false");

    this.intervalMs = intervalMs;
    this.queryHelper = new LimitlessQueryHelper(pluginService);
  }

  @Override
  public void close() {
    try {
      if (this.monitoringConn != null && !this.monitoringConn.isClosed()) {
        this.monitoringConn.close();
      }
    } catch (final SQLException ex) {
      // ignore
    }

    this.monitoringConn = null;
  }

  @Override
  public void monitor() {
    LOGGER.finest(() -> Messages.get(
        "LimitlessRouterMonitor.running",
        new Object[] {this.hostSpec.getHost()}));

    try {
      while (!this.stop.get()) {
        TelemetryContext telemetryContext = this.telemetryFactory.openTelemetryContext(
            "limitless router monitor thread", TelemetryTraceLevel.TOP_LEVEL);
        if (telemetryContext != null) {
          telemetryContext.setAttribute("url", hostSpec.getUrl());
        }

        try {
          this.openConnection();
          if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
            continue;
          }

          List<HostSpec> newLimitlessRouters =
              queryHelper.queryForLimitlessRouters(this.monitoringConn, this.hostSpec.getPort());
          this.storageService.set(this.limitlessRouterCacheKey, new LimitlessRouters(newLimitlessRouters));
          LOGGER.finest(Utils.logTopology(newLimitlessRouters, "[limitlessRouterMonitor] Topology:"));
          TimeUnit.MILLISECONDS.sleep(this.intervalMs); // do not include this in the telemetry
        } catch (final Exception ex) {
          if (telemetryContext != null) {
            telemetryContext.setException(ex);
            telemetryContext.setSuccess(false);
          }
          throw ex;
        } finally {
          if (telemetryContext != null) {
            telemetryContext.closeContext();
          }
        }
      }
    } catch (final InterruptedException exception) {
      LOGGER.finest(
          () -> Messages.get(
              "LimitlessRouterMonitor.interruptedExceptionDuringMonitoring",
              new Object[] {this.hostSpec.getHost()}));
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(
            Level.FINEST,
            Messages.get(
                "LimitlessRouterMonitor.exceptionDuringMonitoringStop",
                new Object[] {this.hostSpec.getHost()}),
            ex); // We want to print full trace stack of the exception.
      }
    } finally {
      this.stop.set(true);
      try {
        if (this.monitoringConn != null && !this.monitoringConn.isClosed()) {
          this.monitoringConn.close();
        }
      } catch (final SQLException ex) {
        // ignore
      }
      this.monitoringConn = null;
    }

  }

  private void openConnection() throws SQLException {
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        LOGGER.finest(() -> Messages.get(
            "LimitlessRouterMonitor.openingConnection",
            new Object[] {this.hostSpec.getUrl()}));
        this.monitoringConn = this.connectionService.open(this.hostSpec, this.props);
        LOGGER.finest(() -> Messages.get(
            "LimitlessRouterMonitor.openedConnection",
            new Object[] {this.monitoringConn}));
      }
    } catch (SQLException ex) {
      if (this.monitoringConn != null && !this.monitoringConn.isClosed()) {
        try {
          this.monitoringConn.close();
        } catch (Exception e) {
          // ignore
        }
        this.monitoringConn = null;
      }
      throw ex;
    }
  }
}
