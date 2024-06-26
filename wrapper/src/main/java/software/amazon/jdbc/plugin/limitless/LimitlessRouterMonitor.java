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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SynchronousExecutor;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class LimitlessRouterMonitor implements AutoCloseable, Runnable {

  private static final Logger LOGGER =
      Logger.getLogger(LimitlessRouterMonitor.class.getName());

  private static final String MONITORING_PROPERTY_PREFIX = "limitless-router-monitor-";
  private final int intervalMs;
  private @NonNull HostSpec hostSpec;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicReference<List<HostSpec>> limitlessRouters = new AtomicReference<>(
      Collections.unmodifiableList(new ArrayList<>()));
  private final @NonNull Properties props;
  private final @NonNull PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private Connection monitoringConn = null;
  final Executor networkTimeoutExecutor = new SynchronousExecutor();
  static final int defaultQueryTimeoutMs = 5000;

  private final ExecutorService threadPool = Executors.newFixedThreadPool(1, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });

  public LimitlessRouterMonitor(
      final @NonNull PluginService pluginService,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs) {
    this.pluginService = pluginService;
    this.hostSpec = hostSpec;
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

    this.intervalMs = intervalMs;
    this.telemetryFactory = this.pluginService.getTelemetryFactory();

    this.threadPool.submit(this);
    this.threadPool.shutdown(); // No more task are accepted by pool.
  }

  public List<HostSpec> getLimitlessRouters() {
    return this.limitlessRouters.get();
  }

  public AtomicBoolean isStopped() {
    return this.stopped;
  }

  @Override
  public void close() throws Exception {
    this.stopped.set(true);

    // Waiting for 5s gives a thread enough time to exit monitoring loop and close database connection.
    if (!this.threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
      this.threadPool.shutdownNow();
    }
    LOGGER.finest(() -> Messages.get(
        "LimitlessRouterMonitor.stopped",
        new Object[] {this.hostSpec.getHost()}));
  }

  @Override
  public void run() {
    LOGGER.finest(() -> Messages.get(
        "LimitlessRouterMonitor.running",
        new Object[] {this.hostSpec.getHost()}));

    while (!this.stopped.get()) {
      TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
          "node response time thread", TelemetryTraceLevel.TOP_LEVEL);
      telemetryContext.setAttribute("url", hostSpec.getUrl());
      try {
        this.openConnection();
        if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
          continue;
        }
        List<HostSpec> newLimitlessRouters = queryForLimitlessRouters(this.monitoringConn);
        this.limitlessRouters.set(Collections.unmodifiableList(newLimitlessRouters));
        RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(this.props, newLimitlessRouters);
        LOGGER.finest(Utils.logTopology(limitlessRouters.get(), "[limitlessRouterMonitor]"));
        TimeUnit.MILLISECONDS.sleep(this.intervalMs); // do not include this in the telemetry
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
        if (telemetryContext != null) {
          telemetryContext.closeContext();
        }
      }
    }
  }

  private void openConnection() throws SQLException {
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        LOGGER.finest(() -> Messages.get(
            "LimitlessRouterMonitor.openingConnection",
            new Object[] {this.hostSpec.getUrl()}));
        this.monitoringConn = this.pluginService.forceConnect(this.hostSpec, this.props);
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

  private List<HostSpec> queryForLimitlessRouters(final Connection conn) throws SQLException {
    int networkTimeout = -1;
    try {
      networkTimeout = conn.getNetworkTimeout();
      // The query is not monitored by the EFM plugin, so it needs a socket timeout
      if (networkTimeout == 0) {
        conn.setNetworkTimeout(networkTimeoutExecutor, defaultQueryTimeoutMs);
      }
    } catch (SQLException e) {
      LOGGER.warning(() -> Messages.get("LimitlessRouterMonitor.getNetworkTimeoutError",
          new Object[] {e.getMessage()}));
    }

    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(
             "select node_id, dns_host, load from aurora_limitless_distributed_transaction_router_details()")) {
      return mapResultSetToHostSpecList(resultSet);
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("LimitlessRouterMonitor.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  private List<HostSpec> mapResultSetToHostSpecList(final ResultSet resultSet) throws SQLException {

    List<HostSpec> hosts = new ArrayList<>();
    while (resultSet.next()) {
      final HostSpec host = createHost(resultSet);
      hosts.add(host);
    }

    return hosts;
  }

  private HostSpec createHost(final ResultSet resultSet) throws SQLException {
    final String hostName = resultSet.getString(2);
    final float cpu = resultSet.getFloat(3);
    final long weight = (long) (10 - cpu * 10);
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(hostName)
        .port(this.hostSpec.getPort())
        .role(HostRole.WRITER)
        .availability(HostAvailability.AVAILABLE)
        .weight(weight)
        .build();
  }
}
