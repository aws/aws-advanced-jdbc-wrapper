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

package software.amazon.jdbc.plugin.bluegreen.routing;

import static software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPlugin.BG_CONNECT_TIMEOUT;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenPhase;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenStatus;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.storage.StorageService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

// Suspend new connection opening till BG is completed.
public class SuspendConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(SuspendConnectRouting.class.getName());

  private static final String TELEMETRY_SWITCHOVER = "Blue/Green switchover";
  private static final long SLEEP_TIME_MS = 100L;

  protected String bgdId;

  public SuspendConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role, final String bgdId) {
    super(hostAndPort, role);
    this.bgdId = bgdId;
  }

  @Override
  public Connection apply(
      ConnectionPlugin plugin,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      boolean useForceConnect,
      JdbcCallable<Connection, SQLException> connectFunc,
      StorageService storageService,
      PluginService pluginService)
      throws SQLException {

    LOGGER.finest(() -> Messages.get("bgd.inProgressHoldConnect"));
    TelemetryFactory telemetryFactory = pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_SWITCHOVER,
        TelemetryTraceLevel.NESTED);

    BlueGreenStatus bgStatus = storageService.get(BlueGreenStatus.class, this.bgdId);

    long timeoutNano = TimeUnit.MILLISECONDS.toNanos(BG_CONNECT_TIMEOUT.getLong(props));
    long holdStartTime = this.getNanoTime();
    long endTime = this.getNanoTime() + timeoutNano;

    try {
      while (this.getNanoTime() <= endTime
          && bgStatus != null
          && bgStatus.getCurrentPhase() == BlueGreenPhase.IN_PROGRESS) {

        try {
          this.delay(SLEEP_TIME_MS, bgStatus, storageService, this.bgdId);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        bgStatus = storageService.get(BlueGreenStatus.class, this.bgdId);
      }

      if (bgStatus != null && bgStatus.getCurrentPhase() == BlueGreenPhase.IN_PROGRESS) {
        throw new SQLTimeoutException(
                Messages.get("bgd.inProgressTryConnectLater", new Object[] {BG_CONNECT_TIMEOUT.getLong(props)}));
      }
      LOGGER.finest(Messages.get("bgd.switchoverCompleteContinueWithConnect",
              new Object[] {TimeUnit.NANOSECONDS.toMillis(this.getNanoTime() - holdStartTime)}));

    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }

    // returning no connection so the next routing can handle it
    return null;
  }
}
