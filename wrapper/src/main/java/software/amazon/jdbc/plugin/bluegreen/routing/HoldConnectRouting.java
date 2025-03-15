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
import software.amazon.jdbc.plugin.bluegreen.BlueGreenPhases;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenStatus;
import software.amazon.jdbc.plugin.bluegreen.IntervalType;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

// Hold new connection opening till BG is completed.
public class HoldConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(HoldConnectRouting.class.getName());

  private static final String TELEMETRY_SWITCHOVER = "Blue/Green switchover";

  protected String bgdId;

  public HoldConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role, final String bgdId) {
    super(hostAndPort, role);
    this.bgdId = bgdId;
  }

  @Override
  public Connection apply(
      ConnectionPlugin plugin,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc,
      PluginService pluginService)
      throws SQLException {

    LOGGER.finest("Blue/Green Deployment switchover is in progress. Hold 'connect' call until switchover is completed.");

    TelemetryFactory telemetryFactory = pluginService.getTelemetryFactory();

    long timeoutNano = TimeUnit.MILLISECONDS.toNanos(BG_CONNECT_TIMEOUT.getLong(props));
    long holdStartTime = this.getNanoTime();
    long holdEndTime;

    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_SWITCHOVER,
        TelemetryTraceLevel.NESTED);

    BlueGreenStatus bgStatus = pluginService.getStatus(BlueGreenStatus.class, this.bgdId);

    try {
      long endTime = this.getNanoTime() + timeoutNano;

      while (this.getNanoTime() <= endTime
          && bgStatus != null
          && bgStatus.getCurrentPhase() == BlueGreenPhases.IN_PROGRESS) {

        try {
          this.delay(100, bgStatus, pluginService, this.bgdId);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        bgStatus = pluginService.getStatus(BlueGreenStatus.class, this.bgdId);
      }

      holdEndTime = this.getNanoTime();

      if (bgStatus != null && bgStatus.getCurrentPhase() == BlueGreenPhases.IN_PROGRESS) {
        throw new SQLTimeoutException(
            String.format(
                "Blue/Green Deployment switchover is still in progress after %d ms. Try connect again later.",
                BG_CONNECT_TIMEOUT.getLong(props)));
      }
      LOGGER.finest(String.format(
          "Blue/Green Deployment switchover is completed. Continue with connect call. The call was held for %d ms.",
          TimeUnit.NANOSECONDS.toMillis(holdEndTime - holdStartTime)));

    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }

    // returning no connection so a next routing can handle it
    return null;
  }
}
