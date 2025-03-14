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

  public HoldConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
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

    BlueGreenStatus bgStatus = pluginService.getStatus(BlueGreenStatus.class, true);

    try {
      long endTime = this.getNanoTime() + timeoutNano;

      while (this.getNanoTime() <= endTime
          && bgStatus != null
          && bgStatus.getCurrentPhase() == BlueGreenPhases.IN_PROGRESS) {

        try {
          this.delay(100, bgStatus, pluginService);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        bgStatus = pluginService.getStatus(BlueGreenStatus.class, true);
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

  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void delay(long delayMs, BlueGreenStatus bgStatus, PluginService pluginService)
      throws InterruptedException {

    long start = System.nanoTime();
    long end = start + TimeUnit.MILLISECONDS.toNanos(delayMs);
    final BlueGreenStatus currentStatus = bgStatus;
    long minDelay = Math.min(delayMs, 50);

    if (currentStatus == null) {
      TimeUnit.MILLISECONDS.sleep(delayMs);
    } else {
      // Check whether intervalType or stop flag change, or until waited specified delay time.
      do {
        synchronized (currentStatus) {
          currentStatus.wait(minDelay);
        }
      } while (
          // check if status reference is changed
          currentStatus == pluginService.getStatus(BlueGreenStatus.class, true)
          && System.nanoTime() < end
          && !Thread.currentThread().isInterrupted());
    }
  }
}
