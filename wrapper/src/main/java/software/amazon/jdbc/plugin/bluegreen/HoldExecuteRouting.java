package software.amazon.jdbc.plugin.bluegreen;

import static software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPlugin.BG_CONNECT_TIMEOUT;

import java.sql.SQLTimeoutException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

// Hold JDBC call execution till BG is completed.
public class HoldExecuteRouting extends BaseExecuteRouting {

  private static final Logger LOGGER = Logger.getLogger(HoldExecuteRouting.class.getName());

  private static final String TELEMETRY_SWITCHOVER = "Blue/Green switchover";

  public HoldExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public <T, E extends Exception> @NonNull Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final PluginService pluginService,
      final Properties props) throws E {

    LOGGER.finest(String.format(
        "Blue/Green Deployment switchover is in progress. Hold '%s' call until switchover is completed.",
        methodName));

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
          && bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        bgStatus = pluginService.getStatus(BlueGreenStatus.class, true);
      }

      holdEndTime = this.getNanoTime();

      if (bgStatus != null && bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass,
            new SQLTimeoutException(
                String.format(
                    "Blue/Green Deployment switchover is still in progress after %d ms. Try '%s' again later.",
                    BG_CONNECT_TIMEOUT.getLong(props), methodName)));
      }
      LOGGER.finest(String.format(
          "Blue/Green Deployment switchover is completed. Continue with '%s' call. The call was held for %d ms.",
          methodName,
          TimeUnit.NANOSECONDS.toMillis(holdEndTime - holdStartTime)));

    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }

    // returning no results so a next routing can handle it
    return Optional.empty();
  }

  protected long getNanoTime() {
    return System.nanoTime();
  }
}
