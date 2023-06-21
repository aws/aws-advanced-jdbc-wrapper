package software.amazon.jdbc.util.telemetry;

public enum TelemetryTraceLevel {
  FORCE_TOP_LEVEL, // always top level despite settings
  TOP_LEVEL, // if allowed by settings
  NESTED,
  NO_TRACE // post no trace
}
