package software.amazon.jdbc.plugin.dev;

import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.HostSpec;

public interface ExceptionSimulatorConnectCallback {
  SQLException getExceptionToRaise(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection);
}
