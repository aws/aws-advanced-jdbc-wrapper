package software.amazon.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialectManager;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;

public class TargetDriverHelper {

  public java.sql.Driver getTargetDriver(
      final @NonNull String driverUrl,
      final @NonNull Properties props)
      throws SQLException {

    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final String protocol = parser.getProtocol(driverUrl);

    TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
    java.sql.Driver targetDriver = null;
    SQLException lastException = null;

    try {
      targetDriver = DriverManager.getDriver(driverUrl);
    } catch (SQLException e) {
      lastException = e;
    }

    if (targetDriver == null) {
      boolean triedToRegister = targetDriverDialectManager.registerDriver(protocol, props);
      if (triedToRegister) {
        try {
          targetDriver = DriverManager.getDriver(driverUrl);
        } catch (SQLException e) {
          lastException = e;
        }
      }
    }

    if (targetDriver == null) {
      final List<String> registeredDrivers = Collections.list(DriverManager.getDrivers())
          .stream()
          .map(x -> x.getClass().getName())
          .collect(Collectors.toList());
      throw new SQLException(
          Messages.get("Driver.missingDriver", new Object[] {driverUrl, registeredDrivers}), lastException);
    }

    return targetDriver;
  }
}
