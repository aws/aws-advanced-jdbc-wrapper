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

  /**
   * The method returns a driver for specified url. If driver couldn't be found,
   * the method tries to identify a driver that corresponds to an url and register it.
   * Registration of the driver could be disabled by provided configuration properties.
   * If driver couldn't be found and couldn't be registered, the method raises an exception.
   *
   * @param driverUrl A driver connection string.
   * @param props Connection properties
   * @return A target driver
   *
   * @throws SQLException when a driver couldn't be found.
   */
  public java.sql.Driver getTargetDriver(
      final @NonNull String driverUrl,
      final @NonNull Properties props)
      throws SQLException {

    final ConnectionUrlParser parser = new ConnectionUrlParser();
    final String protocol = parser.getProtocol(driverUrl);

    TargetDriverDialectManager targetDriverDialectManager = new TargetDriverDialectManager();
    java.sql.Driver targetDriver = null;
    SQLException lastException = null;

    // Try to get a driver that can handle this url.
    try {
      targetDriver = DriverManager.getDriver(driverUrl);
    } catch (SQLException e) {
      lastException = e;
    }

    // If the driver isn't found, it's possible to register a driver that corresponds to the protocol
    // and try again.
    if (targetDriver == null) {
      boolean triedToRegister = targetDriverDialectManager.registerDriver(protocol, props);
      if (triedToRegister) {
        // There was an attempt to register a corresponding to the protocol driver. Try to find the driver again.
        try {
          targetDriver = DriverManager.getDriver(driverUrl);
        } catch (SQLException e) {
          lastException = e;
        }
      }
    }

    // The driver is not found yet. Let's raise an exception.
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
