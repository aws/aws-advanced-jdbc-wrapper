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

package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.util.Messages;

public class ConnectTimeConnectionPlugin extends AbstractConnectionPlugin {

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("connect", "forceConnect")));
  private static long connectTime = 0L;

  private static final Logger LOGGER =
      Logger.getLogger(ConnectTimeConnectionPlugin.class.getName());

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    final long startTime = System.nanoTime();

    final Connection result = connectFunc.call();

    final long elapsedTimeNanos = System.nanoTime() - startTime;
    connectTime += elapsedTimeNanos;
    LOGGER.fine(
        () -> Messages.get(
            "ConnectTimeConnectionPlugin.connectTime",
            new Object[] {elapsedTimeNanos}));
    return result;
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    final long startTime = System.nanoTime();

    final Connection result = forceConnectFunc.call();

    final long elapsedTimeNanos = System.nanoTime() - startTime;
    LOGGER.fine(
        () -> Messages.get(
            "ConnectTimeConnectionPlugin.connectTime",
            new Object[] {elapsedTimeNanos}));
    connectTime += elapsedTimeNanos;

    return result;
  }

  public static void resetConnectTime() {
    connectTime = 0L;
  }

  public static long getTotalConnectTime() {
    return connectTime;
  }
}
