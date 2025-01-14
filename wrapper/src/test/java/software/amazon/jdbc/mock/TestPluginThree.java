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

package software.amazon.jdbc.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;

public class TestPluginThree extends TestPluginOne {

  private Connection connection;

  public TestPluginThree(ArrayList<String> calls) {
    super();
    this.calls = calls;

    this.subscribedMethods = new HashSet<>(Arrays.asList("testJdbcCall_A", "connect", "forceConnect"));
  }

  public TestPluginThree(ArrayList<String> calls, Connection connection) {
    this(calls);
    this.connection = connection;
  }

  @Override
  public Connection connect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    this.calls.add(this.getClass().getSimpleName() + ":before connect");

    if (this.connection != null) {
      this.calls.add(this.getClass().getSimpleName() + ":connection");
      return this.connection;
    }

    Connection result = connectFunc.call();
    this.calls.add(this.getClass().getSimpleName() + ":after connect");

    return result;
  }

  public Connection forceConnect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {

    this.calls.add(this.getClass().getSimpleName() + ":before forceConnect");

    if (this.connection != null) {
      this.calls.add(this.getClass().getSimpleName() + ":forced connection");
      return this.connection;
    }

    Connection result = forceConnectFunc.call();
    this.calls.add(this.getClass().getSimpleName() + ":after forceConnect");

    return result;
  }
}
