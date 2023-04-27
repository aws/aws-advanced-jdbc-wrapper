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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;

public class TestPluginOne implements ConnectionPlugin {

  protected Set<String> subscribedMethods;
  protected ArrayList<String> calls;

  TestPluginOne() {}

  public TestPluginOne(ArrayList<String> calls) {
    this.calls = calls;

    this.subscribedMethods = new HashSet<>(Arrays.asList("*"));
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return this.subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {

    this.calls.add(this.getClass().getSimpleName() + ":before");

    T result;
    try {
      result = jdbcMethodFunc.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (exceptionClass.isInstance(e)) {
        throw exceptionClass.cast(e);
      }
      throw new RuntimeException(e);
    }

    this.calls.add(this.getClass().getSimpleName() + ":after");

    return result;
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
    Connection result = connectFunc.call();
    this.calls.add(this.getClass().getSimpleName() + ":after connect");
    return result;
  }

  @Override
  public Connection forceConnect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {

    this.calls.add(this.getClass().getSimpleName() + ":before forceConnect");
    Connection result = forceConnectFunc.call();
    this.calls.add(this.getClass().getSimpleName() + ":after forceConnect");
    return result;
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    return false;
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) {
    this.calls.add(this.getClass().getSimpleName() + ":before getHostSpecByStrategy");
    HostSpec result = new HostSpec("host", 1234, role);
    this.calls.add(this.getClass().getSimpleName() + ":after getHostSpecByStrategy");
    return result;
  }

  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {

    // do nothing
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    // do nothing
  }
}
