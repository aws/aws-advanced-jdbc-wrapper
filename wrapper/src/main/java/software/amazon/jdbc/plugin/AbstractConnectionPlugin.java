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
import java.util.EnumSet;
import java.util.List;
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

public abstract class AbstractConnectionPlugin implements ConnectionPlugin {

  public abstract Set<String> getSubscribedMethods();

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    return jdbcMethodFunc.call();
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectFunc.call();
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return forceConnectFunc.call();
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    return false;
  }

  @Override
  public HostSpec getHostSpecByStrategy(final HostRole role, final String strategy)
      throws SQLException, UnsupportedOperationException {
    throw new UnsupportedOperationException("getHostSpecByStrategy is not supported by this plugin.");
  }

  @Override
  public HostSpec getHostSpecByStrategy(final List<HostSpec> hosts, final HostRole role, final String strategy)
      throws SQLException, UnsupportedOperationException {
    throw new UnsupportedOperationException("getHostSpecByStrategy is not supported by this plugin.");
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {

    initHostProviderFunc.call();
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(final EnumSet<NodeChangeOptions> changes) {
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {}
}
