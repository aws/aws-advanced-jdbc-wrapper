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

package software.amazon.jdbc.benchmarks.testplugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.cleanup.CanReleaseResources;

public class BenchmarkPlugin implements ConnectionPlugin, CanReleaseResources {
  final List<String> resources = new ArrayList<>();

  private static final Logger LOGGER = Logger.getLogger(BenchmarkPlugin.class.getName());

  @Override
  public Set<String> getSubscribedMethods() {
    return new HashSet<>(Collections.singleton("*"));
  }

  @Override
  public <T, E extends Exception> T execute(Class<T> resultClass, Class<E> exceptionClass,
      Object methodInvokeOn, String methodName, JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs) throws E {
    LOGGER.finer(() -> String.format("execute method=''%s''", methodName));
    resources.add("execute");
    return jdbcMethodFunc.call();
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    LOGGER.finer(() -> String.format("connect=''%s''", hostSpec.getHost()));
    resources.add("connect");
    return connectFunc.call();
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    LOGGER.finer(() -> String.format("forceConnect=''%s''", hostSpec.getHost()));
    resources.add("forceConnect");
    return forceConnectFunc.call();
  }

  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    return false;
  }

  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) {
    LOGGER.finer(() -> String.format("getHostSpecByStrategy=''%s''", strategy));
    resources.add("getHostSpecByStrategy");
    return new HostSpec("host", 1234, role);
  }

  @Override
  public void initHostProvider(String driverProtocol, String initialUrl, Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc) {
    LOGGER.finer(() -> String.format("initHostProvider=''%s''", initialUrl));
    resources.add("initHostProvider");
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    LOGGER.finer(() -> String.format("notifyNodeListChanged=''%s''", changes));
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    LOGGER.finer(() -> String.format("notifyNodeListChanged=''%s''", changes));
    resources.add("notifyNodeListChanged");
  }

  @Override
  public void releaseResources() {
    LOGGER.finer(() -> "releasing resources");
    resources.clear();
  }
}
