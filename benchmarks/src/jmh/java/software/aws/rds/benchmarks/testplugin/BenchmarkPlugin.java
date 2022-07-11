/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package software.aws.rds.benchmarks.testplugin;

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
import java.util.logging.Level;
import java.util.logging.Logger;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.HostListProviderService;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;
import software.aws.rds.jdbc.proxydriver.NodeChangeOptions;
import software.aws.rds.jdbc.proxydriver.OldConnectionSuggestedAction;
import software.aws.rds.jdbc.proxydriver.cleanup.CanReleaseResources;

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
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] execute method=''{0}''", methodName);
    resources.add("execute");
    return jdbcMethodFunc.call();
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] connect=''{0}''", driverProtocol);
    resources.add("connect");
    return connectFunc.call();
  }

  @Override
  public void initHostProvider(String driverProtocol, String initialUrl, Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException {
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] initHostProvider=''{0}''", initialUrl);
    resources.add("initHostProvider");
  }

  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] notifyConnectionChanged=''{0}''", changes);
    return OldConnectionSuggestedAction.NO_OPINION;
  }

  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] notifyNodeListChanged=''{0}''", changes);
    resources.add("notifyNodeListChanged");
  }

  @Override
  public void releaseResources() {
    LOGGER.log(Level.FINER, "[BenchmarkPlugin] releaseResources");
    resources.clear();
  }
}
