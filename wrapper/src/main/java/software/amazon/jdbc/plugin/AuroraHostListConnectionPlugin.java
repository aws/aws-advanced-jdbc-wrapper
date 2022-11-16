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

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class AuroraHostListConnectionPlugin extends AbstractConnectionPlugin {

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("initHostProvider")));

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException {
    final HostListProvider provider = hostListProviderService.getHostListProvider();
    if (provider == null) {
      initHostProviderFunc.call();
      return;
    }

    if (hostListProviderService.isStaticHostListProvider()) {
      hostListProviderService.setHostListProvider(
          new AuroraHostListProvider(driverProtocol, hostListProviderService, props, initialUrl));
    } else if (!(provider instanceof AuroraHostListProvider)) {
      throw new SQLException(Messages.get("AuroraHostListConnectionPlugin.providerAlreadySet",
          new Object[] {provider.getClass().getName()}));
    }
    initHostProviderFunc.call();
  }
}
