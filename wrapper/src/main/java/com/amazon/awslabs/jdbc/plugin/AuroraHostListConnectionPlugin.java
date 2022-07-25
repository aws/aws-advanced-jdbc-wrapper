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

package com.amazon.awslabs.jdbc.plugin;

import com.amazon.awslabs.jdbc.HostListProvider;
import com.amazon.awslabs.jdbc.HostListProviderService;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.PluginService;
import com.amazon.awslabs.jdbc.hostlistprovider.AuroraHostListProvider;
import com.amazon.awslabs.jdbc.util.Messages;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class AuroraHostListConnectionPlugin extends AbstractConnectionPlugin {

  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(
      Collections.singletonList("initHostProvider")));
  private final PluginService pluginService;

  public AuroraHostListConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
  }

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
          new AuroraHostListProvider(driverProtocol, pluginService, props, initialUrl));
    } else if (!(provider instanceof AuroraHostListProvider)) {
      throw new SQLException(Messages.get("Failover.invalidHostListProvider", new Object[] {provider}));
    }
    initHostProviderFunc.call();
  }
}
