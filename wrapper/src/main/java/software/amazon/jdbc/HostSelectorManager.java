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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

public class HostSelectorManager {
  private static final Logger LOGGER = Logger.getLogger(HostSelectorManager.class.getName());
  private final PluginService pluginService;
  private HostSelector selector;

  public HostSelectorManager(PluginService pluginService) {
    this.pluginService = pluginService;
    this.selector = new RandomHostSelector();
  }

  public void setSelector(HostSelector selector) {
    this.selector = selector;
  }

  public HostSpec getHost(HostRole role) throws SQLException {
    List<HostSpec> hosts = refreshHostList();
    return this.selector.getHost(hosts, role);
  }

  private List<HostSpec> refreshHostList() throws SQLException {
    Connection conn = this.pluginService.getCurrentConnection();
    try {
      if (conn != null && !conn.isClosed()) {
        this.pluginService.refreshHostList(conn);
      }
    } catch (SQLException e) {
      // do nothing
    }

    List<HostSpec> hosts = this.pluginService.getHosts();
    if (hosts.size() < 1) {
      throw new SQLException(Messages.get("HostSelectorManager.noHostsAvailable"));
    }
    return hosts;
  }

  public List<HostSpec> getHosts(HostRole role) throws SQLException {
    List<HostSpec> hosts = refreshHostList();
    return this.selector.getHostsByPriority(hosts, role);
  }

  public ConnectionResult getConnectionByPriority(HostRole role, Properties props) throws SQLException {
    List<HostSpec> hostsByPriority = this.selector.getHostsByPriority(refreshHostList(), role);
    return getConnection(props, hostsByPriority);
  }

  private ConnectionResult getConnection(Properties props, List<HostSpec> hostsByPriority)
      throws SQLException {
    for (HostSpec hostSpec : hostsByPriority) {
      try {
        Connection conn = this.pluginService.connect(hostSpec, props);
        return new ConnectionResult(conn, hostSpec);
      } catch (SQLException e) {
        LOGGER.config(() -> Messages.get("HostSelectorManager.failedToConnectToHost",
            new Object[] {hostSpec.getUrl()}));
      }
    }
    throw new SQLException(Messages.get("HostSelectorManager.failedToConnect"));
  }

  public ConnectionResult getConnectionByPriority(List<HostSpec> excludeList, HostRole role, Properties props) throws SQLException {
    List<HostSpec> hostsByPriority = this.selector.getHostsByPriority(refreshHostList(), excludeList, role);
    return getConnection(props, hostsByPriority);
  }
}
