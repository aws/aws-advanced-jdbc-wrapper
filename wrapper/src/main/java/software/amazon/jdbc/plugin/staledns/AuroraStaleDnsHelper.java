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

package software.amazon.jdbc.plugin.staledns;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.Utils;

public class AuroraStaleDnsHelper {

  private static final Logger LOGGER = Logger.getLogger(AuroraStaleDnsHelper.class.getName());

  private final PluginService pluginService;
  private final RdsUtils rdsUtils = new RdsUtils();

  private HostSpec writerHostSpec = null;
  private InetAddress writerHostAddress = null;

  public AuroraStaleDnsHelper(PluginService pluginService) {
    this.pluginService = pluginService;
  }

  public Connection getVerifiedConnection(
      HostSpec hostSpec,
      Properties props,
      JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    if (!this.rdsUtils.isWriterClusterDns(hostSpec.getHost())) {
      return connectFunc.call();
    }

    final Connection conn = connectFunc.call();

    InetAddress clusterInetAddress = null;
    try {
      clusterInetAddress = InetAddress.getByName(hostSpec.getHost());
    } catch (UnknownHostException e) {
      // ignore
    }

    final InetAddress hostInetAddress = clusterInetAddress;
    LOGGER.finest(() -> Messages.get("AuroraStaleDnsHelper.clusterEndpointDns",
        new Object[]{hostInetAddress}));

    if (clusterInetAddress == null) {
      return conn;
    }

    this.pluginService.refreshHostList(conn);
    LOGGER.finest(() -> Utils.logTopology(this.pluginService.getHosts()));

    if (this.writerHostSpec == null) {
      final HostSpec writerCandidate = this.getWriter();
      if (writerCandidate != null && this.rdsUtils.isRdsClusterDns(writerCandidate.getHost())) {
        return null;
      }
      this.writerHostSpec = writerCandidate;
    }

    LOGGER.finest(() -> Messages.get("AuroraStaleDnsHelper.writerHostSpec",
        new Object[]{this.writerHostSpec}));

    if (this.writerHostSpec == null) {
      return conn;
    }

    if (this.writerHostAddress == null) {
      try {
        this.writerHostAddress = InetAddress.getByName(this.writerHostSpec.getHost());
      } catch (UnknownHostException e) {
        // ignore
      }
    }

    LOGGER.finest(() -> Messages.get("AuroraStaleDnsHelper.writerInetAddress",
        new Object[]{this.writerHostAddress}));

    if (this.writerHostAddress == null) {
      return conn;
    }

    if (!writerHostAddress.equals(clusterInetAddress)) {
      // DNS resolves a cluster endpoint to a wrong writer
      // opens a connection to a proper writer node

      LOGGER.fine(() -> Messages.get("AuroraStaleDnsHelper.staleDnsDetected",
          new Object[]{this.writerHostSpec}));

      Connection writerConn = this.pluginService.connect(this.writerHostSpec, props);

      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException ex) {
          // ignore
        }
        return writerConn;
      }
    }

    return conn;
  }

  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    if (this.writerHostSpec == null) {
      return;
    }

    for (Map.Entry<String, EnumSet<NodeChangeOptions>> entry : changes.entrySet()) {
      LOGGER.finest(() -> String.format("[%s]: %s", entry.getKey(), entry.getValue()));
      if (entry.getKey().equals(this.writerHostSpec.getUrl())
          && entry.getValue().contains(NodeChangeOptions.PROMOTED_TO_READER)) {
        LOGGER.finest(() -> Messages.get("AuroraStaleDnsHelper.reset"));
        this.writerHostSpec = null;
        this.writerHostAddress = null;
      }
    }
  }

  private HostSpec getWriter() {
    for (HostSpec host : this.pluginService.getHosts()) {
      if (host.getRole() == HostRole.WRITER) {
        return host;
      }
    }
    return null;
  }
}
