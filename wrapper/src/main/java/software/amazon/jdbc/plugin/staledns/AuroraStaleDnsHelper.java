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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class AuroraStaleDnsHelper {

  private static final Logger LOGGER = Logger.getLogger(AuroraStaleDnsHelper.class.getName());

  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final TelemetryCounter staleDNSDetectedCounter;

  private final RdsUtils rdsUtils = new RdsUtils();

  private HostSpec writerHostSpec = null;
  private String writerHostAddress = null;

  private static final int RETRIES = 3;

  public AuroraStaleDnsHelper(final PluginService pluginService) {
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.staleDNSDetectedCounter = telemetryFactory.createCounter("staleDNS.stale.detected");
  }

  public Connection getVerifiedConnection(
      final boolean isInitialConnection,
      final HostListProviderService hostListProviderService,
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    if (!this.rdsUtils.isWriterClusterDns(hostSpec.getHost())) {
      return connectFunc.call();
    }

    final Connection conn = connectFunc.call();

    String clusterInetAddress = null;
    try {
      clusterInetAddress = InetAddress.getByName(hostSpec.getHost()).getHostAddress();
    } catch (UnknownHostException e) {
      // ignore
    }

    final String hostInetAddress = clusterInetAddress;
    LOGGER.finest(() -> Messages.get("AuroraStaleDnsHelper.clusterEndpointDns",
        new Object[]{hostInetAddress}));

    if (clusterInetAddress == null) {
      return conn;
    }

    if (this.pluginService.getHostRole(conn) == HostRole.READER) {
      // This is if-statement is only reached if the connection url is a writer cluster endpoint.
      // If the new connection resolves to a reader instance, this means the topology is outdated.
      // Force refresh to update the topology.
      this.pluginService.forceRefreshHostList(conn);
    } else {
      this.pluginService.refreshHostList(conn);
    }

    LOGGER.finest(() -> Utils.logTopology(this.pluginService.getAllHosts()));

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
        this.writerHostAddress = InetAddress.getByName(this.writerHostSpec.getHost()).getHostAddress();
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
      staleDNSDetectedCounter.inc();

      final List<HostSpec> allowedHosts = this.pluginService.getHosts();
      if (!Utils.containsUrl(allowedHosts, this.writerHostSpec.getUrl())) {
        throw new SQLException(
            Messages.get("AuroraStaleDnsHelper.currentWriterNotAllowed",
                new Object[] {
                    this.writerHostSpec == null ? "<null>" : this.writerHostSpec.getUrl(),
                    Utils.logTopology(allowedHosts, "")})
        );
      }

      final Connection writerConn = this.pluginService.connect(this.writerHostSpec, props);
      if (isInitialConnection) {
        hostListProviderService.setInitialConnectionHostSpec(this.writerHostSpec);
      }

      if (conn != null) {
        try {
          conn.close();
        } catch (final SQLException ex) {
          // ignore
        }
        return writerConn;
      }
    }

    return conn;
  }

  public void notifyNodeListChanged(final Map<String, EnumSet<NodeChangeOptions>> changes) {
    if (this.writerHostSpec == null) {
      return;
    }

    for (final Map.Entry<String, EnumSet<NodeChangeOptions>> entry : changes.entrySet()) {
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
    for (final HostSpec host : this.pluginService.getAllHosts()) {
      if (host.getRole() == HostRole.WRITER) {
        return host;
      }
    }
    return null;
  }
}
