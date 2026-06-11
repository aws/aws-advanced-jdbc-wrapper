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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;

/**
 * GDB-specific implementation of {@link MonitoringConnectionHandler} that uses
 * {@link GdbMonitoringConnectionPriority} with region and primary/secondary awareness.
 */
public class GdbMonitoringConnectionHandler
    extends AbstractMonitoringConnectionHandler<GdbMonitoringConnectionPriority> {

  private static final Logger LOGGER = Logger.getLogger(GdbMonitoringConnectionHandler.class.getName());

  public static final AwsWrapperProperty GDB_MONITORING_CONNECTION_PRIORITY =
      new AwsWrapperProperty(
          "gdbMonitoringConnectionPriority", "strict-writer-primary",
          "Comma-separated list of monitoring connection priorities for Global Aurora Databases, "
              + "in order of preference. Possible values: strict-writer-primary, strict-reader-primary, "
              + "strict-reader-secondary, strict-writer-<region>, strict-reader-<region>, <region>.",
          false,
          null);

  static {
    PropertyDefinition.registerPluginProperties(GdbMonitoringConnectionHandler.class);
  }

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected final AtomicReference<HostSpec> writerHostSpec;

  public GdbMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties,
      final AtomicReference<HostSpec> writerHostSpec) {
    this(monitoringConnection, pluginService, topologyUtils, properties, monitoringProperties, writerHostSpec, null);
  }

  public GdbMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties,
      final AtomicReference<HostSpec> writerHostSpec,
      final @Nullable Runnable upgradeReadyNotifier) {
    super(
        monitoringConnection,
        pluginService,
        topologyUtils,
        monitoringProperties,
        GdbMonitoringConnectionPriority.parseList(GDB_MONITORING_CONNECTION_PRIORITY.getString(properties)),
        upgradeReadyNotifier);
    this.writerHostSpec = writerHostSpec;
  }

  /**
   * GDB needs to evaluate priorities relative to the cluster's primary region. The base implementation calls
   * {@link AbstractMonitoringConnectionHandler#getPriorityIndex(HostSpec, boolean)} which doesn't carry region
   * context, so {@code acceptConnections} is overridden to allow the caller's writer host to seed the primary
   * region (used during panic-mode resolution where the newly detected writer may differ from the cached one).
   */
  @Override
  public @Nullable HostSpec acceptConnections(
      Map<HostSpec, AtomicConnection> connections,
      @Nullable HostSpec writerHostSpecOverride,
      List<HostSpec> topology) {
    if (connections == null || connections.isEmpty()) {
      return null;
    }

    // The primary region is determined by the just-detected writer (if any), else fall back to cached writer.
    final String primaryRegion = writerHostSpecOverride != null
        ? this.rdsUtils.getRdsRegion(writerHostSpecOverride.getHost())
        : getPrimaryRegion();

    HostSpec bestHost = null;
    int bestIndex = Integer.MAX_VALUE;
    for (Map.Entry<HostSpec, AtomicConnection> entry : connections.entrySet()) {
      final HostSpec host = entry.getKey();
      final AtomicConnection atomicConn = entry.getValue();
      if (atomicConn == null || atomicConn.get() == null) {
        continue;
      }
      final boolean isWriter = writerHostSpecOverride != null
          && writerHostSpecOverride.getHostAndPort().equals(host.getHostAndPort());
      final int effectiveIndex = effectiveIndex(determinePriorityIndex(host, isWriter, primaryRegion));
      if (bestHost == null || effectiveIndex < bestIndex) {
        bestIndex = effectiveIndex;
        bestHost = host;
      }
    }

    if (bestHost == null) {
      return null;
    }

    final AtomicConnection bestAtomic = connections.get(bestHost);
    final Connection bestConn = bestAtomic.get();
    bestAtomic.set(null, false);
    this.monitoringConnection.set(bestConn);
    this.currentPriorityIndex = bestIndex;
    final HostSpec acceptedHost = bestHost;
    final int acceptedIndex = bestIndex;
    LOGGER.fine(() -> Messages.get(
        "ClusterTopologyMonitorImpl.connectionAccepted",
        new Object[]{acceptedHost.getHost(), acceptedHost.getRole(), formatPriorityIndex(acceptedIndex)}));
    return bestHost;
  }

  @Override
  protected int getPriorityIndex(HostSpec host, boolean isWriter) {
    return determinePriorityIndex(host, isWriter, getPrimaryRegion());
  }

  @Override
  protected List<HostSpec> findHostsForPriority(int priorityIndex, List<HostSpec> hosts) {
    return this.priorities.get(priorityIndex).findMatchingHosts(hosts, getPrimaryRegion(), this.rdsUtils);
  }

  @Override
  protected String getUpgradeThreadName() {
    return "gatmu";
  }

  @Override
  protected @Nullable List<Pair<String, Object>> getAdditionalSnapshotState() {
    final List<Pair<String, Object>> extra = new ArrayList<>();
    extra.add(Pair.create("primaryRegion", getPrimaryRegion()));
    return extra;
  }

  protected @Nullable String getPrimaryRegion() {
    final HostSpec writer = this.writerHostSpec.get();
    if (writer != null) {
      return this.rdsUtils.getRdsRegion(writer.getHost());
    }
    return null;
  }

  protected int determinePriorityIndex(
      final @NonNull HostSpec hostSpec, boolean isWriter, final @Nullable String primaryRegion) {
    final HostSpec effectiveHost = new HostSpec(hostSpec, isWriter ? HostRole.WRITER : HostRole.READER);
    for (int i = 0; i < this.priorities.size(); i++) {
      if (this.priorities.get(i).isSatisfiedBy(effectiveHost, primaryRegion, this.rdsUtils)) {
        return i;
      }
    }
    return -1;
  }
}
