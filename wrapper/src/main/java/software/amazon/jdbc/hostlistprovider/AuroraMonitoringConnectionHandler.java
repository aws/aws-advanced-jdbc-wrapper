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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AtomicConnection;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;

/**
 * Aurora implementation of {@link MonitoringConnectionHandler} that uses
 * {@link MonitoringConnectionPriority} to manage monitoring connection preferences.
 */
public class AuroraMonitoringConnectionHandler extends
    AbstractMonitoringConnectionHandler<MonitoringConnectionPriority> {

  public static final AwsWrapperProperty MONITORING_CONNECTION_PRIORITY =
      new AwsWrapperProperty(
          "monitoringConnectionPriority", "strict-writer",
          "Comma-separated list of monitoring connection priorities in order of preference. "
              + "The monitor will try to establish a connection matching the highest priority first. "
              + "Possible values: strict-writer, strict-reader, writer-or-reader.",
          false,
          new String[] {
              "strict-writer", "strict-reader", "writer-or-reader"
          });

  static {
    PropertyDefinition.registerPluginProperties(AuroraMonitoringConnectionHandler.class);
  }

  protected final int writerPriorityIndex;
  protected final int readerPriorityIndex;

  public AuroraMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties) {
    this(monitoringConnection, pluginService, topologyUtils, properties, monitoringProperties, null);
  }

  public AuroraMonitoringConnectionHandler(
      final AtomicConnection monitoringConnection,
      final PluginService pluginService,
      final TopologyUtils topologyUtils,
      final Properties properties,
      final Properties monitoringProperties,
      final @Nullable Runnable upgradeReadyNotifier) {
    super(
        monitoringConnection,
        pluginService,
        topologyUtils,
        monitoringProperties,
        MonitoringConnectionPriority.parseList(MONITORING_CONNECTION_PRIORITY.getString(properties)),
        upgradeReadyNotifier);

    // Precompute priority indices.
    int writerIdx = -1;
    int readerIdx = -1;
    for (int i = 0; i < this.priorities.size(); i++) {
      MonitoringConnectionPriority p = this.priorities.get(i);
      if (writerIdx < 0 && p.isSatisfiedBy(true)) {
        writerIdx = i;
      }
      if (readerIdx < 0 && p.isSatisfiedBy(false)) {
        readerIdx = i;
      }
    }
    this.writerPriorityIndex = writerIdx;
    this.readerPriorityIndex = readerIdx;
  }

  @Override
  protected int getPriorityIndex(HostSpec host, boolean isWriter) {
    return isWriter ? this.writerPriorityIndex : this.readerPriorityIndex;
  }

  @Override
  protected List<HostSpec> findHostsForPriority(int priorityIndex, List<HostSpec> hosts) {
    final MonitoringConnectionPriority priority = this.priorities.get(priorityIndex);
    switch (priority) {
      case STRICT_WRITER:
        return hosts.stream().filter(h -> h.getRole() == HostRole.WRITER).collect(Collectors.toList());
      case STRICT_READER:
        return hosts.stream().filter(h -> h.getRole() == HostRole.READER).collect(Collectors.toList());
      case WRITER_OR_READER:
        return new ArrayList<>(hosts);
      default:
        return new ArrayList<>();
    }
  }

  @Override
  protected String getUpgradeThreadName() {
    return "atmu";
  }
}
