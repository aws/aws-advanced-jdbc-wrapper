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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.RdsHostListProvider;
import software.amazon.jdbc.hostlistprovider.monitoring.MonitoringRdsHostListProvider;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class MultiAzDialectUtils {
  private static final Logger LOGGER = Logger.getLogger(MultiAzDialectUtils.class.getName());
  private final String instanceIdQuery;
  private final String writerIdQuery;
  private final String writerIdQueryColumn;

  public MultiAzDialectUtils(String instanceIdQuery, String writerIdQuery, String writerIdQueryColumn) {
    this.instanceIdQuery = instanceIdQuery;
    this.writerIdQuery = writerIdQuery;
    this.writerIdQueryColumn = writerIdQueryColumn;
  }

  public @Nullable List<TopologyQueryHostSpec> processTopologyResults(Connection conn, ResultSet resultSet)
      throws SQLException {
    List<TopologyQueryHostSpec> hosts = new ArrayList<>();
    while (resultSet.next()) {
      try {
        final TopologyQueryHostSpec host = createHost(resultSet, this.getWriterId(conn));
        hosts.add(host);
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get("MultiAzDialectUtils.errorProcessingQueryResults", new Object[]{e.getMessage()}));
        return null;
      }
    }

    return hosts;
  }

  protected TopologyQueryHostSpec createHost(
      final ResultSet resultSet,
      final @Nullable String writerId) throws SQLException {

    String endpoint = resultSet.getString("endpoint"); // "instance-name.XYZ.us-west-2.rds.amazonaws.com"
    String instanceName = endpoint.substring(0, endpoint.indexOf(".")); // "instance-name"
    String hostId = resultSet.getString("id"); // "1034958454"
    final boolean isWriter = hostId.equals(writerId);

    return new TopologyQueryHostSpec(instanceName, isWriter, 0, Timestamp.from(Instant.now()));
  }

  protected @Nullable String getWriterId(Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.writerIdQuery)) {
        if (resultSet.next()) {
          String writerId = resultSet.getString(this.writerIdQueryColumn);
          if (!StringUtils.isNullOrEmpty(writerId)) {
            return writerId;
          }
        }
      }

      // Replica status doesn't exist, which means that this instance is a writer. We execute instanceIdQuery to get the
      // ID of this writer instance.
      try (final ResultSet resultSet = stmt.executeQuery(this.instanceIdQuery)) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    }
    return null;
  }

  public boolean isWriterInstance(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.writerIdQuery)) {
        if (resultSet.next()) {
          String instanceId = resultSet.getString(this.writerIdQueryColumn);
          // The writer ID is only returned when connected to a reader, so if the query does not return a value, it
          // means we are connected to a writer.
          return StringUtils.isNullOrEmpty(instanceId);
        }
      }
    }
    return false;
  }

  protected HostListProviderSupplier getHostListProviderSupplier(TopologyDialect dialect) {
    return (properties, initialUrl, servicesContainer) -> {
      final PluginService pluginService = servicesContainer.getPluginService();
      if (pluginService.isPluginInUse(FailoverConnectionPlugin.class)) {
        return new MonitoringRdsHostListProvider(dialect, properties, initialUrl, servicesContainer);

      } else {
        return new RdsHostListProvider(dialect, properties, initialUrl, servicesContainer);
      }
    };
  }
}
