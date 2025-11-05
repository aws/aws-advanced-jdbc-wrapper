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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class AuroraTopologyUtils extends TopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(AuroraTopologyUtils.class.getName());

  public AuroraTopologyUtils(TopologyDialect dialect, HostSpecBuilder hostSpecBuilder) {
    super(dialect, hostSpecBuilder);
  }

  @Override
  protected @Nullable List<HostSpec> getHosts(
      Connection conn, ResultSet rs, HostSpec initialHostSpec, HostSpec hostTemplate) throws SQLException {
    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    List<HostSpec> hosts = new ArrayList<>();
    while (rs.next()) {
      try {
        hosts.add(createHost(rs, initialHostSpec, hostTemplate));
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get("AuroraDialectUtils.errorProcessingQueryResults", new Object[]{e.getMessage()}));
        return null;
      }
    }

    return hosts;
  }

  @Override
  public boolean isWriterInstance(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.dialect.getWriterIdQuery())) {
        if (resultSet.next()) {
          return !StringUtils.isNullOrEmpty(resultSet.getString(1));
        }
      }
    }

    return false;
  }

  protected HostSpec createHost(ResultSet rs, HostSpec initialHostSpec, HostSpec hostTemplate) throws SQLException {

    // According to the topology query the result set
    // should contain 4 columns: node ID, 1/0 (writer/reader), CPU utilization, node lag in time.
    String hostName = rs.getString(1);
    final boolean isWriter = rs.getBoolean(2);
    final double cpuUtilization = rs.getDouble(3);
    final double nodeLag = rs.getDouble(4);
    Timestamp lastUpdateTime;
    try {
      lastUpdateTime = rs.getTimestamp(5);
    } catch (Exception e) {
      lastUpdateTime = Timestamp.from(Instant.now());
    }

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L + Math.round(cpuUtilization);

    return createHost(hostName, hostName, isWriter, weight, lastUpdateTime, initialHostSpec, hostTemplate);
  }
}
