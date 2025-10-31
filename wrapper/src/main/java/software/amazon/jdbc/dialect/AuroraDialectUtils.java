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
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class AuroraDialectUtils {

  protected final String writerIdQuery;

  private static final Logger LOGGER = Logger.getLogger(AuroraDialectUtils.class.getName());

  public AuroraDialectUtils(String writerIdQuery) {
    this.writerIdQuery = writerIdQuery;
  }

  public @Nullable List<TopologyQueryHostSpec> processTopologyResults(ResultSet resultSet)
      throws SQLException {
    if (resultSet.getMetaData().getColumnCount() == 0) {
      // We expect at least 4 columns. Note that the server may return 0 columns if failover has occurred.
      LOGGER.finest(Messages.get("AuroraDialectUtils.unexpectedTopologyQueryColumnCount"));
      return null;
    }

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    List<TopologyQueryHostSpec> hosts = new ArrayList<>();
    while (resultSet.next()) {
      try {
        hosts.add(createHost(resultSet));
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get("AuroraDialectUtils.errorProcessingQueryResults", new Object[]{e.getMessage()}));
        return null;
      }
    }

    return hosts;
  }

  protected TopologyQueryHostSpec createHost(final ResultSet resultSet) throws SQLException {
    // According to the topology query the result set should contain 4 columns:
    // instance ID, 1/0 (writer/reader), CPU utilization, instance lag
    String hostName = resultSet.getString(1);
    final boolean isWriter = resultSet.getBoolean(2);
    final float cpuUtilization = resultSet.getFloat(3);
    final float instanceLag = resultSet.getFloat(4);
    Timestamp lastUpdateTime;
    try {
      lastUpdateTime = resultSet.getTimestamp(5);
    } catch (Exception e) {
      lastUpdateTime = Timestamp.from(Instant.now());
    }

    // Calculate weight based on instance lag in time and CPU utilization.
    final long weight = Math.round(instanceLag) * 100L + Math.round(cpuUtilization);

    return new TopologyQueryHostSpec(hostName, isWriter, weight, lastUpdateTime);
  }

  public boolean isWriterInstance(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.writerIdQuery)) {
        if (resultSet.next()) {
          return !StringUtils.isNullOrEmpty(resultSet.getString(1));
        }
      }
    }

    return false;
  }
}
