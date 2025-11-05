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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.GlobalAuroraTopologyDialect;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class GlobalAuroraTopologyUtils extends AuroraTopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(GlobalAuroraTopologyUtils.class.getName());

  protected final GlobalAuroraTopologyDialect dialect;

  public GlobalAuroraTopologyUtils(GlobalAuroraTopologyDialect dialect, HostSpecBuilder hostSpecBuilder) {
    super(dialect, hostSpecBuilder);
    this.dialect = dialect;
  }

  public @Nullable List<HostSpec> queryForTopology(
      Connection conn, HostSpec initialHostSpec, Map<String, HostSpec> instanceTemplatesByRegion)
      throws SQLException {
    int networkTimeout = setNetworkTimeout(conn);
    try (final Statement stmt = conn.createStatement();
         final ResultSet resultSet = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      if (resultSet.getMetaData().getColumnCount() == 0) {
        // We expect at least 4 columns. Note that the server may return 0 columns if failover has occurred.
        LOGGER.finest(Messages.get("AuroraDialectUtils.unexpectedTopologyQueryColumnCount"));
        return null;
      }

      return this.verifyWriter(this.getHosts(conn, resultSet, initialHostSpec, instanceTemplatesByRegion));
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  protected @Nullable List<HostSpec> getHosts(
      Connection conn, ResultSet rs, HostSpec initialHostSpec, Map<String, HostSpec> instanceTemplatesByRegion)
      throws SQLException {
    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    List<HostSpec> hosts = new ArrayList<>();
    while (rs.next()) {
      try {
        hosts.add(createHost(rs, initialHostSpec, instanceTemplatesByRegion));
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get("AuroraDialectUtils.errorProcessingQueryResults", new Object[]{e.getMessage()}));
        return null;
      }
    }

    return hosts;
  }

  protected HostSpec createHost(
      ResultSet rs, HostSpec initialHostSpec, Map<String, HostSpec> instanceTemplatesByRegion)
      throws SQLException {
    // According to the topology query the result set
    // should contain 4 columns: node ID, 1/0 (writer/reader), node lag in time (msec), AWS region.
    String hostName = rs.getString(1);
    final boolean isWriter = rs.getBoolean(2);
    final float nodeLag = rs.getFloat(3);
    final String awsRegion = rs.getString(4);

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L;

    final HostSpec clusterInstanceTemplateForRegion = instanceTemplatesByRegion.get(awsRegion);
    if (clusterInstanceTemplateForRegion == null) {
      throw new SQLException("Can't find cluster template for region " + awsRegion);
    }

    return createHost(hostName, hostName, isWriter, weight, Timestamp.from(Instant.now()), initialHostSpec, clusterInstanceTemplateForRegion);
  }

  public @Nullable String getRegion(String instanceId, Connection conn) throws SQLException {
    try (final PreparedStatement stmt = conn.prepareStatement(this.dialect.getRegionByInstanceIdQuery())) {
      stmt.setString(1, instanceId);
      try (final ResultSet resultSet = stmt.executeQuery()) {
        if (resultSet.next()) {
          String awsRegion = resultSet.getString(1);
          return StringUtils.isNullOrEmpty(awsRegion) ? null : awsRegion;
        }
      }
    }

    return null;
  }
}
