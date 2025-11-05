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
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.MultiAzClusterDialect;
import software.amazon.jdbc.dialect.TopologyDialect;
import software.amazon.jdbc.dialect.TopologyQueryHostSpec;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class MultiAzTopologyUtils extends TopologyUtils {
  private static final Logger LOGGER = Logger.getLogger(MultiAzTopologyUtils.class.getName());

  protected final MultiAzClusterDialect dialect;

  public MultiAzTopologyUtils(MultiAzClusterDialect dialect, HostSpecBuilder hostSpecBuilder) {
    super(dialect, hostSpecBuilder);
    this.dialect = dialect;
  }

  @Override
  protected @Nullable List<HostSpec> getHosts(
      Connection conn, ResultSet rs, HostSpec initialHostSpec, HostSpec hostTemplate)
      throws SQLException {
    String writerId = this.getWriterId(conn);

    // Data is result set is ordered by last updated time so the latest records go last.
    // When adding hosts to a map, the newer records replace the older ones.
    List<HostSpec> hosts = new ArrayList<>();
    while (rs.next()) {
      try {
        hosts.add(createHost(rs, initialHostSpec, hostTemplate, writerId));
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
          String instanceId = resultSet.getString(this.dialect.getWriterIdColumnName());
          // The writer ID is only returned when connected to a reader, so if the query does not return a value, it
          // means we are connected to a writer.
          return StringUtils.isNullOrEmpty(instanceId);
        }
      }
    }
    return false;
  }


  protected @Nullable String getWriterId(Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet resultSet = stmt.executeQuery(this.dialect.getWriterIdQuery())) {
        if (resultSet.next()) {
          String writerId = resultSet.getString(this.dialect.getWriterIdColumnName());
          if (!StringUtils.isNullOrEmpty(writerId)) {
            return writerId;
          }
        }
      }

      // Replica status doesn't exist, which means that this instance is a writer. We execute instanceIdQuery to get the
      // ID of this writer instance.
      try (final ResultSet resultSet = stmt.executeQuery(this.dialect.getInstanceIdQuery())) {
        if (resultSet.next()) {
          return resultSet.getString(1);
        }
      }
    }
    return null;
  }

  protected HostSpec createHost(
      final ResultSet resultSet,
      final HostSpec initialHostSpec,
      final HostSpec hostTemplate,
      final @Nullable String writerId) throws SQLException {

    String endpoint = resultSet.getString("endpoint"); // "instance-name.XYZ.us-west-2.rds.amazonaws.com"
    String instanceName = endpoint.substring(0, endpoint.indexOf(".")); // "instance-name"
    String hostId = resultSet.getString("id"); // "1034958454"
    final boolean isWriter = hostId.equals(writerId);

    return createHost(hostId, instanceName, isWriter, 0, Timestamp.from(Instant.now()), initialHostSpec, hostTemplate);
  }
}
