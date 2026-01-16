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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.MultiAzClusterDialect;
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
      Connection conn, ResultSet rs, HostSpec initialHostSpec, HostSpec instanceTemplate)
      throws SQLException {
    String writerId = this.getWriterId(conn);

    // Data in the result set is ordered by last update time, so the latest records are last.
    // We add hosts to a map to ensure newer records are not overwritten by older ones.
    Map<String, HostSpec> hostsMap = new HashMap<>();
    while (rs.next()) {
      try {
        HostSpec host = createHost(rs, initialHostSpec, instanceTemplate, writerId);
        hostsMap.put(host.getHost(), host);
      } catch (Exception e) {
        LOGGER.finest(
            Messages.get(
                "TopologyUtils.errorProcessingQueryResults", new Object[] {e.getMessage()}));
        return null;
      }
    }

    return new ArrayList<>(hostsMap.values());
  }

  @Override
  public boolean isWriterInstance(final Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {

      try (final ResultSet rs = stmt.executeQuery(this.dialect.getWriterIdQuery())) {
        // When connected to a writer, the result is empty, otherwise it contains a single row.
        return !rs.next();
      }
    }
  }

  protected @Nullable String getWriterId(Connection connection) throws SQLException {
    try (final Statement stmt = connection.createStatement()) {
      try (final ResultSet rs = stmt.executeQuery(this.dialect.getWriterIdQuery())) {
        if (rs.next()) {
          String writerId = rs.getString(this.dialect.getWriterIdColumnName());
          if (!StringUtils.isNullOrEmpty(writerId)) {
            return writerId;
          }
        }
      }

      // The writer ID is only returned when connected to a reader, so if the query does not return
      // a value, it
      // means we are connected to a writer
      try (final ResultSet rs = stmt.executeQuery(this.dialect.getInstanceIdQuery())) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    }

    return null;
  }

  protected HostSpec createHost(
      final ResultSet rs,
      final HostSpec initialHostSpec,
      final HostSpec instanceTemplate,
      final @Nullable String writerId)
      throws SQLException {

    String endpoint = rs.getString("endpoint"); // "instance-name.XYZ.us-west-2.rds.amazonaws.com"
    String instanceName = endpoint.substring(0, endpoint.indexOf(".")); // "instance-name"
    String hostId = rs.getString("id"); // "1034958454"
    final boolean isWriter = hostId.equals(writerId);

    return createHost(
        hostId,
        instanceName,
        isWriter,
        0,
        Timestamp.from(Instant.now()),
        initialHostSpec,
        instanceTemplate);
  }
}
