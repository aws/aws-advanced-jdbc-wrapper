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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.dialect.GlobalAuroraTopologyDialect;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
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
         final ResultSet rs = stmt.executeQuery(this.dialect.getTopologyQuery())) {
      if (rs.getMetaData().getColumnCount() == 0) {
        // We expect at least 4 columns. Note that the server may return 0 columns if failover has occurred.
        LOGGER.finest(Messages.get("TopologyUtils.unexpectedTopologyQueryColumnCount"));
        return null;
      }

      return this.verifyWriter(this.getHosts(rs, initialHostSpec, instanceTemplatesByRegion));
    } catch (final SQLSyntaxErrorException e) {
      throw new SQLException(Messages.get("TopologyUtils.invalidQuery"), e);
    } finally {
      if (networkTimeout == 0 && !conn.isClosed()) {
        conn.setNetworkTimeout(networkTimeoutExecutor, networkTimeout);
      }
    }
  }

  protected @Nullable List<HostSpec> getHosts(
      ResultSet rs, HostSpec initialHostSpec, Map<String, HostSpec> instanceTemplatesByRegion) throws SQLException {
    // Data in the result set is ordered by last update time, so the latest records are last.
    // We add hosts to a map to ensure newer records are not overwritten by older ones.
    Map<String, HostSpec> hostsMap = new HashMap<>();
    while (rs.next()) {
      try {
        HostSpec host = createHost(rs, initialHostSpec, instanceTemplatesByRegion);
        hostsMap.put(host.getHost(), host);
      } catch (Exception e) {
        LOGGER.finest(Messages.get("TopologyUtils.errorProcessingQueryResults", new Object[] {e.getMessage()}));
        return null;
      }
    }

    return new ArrayList<>(hostsMap.values());
  }

  protected HostSpec createHost(
      ResultSet rs, HostSpec initialHostSpec, Map<String, HostSpec> instanceTemplatesByRegion)
      throws SQLException {
    // According to the topology query the result set should contain 4 columns:
    // instance ID, 1/0 (writer/reader), node lag in time (msec), AWS region.
    String hostName = rs.getString(1);
    final boolean isWriter = rs.getBoolean(2);
    final float nodeLag = rs.getFloat(3);
    final String awsRegion = rs.getString(4);

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L;

    final HostSpec instanceTemplate = instanceTemplatesByRegion.get(awsRegion);
    if (instanceTemplate == null) {
      throw new SQLException(Messages.get(
          "GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", new Object[] {awsRegion}));
    }

    return createHost(hostName, hostName, isWriter, weight, Timestamp.from(Instant.now()), initialHostSpec,
        instanceTemplate);
  }

  public @Nullable String getRegion(String instanceId, Connection conn) throws SQLException {
    try (final PreparedStatement stmt = conn.prepareStatement(this.dialect.getRegionByInstanceIdQuery())) {
      stmt.setString(1, instanceId);
      try (final ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String awsRegion = rs.getString(1);
          return StringUtils.isNullOrEmpty(awsRegion) ? null : awsRegion;
        }
      }
    }

    return null;
  }

  public Map<String, HostSpec> parseInstanceTemplates(String instanceTemplatesString, Consumer<String> hostValidator)
      throws SQLException {
    if (StringUtils.isNullOrEmpty(instanceTemplatesString)) {
      throw new SQLException(Messages.get("GlobalAuroraTopologyUtils.globalClusterInstanceHostPatternsRequired"));
    }

    Map<String, HostSpec> instanceTemplates = Arrays.stream(instanceTemplatesString.split(","))
        .map(x -> ConnectionUrlParser.parseHostPortPairWithRegionPrefix(x.trim(), () -> hostSpecBuilder))
        .collect(Collectors.toMap(
            Pair::getValue1,
            v -> {
              hostValidator.accept(v.getValue2().getHost());
              return v.getValue2();
            }));
    LOGGER.finest(Messages.get(
        "GlobalAuroraTopologyUtils.detectedGdbPatterns",
        new Object[] {LogUtils.toLogString(instanceTemplates)}));

    return instanceTemplates;
  }
}
