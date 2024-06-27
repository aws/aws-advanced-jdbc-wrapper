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

package software.amazon.jdbc.plugin.bluegreen;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.SupportBlueGreen;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.StringUtils;

public class BlueGreenStatusMonitor {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusMonitor.class.getName());
  private static final long DEFAULT_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long CACHE_CLEANUP_NANO = TimeUnit.SECONDS.toNanos(5);
  private static final long CACHE_HOST_REPLACEMENT_EXPIRATION_NANO = TimeUnit.SECONDS.toNanos(1);

  private static final HashMap<String, BlueGreenPhases> blueGreenStatusMapping =
      new HashMap<String, BlueGreenPhases>() {
        {
          put("SOURCE", BlueGreenPhases.CREATED);
          put("TARGET", BlueGreenPhases.CREATED);
          put("SWITCHOVER_STARTING_ON_SOURCE", BlueGreenPhases.PREPARATION_TO_SWITCH_OVER);
          put("SWITCHOVER_STARTING_ON_TARGET", BlueGreenPhases.PREPARATION_TO_SWITCH_OVER);
          put("SWITCHOVER_IN_PROGRESS_ON_SOURCE", BlueGreenPhases.SWITCHING_OVER);
          put("SWITCHOVER_IN_PROGRESS_ON_TARGET", BlueGreenPhases.SWITCHING_OVER);
          put("TARGET_PROMOTED_DNS_UPDATING", BlueGreenPhases.SWITCH_OVER_COMPLETED);
          put("SOURCE_DEMOTED_DNS_UPDATING", BlueGreenPhases.SWITCH_OVER_COMPLETED);
        }
      };

  protected final SlidingExpirationCacheWithCleanupThread<String, HostReplacementHolder> hostReplacements =
      new SlidingExpirationCacheWithCleanupThread<>(
          BlueGreenStatusMonitor::canRemoveHostReplacement,
          null,
          CACHE_CLEANUP_NANO);

  protected final SupportBlueGreen supportBlueGreen;
  protected final PluginService pluginService;
  protected final Properties props;

  protected final Map<BlueGreenPhases, IntervalType> checkIntervalTypeMap;
  protected final Map<IntervalType, Long> checkIntervalMap;
  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected boolean canHoldConnection = true;
  protected Connection connection = null;
  protected HostSpec connectionHostSpec = null;

  protected Random random = new Random();

  public BlueGreenStatusMonitor(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull SupportBlueGreen supportBlueGreen,
      final @NonNull Map<BlueGreenPhases, IntervalType> checkIntervalTypeMap,
      final @NonNull Map<IntervalType, Long> checkIntervalMap) {

    this.pluginService = pluginService;
    this.props = props;
    this.supportBlueGreen = supportBlueGreen;
    this.checkIntervalTypeMap = checkIntervalTypeMap;
    this.checkIntervalMap = checkIntervalMap;

    executorService.submit(() -> {

      TimeUnit.SECONDS.sleep(1); // Some delay so the connection is initialized and topology is fetched.

      try {
        while (true) {
          try {
            BlueGreenStatus currentStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
            final BlueGreenStatus status = this.getStatus(currentStatus);
            if (currentStatus == null
                || (status != null && currentStatus.getCurrentPhase() != status.getCurrentPhase())) {
              LOGGER.finest("Status changed to: " + status.getCurrentPhase());
            }
            this.pluginService.setStatus(BlueGreenStatus.class, status, true);

            IntervalType intervalType =
                checkIntervalTypeMap.getOrDefault(status.getCurrentPhase(), IntervalType.BASELINE);
            long delay = checkIntervalMap.getOrDefault(intervalType, DEFAULT_CHECK_INTERVAL_MS);
            this.canHoldConnection = (delay <= TimeUnit.MINUTES.toMillis(5));
            TimeUnit.MILLISECONDS.sleep(delay);

          } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Unhandled exception while monitoring blue/green status.", ex);
          }
        }
      } finally {
        this.closeConnection();
        LOGGER.finest("Blue/green status monitoring thread is completed.");
      }
    });
    executorService.shutdown(); // executor accepts no more tasks
  }

  protected static boolean canRemoveHostReplacement(final HostReplacementHolder hostReplacementHolder) {
    try {
      final String currentIp = InetAddress.getByName(hostReplacementHolder.host).getHostAddress();
      if (!hostReplacementHolder.currentIp.equals(currentIp)) {
        // DNS has changed. We can remove this replacement.
        LOGGER.finest("Removing host replacement for " + hostReplacementHolder.host);
        return true;
      }
    } catch (UnknownHostException ex) {
      if (hostReplacementHolder.isGreenPrefix) {
        // Green node DNS doesn't exist anymore. We can delete it from the cache.
        LOGGER.finest("Removing host replacement for " + hostReplacementHolder.host);
        return true;
      }
      // otherwise do nothing
    }
    return false;
  }

  /**
   * Collect IP addresses of green nodes and map them to a new-blue FQDN host names.
   * It's assumed that each blue nodes will be mapped to a single green node.
   * <p></p>
   * Example:
   * Current topology:
   * - instance-1.XYZ.us-east-2.rds.amazonaws.com (10.0.1.100)
   * - instance-2.XYZ.us-east-2.rds.amazonaws.com (10.0.1.101)
   * - instance-3.XYZ.us-east-2.rds.amazonaws.com (10.0.1.102)
   * - instance-1-green-123456.XYZ.us-east-2.rds.amazonaws.com (10.0.1.103)
   * - instance-2-green-234567.XYZ.us-east-2.rds.amazonaws.com (10.0.1.104)
   * -instance-3-green-345678.XYZ.us-east-2.rds.amazonaws.com (10.0.1.105)
   * Expected mapping:
   * - instance-1.XYZ.us-east-2.rds.amazonaws.com (10.0.1.103)
   * - instance-2.XYZ.us-east-2.rds.amazonaws.com (10.0.1.104)
   * - instance-3.XYZ.us-east-2.rds.amazonaws.com (10.0.1.105)
   * Resulting map contains both blue and green nodes.
   */
  protected void collectHostIpAddresses(final List<HostSpec> hosts) {

    for (HostSpec hostSpec : hosts) {
      if (this.rdsUtils.isGreenInstance(hostSpec.getHost())) {
        final String greenHost = hostSpec.getHost();
        final String newBlueHost = this.rdsUtils.removeGreenInstancePrefix(greenHost);
        try {
          final String greenIp = InetAddress.getByName(greenHost).getHostAddress();
          final String newBlueIp = InetAddress.getByName(newBlueHost).getHostAddress();

          hostReplacements.remove(newBlueHost);
          hostReplacements.computeIfAbsent(
              newBlueHost,
              (key) -> new HostReplacementHolder(newBlueHost, false, greenHost, newBlueIp, greenIp),
              CACHE_HOST_REPLACEMENT_EXPIRATION_NANO);

          hostReplacements.remove(greenHost);
          hostReplacements.computeIfAbsent(
              greenHost,
              (key) -> new HostReplacementHolder(greenHost, true, newBlueHost, greenIp, greenIp),
              CACHE_HOST_REPLACEMENT_EXPIRATION_NANO);

        } catch (UnknownHostException ex) {
          // do nothing
        }
      }
    }
  }

  protected Map<String, String> getHostIpAddresses() {
    return new ConcurrentHashMap<>(
        hostReplacements.getEntries().values().stream()
            .collect(Collectors.toMap(k -> k.host, v -> v.replacementIp)));
  }

  protected Map<String, String> getCorrespondingHosts() {
    return new ConcurrentHashMap<>(
        hostReplacements.getEntries().values().stream()
            .collect(Collectors.toMap(k -> k.host, v -> v.correspondingHost)));
  }

  protected void closeConnection() {
    try {
      if (this.connection != null && !this.connection.isClosed()) {
        this.connection.close();
      }
    } catch (SQLException sqlException) {
      // ignore
    }
    this.connection = null;
  }

  protected BlueGreenStatus getStatus(final BlueGreenStatus currentStatus) {
    try {
      if (this.connection == null || this.connection.isClosed()) {
        this.closeConnection();
        if (this.connectionHostSpec == null) {
          this.connectionHostSpec = this.getHostSpec();
        }
        if (this.connectionHostSpec == null) {
          LOGGER.finest("No node found for blue/green status check.");
          return new BlueGreenStatus(
              BlueGreenPhases.NOT_CREATED, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        }
        try {
          LOGGER.info("Opening monitoring connection to " + this.connectionHostSpec.getHost());
          this.connection = this.getConnection(this.connectionHostSpec);
        } catch (SQLException ex) {
          // can't open connection
          // let's return the current status and try to open a connection on the next round
          return currentStatus;
        }
      }

      if (!supportBlueGreen.isStatusAvailable(this.connection)) {
        if (currentStatus == null || currentStatus.getCurrentPhase() == BlueGreenPhases.NOT_CREATED) {
          //LOGGER.finest("(status not available) currentPhase: " + BlueGreenPhases.NOT_CREATED);
          return new BlueGreenStatus(
              BlueGreenPhases.NOT_CREATED, this.getHostIpAddresses(), this.getCorrespondingHosts());
        } else {
          //LOGGER.finest("(status not available) currentPhase: " + BlueGreenPhases.SWITCH_OVER_COMPLETED);
          return new BlueGreenStatus(
              BlueGreenPhases.SWITCH_OVER_COMPLETED, this.getHostIpAddresses(), this.getCorrespondingHosts());
        }
      }

      final Statement statement = this.connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(this.supportBlueGreen.getBlueGreenStatusQuery());

      final HashMap<String, BlueGreenPhases> phasesByHost = new HashMap<>();
      final HashSet<BlueGreenPhases> hostPhases = new HashSet<>();
      final List<HostSpec> hosts = new ArrayList<>();

      while (resultSet.next()) {
        final String endpoint = resultSet.getString("endpoint");
        final int port = resultSet.getInt("port");
        final BlueGreenPhases phase = this.parsePhase(resultSet.getString("blue_green_deployment"));
        phasesByHost.put(endpoint, phase);
        hostPhases.add(phase);
        final HostSpec hostSpec = this.pluginService.getHostSpecBuilder()
            .host(endpoint)
            .hostId(this.getHostId(endpoint))
            .port(port)
            .build();
        hosts.add(hostSpec);
      }

      // try to reconnect to a green host if possible
      if (!this.rdsUtils.isGreenInstance(this.connectionHostSpec.getHost())) {
        final HostSpec candidateHostSpec = this.getHostSpec(hosts);
        if (candidateHostSpec != null && this.rdsUtils.isGreenInstance(candidateHostSpec.getHost())) {
          try {
            LOGGER.info("(candidate) Opening monitoring connection to " + candidateHostSpec.getHost());
            final Connection candidateConnection = this.getConnection(candidateHostSpec);

            // replace current connection with a candidate one
            LOGGER.info("Reconnected to green node.");
            this.closeConnection();
            this.connectionHostSpec = candidateHostSpec;
            this.connection = candidateConnection;
          } catch (SQLException ex) {
            // can't open connection
            // continue and try to open a connection on the next round
          }
        }
      }

      BlueGreenPhases currentPhase;

      if (hostPhases.contains(BlueGreenPhases.SWITCHING_OVER)) {
        // At least one node is in active switching over phase.
        currentPhase = BlueGreenPhases.SWITCHING_OVER;
      } else if (hostPhases.contains(BlueGreenPhases.PREPARATION_TO_SWITCH_OVER)) {
        // At least one node is in preparation for switching over phase.
        currentPhase = BlueGreenPhases.PREPARATION_TO_SWITCH_OVER;
      } else if (hostPhases.contains(BlueGreenPhases.SWITCH_OVER_COMPLETED) && hostPhases.size() == 1) {
        // All nodes have completed switchover.
        currentPhase = BlueGreenPhases.SWITCH_OVER_COMPLETED;
      } else if (hostPhases.contains(BlueGreenPhases.CREATED)) {
        // At least one node reports that Bleu/Green deployment is created.
        currentPhase = BlueGreenPhases.CREATED;
      } else {
        currentPhase = BlueGreenPhases.NOT_CREATED;
      }

      if (currentPhase == BlueGreenPhases.PREPARATION_TO_SWITCH_OVER) {
        this.pluginService.refreshHostList(this.connection);
        this.collectHostIpAddresses(hosts);
      }

      return new BlueGreenStatus(currentPhase, this.getHostIpAddresses(), this.getCorrespondingHosts());

    } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
      if (currentStatus == null || currentStatus.getCurrentPhase() == BlueGreenPhases.NOT_CREATED) {
        LOGGER.finest("(SQLSyntaxErrorException) currentPhase: " + BlueGreenPhases.NOT_CREATED);
        return new BlueGreenStatus(
            BlueGreenPhases.NOT_CREATED, this.getHostIpAddresses(), this.getCorrespondingHosts());
      } else {
        LOGGER.finest("(SQLSyntaxErrorException) currentPhase: " + BlueGreenPhases.SWITCH_OVER_COMPLETED);
        return new BlueGreenStatus(
            BlueGreenPhases.SWITCH_OVER_COMPLETED, this.getHostIpAddresses(), this.getCorrespondingHosts());
      }
    } catch (Exception e) {
      // do nothing
      LOGGER.log(Level.FINEST, "Unhandled exception.", e);
    } finally {
      if (!this.canHoldConnection) {
        this.closeConnection();
      }
    }

    return new BlueGreenStatus(BlueGreenPhases.NOT_CREATED, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  }

  protected String getHostId(final String endpoint) {
    if (StringUtils.isNullOrEmpty(endpoint)) {
      return null;
    }
    final String[] parts = endpoint.split("\\.");
    return parts != null && parts.length > 0 ? parts[0] : null;
  }

  protected BlueGreenPhases parsePhase(final String value) {
    if (StringUtils.isNullOrEmpty(value)) {
      return BlueGreenPhases.NOT_CREATED;
    }
    final BlueGreenPhases phase = blueGreenStatusMapping.get(value.toUpperCase());

    if (phase == null) {
      throw new IllegalArgumentException("Unknown blue/green status " + value);
    }
    return phase;
  }

  protected Connection getConnection(final HostSpec hostSpec) throws SQLException {
    return this.pluginService.forceConnect(hostSpec, this.props);
  }

  protected HostSpec getHostSpec() {
    return this.getHostSpec(this.pluginService.getHosts());
  }

  protected HostSpec getHostSpec(final List<HostSpec> hosts) {

    HostSpec hostSpec = this.getRandomHost(hosts, HostRole.UNKNOWN, true);

    if (hostSpec != null) {
      return hostSpec;
    }

    // There's no green nodes in topology.
    // Try to get any node.
    hostSpec = this.getRandomHost(hosts, HostRole.READER, false);

    if (hostSpec != null) {
      return hostSpec;
    }

    // There's no green nodes in topology.
    // Try to get any node.
    hostSpec = this.getRandomHost(hosts, HostRole.WRITER, false);

    if (hostSpec != null) {
      return hostSpec;
    }

    return this.getRandomHost(hosts, HostRole.UNKNOWN, false);
  }

  protected HostSpec getRandomHost(List<HostSpec> hosts, HostRole role, boolean greenHostOnly) {
    final List<HostSpec> filteredHosts = hosts.stream()
        .filter(x -> (x.getRole() == role || role == HostRole.UNKNOWN)
            && (!greenHostOnly || this.rdsUtils.isGreenInstance(x.getHost())))
        .collect(Collectors.toList());

    return filteredHosts.isEmpty()
        ? null
        : filteredHosts.get(this.random.nextInt(filteredHosts.size()));
  }

  public static class HostReplacementHolder {
    public final String host;
    public final boolean isGreenPrefix;
    public final String correspondingHost;
    public final String currentIp;
    public final String replacementIp;

    public HostReplacementHolder(
        final String host,
        final boolean isGreenHost,
        final String correspondingHost,
        final String currentIp,
        final String replacementIp) {

      this.host = host;
      this.isGreenPrefix = isGreenHost;
      this.correspondingHost = correspondingHost;
      this.currentIp = currentIp;
      this.replacementIp = replacementIp;
    }
  }
}
