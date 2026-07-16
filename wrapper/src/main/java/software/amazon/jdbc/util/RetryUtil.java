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

package software.amazon.jdbc.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;

public class RetryUtil {

  private static final Logger LOGGER = Logger.getLogger(RetryUtil.class.getName());

  public Results getWriterConnection(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties,
      final @Nullable ConnectionPlugin plugin,
      final boolean verifyRole,
      final long timeoutEndNano)
      throws TimeoutException, SQLException {
    return this.getAllowedConnection(
        pluginService,
        properties,
        plugin,
        () -> {
          final List<HostSpec> updatedHosts = pluginService.getAllHosts();

          final HostSpec writerCandidate = updatedHosts.stream()
              .filter(x -> x.getRole() == HostRole.WRITER)
              .findFirst()
              .orElse(null);

          if (writerCandidate == null) {
            LOGGER.finest(() -> LogUtils.logTopology(updatedHosts, Messages.get("RetryUtil.noWriterHost")));
            return null;
          }

          final List<HostSpec> allowedHosts = pluginService.getHosts();
          if (!Utils.containsHostAndPort(allowedHosts, writerCandidate.getHostAndPort())) {
            LOGGER.finest(() -> Messages.get("RetryUtil.newWriterNotAllowed",
                new Object[]{
                    writerCandidate.getUrl(),
                    LogUtils.logTopology(allowedHosts, "")}));
            return null;
          }
          return new HashSet<>(Collections.singletonList(writerCandidate));
        },
        null,
        verifyRole ? HostRole.WRITER : null,
        timeoutEndNano);
  }

  public Results getAllowedConnection(
      final @NonNull PluginService pluginService,
      final @NonNull Properties properties,
      final @Nullable ConnectionPlugin plugin,
      final @NonNull Supplier<@Nullable Set<HostSpec>> allowedHosts,
      @Nullable String strategy,
      @Nullable HostRole verifyRole,
      final long retryEndNano)
      throws TimeoutException, SQLException {

    if (StringUtils.isNullOrEmpty(strategy)) {
      strategy = "random";
    }

    Connection candidateConn = null;
    try {
      do {
        // The roles in this list might not be accurate, depending on whether the new topology has become available yet.
        pluginService.refreshHostList();
        Set<HostSpec> updatedAllowedHosts = allowedHosts.get();
        if (updatedAllowedHosts == null) {
          this.shortDelay();
          continue;
        }

        // Make a copy of hosts and set their availability.
        updatedAllowedHosts = updatedAllowedHosts.stream()
            .map(x -> pluginService.getHostSpecBuilder()
                .copyFrom(x)
                .availability(HostAvailability.AVAILABLE)
                .build())
            .collect(Collectors.toSet());
        final Set<HostSpec> remainingAllowedHosts = new HashSet<>(updatedAllowedHosts);

        if (remainingAllowedHosts.isEmpty()) {
          this.shortDelay();
          continue;
        }

        while (!remainingAllowedHosts.isEmpty() && System.nanoTime() < retryEndNano) {
          HostSpec candidateHost = null;
          try {
            candidateHost = verifyRole == null
                // Any role is acceptable. Strategy-based selection requires an explicit
                // reader/writer role, so pick directly from the already-filtered allowed set.
                ? selectAnyHost(remainingAllowedHosts)
                : pluginService.getHostSpecByStrategy(
                    new ArrayList<>(remainingAllowedHosts),
                    verifyRole,
                    strategy);
          } catch (SQLException ex) {
            // Strategy can't get a host according to requested conditions.
            // Do nothing
          }

          if (candidateHost == null) {
            LOGGER.finest(
                LogUtils.logTopology(
                    new ArrayList<>(remainingAllowedHosts),
                    Messages.get("RetryUtil.candidateNull", new Object[]{verifyRole})));
            this.shortDelay();
            break; // Exit loop over remainingAllowedHosts and fresh topology
          }

          try {
            candidateConn = pluginService.connect(candidateHost, properties, plugin);
            // Since the roles in the host list might not be accurate, we execute a query to check the instance's role.
            HostRole role = verifyRole == null ? null : pluginService.getHostRole(candidateConn);
            if (verifyRole == null || verifyRole == role) {
              HostSpec updatedHostSpec = new HostSpec(candidateHost, role);
              Results results = new Results(candidateConn, updatedHostSpec);
              candidateConn = null; // Prevents connection from closing in the finally block
              return results;
            }
          } catch (SQLException ex) {
            HostSpec finalCandidateHost = candidateHost;
            LOGGER.finest(() -> Messages.get("RetryUtil.exceptionConnectingToWriter",
                    new Object[]{finalCandidateHost.getHost(), ex.getMessage()}));
          }

          // Connection couldn't be opened or the role is not as expected, so the connection is not valid.
          remainingAllowedHosts.remove(candidateHost);
          if (candidateConn != null) {
            try {
              candidateConn.close();
            } catch (SQLException e) {
              // Ignore
            }
          }
        }
      } while (System.nanoTime() < retryEndNano); // All hosts failed. Keep trying until we hit the timeout.

      throw new TimeoutException(Messages.get("RetryUtil.timeout"));

    } finally {
      if (candidateConn != null) {
        try {
          candidateConn.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
  }

  /**
   * Selects an arbitrary host from the allowed set. Used when any role is acceptable
   * ({@code verifyRole == null}), since strategy-based selection via
   * {@link PluginService#getHostSpecByStrategy} requires an explicit reader or writer role.
   * The allowed set is already filtered by the caller, and the caller removes hosts that fail to
   * connect and retries, so a simple random pick spreads attempts across the eligible hosts.
   *
   * @param allowedHosts the eligible hosts to choose from
   * @return a randomly chosen host, or {@code null} if the set is empty
   */
  protected @Nullable HostSpec selectAnyHost(final Set<HostSpec> allowedHosts) {
    if (allowedHosts.isEmpty()) {
      return null;
    }
    final List<HostSpec> candidates = new ArrayList<>(allowedHosts);
    return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
  }

  protected void shortDelay() {
    try {
      TimeUnit.MILLISECONDS.sleep(100);
    } catch (InterruptedException ex1) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex1);
    }
  }

  public static class Results {
    private final @NonNull Connection connection;
    private final @NonNull HostSpec hostSpec;

    public Results(
        @NonNull Connection connection,
        @NonNull HostSpec hostSpec) {
      this.connection = connection;
      this.hostSpec = hostSpec;
    }

    public @NonNull Connection getConnection() {
      return connection;
    }

    public @NonNull HostSpec getHostSpec() {
      return hostSpec;
    }
  }
}
