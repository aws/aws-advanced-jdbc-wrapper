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

package software.amazon.jdbc.plugin;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class OpenedConnectionTracker {

  static final Map<String, Queue<WeakReference<Connection>>> openedConnections = new ConcurrentHashMap<>();
  private static final ExecutorService invalidateConnectionsExecutorService =
      Executors.newCachedThreadPool(
          r -> {
            final Thread invalidateThread = new Thread(r);
            invalidateThread.setDaemon(true);
            return invalidateThread;
          });
  private static final ExecutorService abortConnectionExecutorService =
      Executors.newCachedThreadPool(
          r -> {
            final Thread abortThread = new Thread(r);
            abortThread.setDaemon(true);
            return abortThread;
          });

  private static final Logger LOGGER = Logger.getLogger(OpenedConnectionTracker.class.getName());
  private static final RdsUtils rdsUtils = new RdsUtils();

  public void populateOpenedConnectionQueue(final HostSpec hostSpec, final Connection conn) {
    final Set<String> aliases = hostSpec.asAliases();

    // Check if the connection was established using a cluster endpoint
    final String host = hostSpec.getHost();
    if (rdsUtils.isRdsInstance(host)) {
      trackConnection(host, conn);
      return;
    }

    final Optional<String> instanceEndpoint = aliases.stream().filter(rdsUtils::isRdsInstance).findFirst();

    if (!instanceEndpoint.isPresent()) {
      LOGGER.finest(Messages.get("OpenedConnectionTracker.unableToPopulateOpenedConnectionQueue"));
      return;
    }

    trackConnection(instanceEndpoint.get(), conn);
  }

  /**
   * Invalidates all opened connections pointing to the same node in a daemon thread. 1
   *
   * @param hostSpec The {@link HostSpec} object containing the url of the node.
   */
  public void invalidateAllConnections(final HostSpec hostSpec) {
    invalidateAllConnections(hostSpec.getAliases().toArray(new String[] {}));
  }

  public void invalidateAllConnections(final String... node) {
    final Optional<String> instanceEndpoint = Arrays.stream(node).filter(rdsUtils::isRdsInstance).findFirst();
    if (!instanceEndpoint.isPresent()) {
      return;
    }

    invalidateConnections(openedConnections.get(instanceEndpoint.get()));
  }

  public void invalidateCurrentConnection(final HostSpec hostSpec, final Connection connection) {
    String host = rdsUtils.isRdsInstance(hostSpec.getHost())
        ? hostSpec.getHost()
        : hostSpec.getAliases().stream().filter(rdsUtils::isRdsInstance).findFirst().get();

    if (StringUtils.isNullOrEmpty(host)) {
      return;
    }

    final Queue<WeakReference<Connection>> connectionQueue = openedConnections.get(host);
    connectionQueue.removeIf(connectionWeakReference -> Objects.equals(connectionWeakReference.get(), connection));
  }

  private void trackConnection(final String instanceEndpoint, final Connection connection) {
    final Queue<WeakReference<Connection>> connectionQueue =
        openedConnections.computeIfAbsent(
            instanceEndpoint,
            (k) -> new ConcurrentLinkedQueue<>());
    connectionQueue.add(new WeakReference<>(connection));
    logOpenedConnections();
  }

  private void invalidateConnections(Queue<WeakReference<Connection>> connectionQueue) {
    invalidateConnectionsExecutorService.submit(() -> {
      WeakReference<Connection> connReference;
      while ((connReference = connectionQueue.poll()) != null) {
        final Connection conn = connReference.get();
        if (conn == null) {
          continue;
        }

        try {
          conn.abort(abortConnectionExecutorService);
        } catch (final SQLException e) {
          // swallow this exception, current connection should be useless anyway.
        }
      }
    });
  }

  public void logOpenedConnections() {
    LOGGER.finest(() -> {
      StringBuilder builder = new StringBuilder();
      openedConnections.forEach((key, queue) -> {
        builder.append("\t[ ");
        builder.append(key).append(":");
        builder.append("\n\t {");
        for (WeakReference<Connection> connection : queue) {
          builder.append("\n\t\t").append(connection.get());
        }
        builder.append("\n\t }\n");
        builder.append("\t");
      });
      return String.format("Opened Connections Tracked: \n[\n%s\n]", builder);
    });
  }
}
