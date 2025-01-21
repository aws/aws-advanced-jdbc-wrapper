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

package software.amazon.jdbc.plugin.failover;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.Utils;

/**
 * An implementation of ReaderFailoverHandler.
 *
 * <p>Reader Failover Process goal is to connect to any available reader. In order to connect
 * faster, this implementation tries to connect to two readers at the same time. The first
 * successfully connected reader is returned as the process result. If both readers are unavailable
 * (i.e. could not be connected to), the process picks up another pair of readers and repeat. If no
 * reader has been connected to, the process may consider a writer host, and other hosts marked
 * down, to connect to.
 */
public class ClusterAwareReaderFailoverHandler implements ReaderFailoverHandler {

  private static final Logger LOGGER =
      Logger.getLogger(ClusterAwareReaderFailoverHandler.class.getName());
  protected static final int DEFAULT_FAILOVER_TIMEOUT = 60000; // 60 sec
  protected static final int DEFAULT_READER_CONNECT_TIMEOUT = 30000; // 30 sec
  public static final ReaderFailoverResult FAILED_READER_FAILOVER_RESULT =
      new ReaderFailoverResult(null, null, false);
  protected Properties initialConnectionProps;
  protected int maxFailoverTimeoutMs;
  protected int timeoutMs;
  protected boolean isStrictReaderRequired;
  protected final PluginService pluginService;

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param pluginService          A provider for creating new connections.
   * @param initialConnectionProps The initial connection properties to copy over to the new reader.
   */
  public ClusterAwareReaderFailoverHandler(
      final PluginService pluginService,
      final Properties initialConnectionProps) {
    this(
        pluginService,
        initialConnectionProps,
        DEFAULT_FAILOVER_TIMEOUT,
        DEFAULT_READER_CONNECT_TIMEOUT,
        false);
  }

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param pluginService              A provider for creating new connections.
   * @param initialConnectionProps     The initial connection properties to copy over to the new reader.
   * @param maxFailoverTimeoutMs       Maximum allowed time for the entire reader failover process.
   * @param timeoutMs                  Maximum allowed time in milliseconds for each reader connection attempt during
   *                                   the reader failover process.
   * @param isStrictReaderRequired When true, it disables adding a writer to a list of nodes to connect
   */
  public ClusterAwareReaderFailoverHandler(
      final PluginService pluginService,
      final Properties initialConnectionProps,
      final int maxFailoverTimeoutMs,
      final int timeoutMs,
      final boolean isStrictReaderRequired) {
    this.pluginService = pluginService;
    this.initialConnectionProps = initialConnectionProps;
    this.maxFailoverTimeoutMs = maxFailoverTimeoutMs;
    this.timeoutMs = timeoutMs;
    this.isStrictReaderRequired = isStrictReaderRequired;
  }

  /**
   * Set process timeout in millis. Entire process of connecting to a reader will be limited by this
   * time duration.
   *
   * @param timeoutMs Process timeout in millis
   */
  protected void setTimeoutMs(final int timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts       Cluster current topology
   * @param currentHost The currently connected host that has failed.
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult failover(final List<HostSpec> hosts, final HostSpec currentHost)
      throws SQLException {
    if (Utils.isNullOrEmpty(hosts)) {
      LOGGER.fine(() -> Messages.get("ClusterAwareReaderFailoverHandler.invalidTopology", new Object[] {"failover"}));
      return FAILED_READER_FAILOVER_RESULT;
    }

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<ReaderFailoverResult> future = submitInternalFailoverTask(hosts, currentHost, executor);
    return getInternalFailoverResult(executor, future);
  }

  private Future<ReaderFailoverResult> submitInternalFailoverTask(
      final List<HostSpec> hosts,
      final HostSpec currentHost,
      final ExecutorService executor) {
    final Future<ReaderFailoverResult> future = executor.submit(() -> {
      ReaderFailoverResult result;
      try {
        while (true) {
          result = failoverInternal(hosts, currentHost);
          if (result != null && result.isConnected()) {
            return result;
          }

          TimeUnit.SECONDS.sleep(1);
        }
      } catch (final SQLException ex) {
        return new ReaderFailoverResult(null, null, false, ex);
      } catch (final Exception ex) {
        return new ReaderFailoverResult(null, null, false, new SQLException(ex));
      }
    });
    executor.shutdown();
    return future;
  }

  private ReaderFailoverResult getInternalFailoverResult(
      final ExecutorService executor,
      final Future<ReaderFailoverResult> future) throws SQLException {
    try {
      final ReaderFailoverResult result = future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      if (result == null) {
        LOGGER.warning(
            Messages.get("ClusterAwareReaderFailoverHandler.timeout", new Object[] {this.maxFailoverTimeoutMs}));
        return FAILED_READER_FAILOVER_RESULT;
      }

      return result;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException(Messages.get("ClusterAwareReaderFailoverHandler.interruptedThread"), "70100", e);
    } catch (final ExecutionException e) {
      return FAILED_READER_FAILOVER_RESULT;
    } catch (final TimeoutException e) {
      future.cancel(true);
      return FAILED_READER_FAILOVER_RESULT;
    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  protected ReaderFailoverResult failoverInternal(
      final List<HostSpec> hosts,
      final HostSpec currentHost)
      throws SQLException {
    if (currentHost != null) {
      this.pluginService.setAvailability(currentHost.asAliases(), HostAvailability.NOT_AVAILABLE);
    }
    final List<HostSpec> hostsByPriority = getHostsByPriority(hosts);
    return getConnectionFromHostGroup(hostsByPriority);
  }

  public List<HostSpec> getHostsByPriority(final List<HostSpec> hosts) {
    final List<HostSpec> activeReaders = new ArrayList<>();
    final List<HostSpec> downHostList = new ArrayList<>();
    HostSpec writerHost = null;

    for (final HostSpec host : hosts) {
      if (host.getRole() == HostRole.WRITER) {
        writerHost = host;
        continue;
      }
      if (host.getRawAvailability() == HostAvailability.AVAILABLE) {
        activeReaders.add(host);
      } else {
        downHostList.add(host);
      }
    }
    Collections.shuffle(activeReaders);
    Collections.shuffle(downHostList);

    final List<HostSpec> hostsByPriority = new ArrayList<>(activeReaders);
    final int numOfReaders = activeReaders.size() + downHostList.size();
    // Since the writer instance may change during failover, the original writer is likely now a reader. We will include
    // it and then verify the role once connected if using "strict-reader".
    if (writerHost != null || numOfReaders == 0) {
      hostsByPriority.add(writerHost);
    }
    hostsByPriority.addAll(downHostList);

    return hostsByPriority;
  }

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer.
   *
   * @param hostList Cluster current topology
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult getReaderConnection(final List<HostSpec> hostList)
      throws SQLException {
    if (Utils.isNullOrEmpty(hostList)) {
      LOGGER.fine(
          () -> Messages.get(
              "ClusterAwareReaderFailover.invalidTopology",
              new Object[] {"getReaderConnection"}));
      return FAILED_READER_FAILOVER_RESULT;
    }

    final List<HostSpec> hostsByPriority = getReaderHostsByPriority(hostList);
    return getConnectionFromHostGroup(hostsByPriority);
  }

  public List<HostSpec> getReaderHostsByPriority(final List<HostSpec> hosts) {
    final List<HostSpec> activeReaders = new ArrayList<>();
    final List<HostSpec> downHostList = new ArrayList<>();
    HostSpec writerHost = null;

    for (final HostSpec host : hosts) {
      if (host.getRole() == HostRole.WRITER) {
        writerHost = host;
        continue;
      }
      if (host.getRawAvailability() == HostAvailability.AVAILABLE) {
        activeReaders.add(host);
      } else {
        downHostList.add(host);
      }
    }
    Collections.shuffle(activeReaders);
    Collections.shuffle(downHostList);

    final List<HostSpec> hostsByPriority = new ArrayList<>();
    hostsByPriority.addAll(activeReaders);
    hostsByPriority.addAll(downHostList);

    final int numOfReaders = activeReaders.size() + downHostList.size();
    if (writerHost != null && (numOfReaders == 0
          || this.pluginService.getDialect().getFailoverRestrictions()
                .contains(FailoverRestriction.ENABLE_WRITER_IN_TASK_B))) {
      hostsByPriority.add(writerHost);
    }

    return hostsByPriority;
  }

  private ReaderFailoverResult getConnectionFromHostGroup(final List<HostSpec> hosts)
      throws SQLException {
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CompletionService<ReaderFailoverResult> completionService = new ExecutorCompletionService<>(executor);

    try {
      for (int i = 0; i < hosts.size(); i += 2) {
        // submit connection attempt tasks in batches of 2
        final ReaderFailoverResult result = getResultFromNextTaskBatch(hosts, executor, completionService, i);
        if (result.isConnected() || result.getException() != null) {
          return result;
        }

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException(Messages.get("ClusterAwareReaderFailoverHandler.interruptedThread"), "70100", e);
        }
      }

      return new ReaderFailoverResult(
          null,
          null,
          false);
    } finally {
      executor.shutdownNow();
    }
  }

  private ReaderFailoverResult getResultFromNextTaskBatch(
      final List<HostSpec> hosts,
      final ExecutorService executor,
      final CompletionService<ReaderFailoverResult> completionService,
      final int i) throws SQLException {
    ReaderFailoverResult result;
    final int numTasks = i + 1 < hosts.size() ? 2 : 1;
    completionService.submit(new ConnectionAttemptTask(hosts.get(i), this.isStrictReaderRequired));
    if (numTasks == 2) {
      completionService.submit(new ConnectionAttemptTask(hosts.get(i + 1), this.isStrictReaderRequired));
    }
    for (int taskNum = 0; taskNum < numTasks; taskNum++) {
      result = getNextResult(completionService);
      if (result.isConnected()) {
        executor.shutdownNow();
        return result;
      }
      if (result.getException() != null) {
        executor.shutdownNow();
        return result;
      }
    }
    return new ReaderFailoverResult(
        null,
        null,
        false);
  }

  private ReaderFailoverResult getNextResult(final CompletionService<ReaderFailoverResult> service)
      throws SQLException {
    try {
      final Future<ReaderFailoverResult> future = service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (future == null) {
        return FAILED_READER_FAILOVER_RESULT;
      }
      final ReaderFailoverResult result = future.get();
      return result == null ? FAILED_READER_FAILOVER_RESULT : result;
    } catch (final ExecutionException e) {
      return FAILED_READER_FAILOVER_RESULT;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException(
          Messages.get(
              "ClusterAwareReaderFailoverHandler.interruptedThread"),
          "70100",
          e);
    }
  }

  private class ConnectionAttemptTask implements Callable<ReaderFailoverResult> {

    private final HostSpec newHost;
    private final boolean isStrictReaderRequired;

    private ConnectionAttemptTask(final HostSpec newHost, final boolean isStrictReaderRequired) {
      this.newHost = newHost;
      this.isStrictReaderRequired = isStrictReaderRequired;
    }

    /**
     * Call ConnectionAttemptResult.
     */
    @Override
    public ReaderFailoverResult call() {
      LOGGER.fine(
          () -> Messages.get(
              "ClusterAwareReaderFailoverHandler.attemptingReaderConnection",
              new Object[] {this.newHost.getUrl(), PropertyUtils.maskProperties(initialConnectionProps)}));

      try {
        final Properties copy = new Properties();
        copy.putAll(initialConnectionProps);

        final Connection conn = pluginService.forceConnect(this.newHost, copy);
        pluginService.setAvailability(this.newHost.asAliases(), HostAvailability.AVAILABLE);

        if (this.isStrictReaderRequired) {
          // need to ensure that new connection is a connection to a reader node
          try {
            HostRole role = pluginService.getHostRole(conn);
            if (!HostRole.READER.equals(role)) {
              LOGGER.fine(
                  Messages.get(
                      "ClusterAwareReaderFailoverHandler.readerRequired",
                      new Object[]{ this.newHost.getUrl(), role }));

              try {
                conn.close();
              } catch (final SQLException innerException) {
                // ignore
              }

              return FAILED_READER_FAILOVER_RESULT;
            }
          } catch (SQLException e) {
            LOGGER.fine(Messages.get("ClusterAwareReaderFailoverHandler.errorGettingHostRole", new Object[]{e}));

            try {
              conn.close();
            } catch (final SQLException innerException) {
              // ignore
            }

            return FAILED_READER_FAILOVER_RESULT;
          }
        }

        LOGGER.fine(
            () -> Messages.get(
                "ClusterAwareReaderFailoverHandler.successfulReaderConnection",
                new Object[] {this.newHost.getUrl()}));
        LOGGER.fine("New reader failover connection object: " + conn);
        return new ReaderFailoverResult(conn, this.newHost, true);
      } catch (final SQLException e) {
        pluginService.setAvailability(newHost.asAliases(), HostAvailability.NOT_AVAILABLE);
        LOGGER.fine(
            () -> Messages.get(
                "ClusterAwareReaderFailoverHandler.failedReaderConnection",
                new Object[] {this.newHost.getUrl()}));
        // Propagate exceptions that are not caused by network errors.
        if (!pluginService.isNetworkException(e)) {
          return new ReaderFailoverResult(
              null,
              null,
              false,
              e);
        }

        return FAILED_READER_FAILOVER_RESULT;
      }
    }
  }
}
