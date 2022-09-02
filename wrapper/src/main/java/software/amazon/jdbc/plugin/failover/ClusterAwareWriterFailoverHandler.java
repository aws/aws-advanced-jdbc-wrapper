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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.jdbc.HostAvailability;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.Utils;

/**
 * An implementation of WriterFailoverHandler.
 *
 * <p>Writer Failover Process goal is to re-establish connection to a writer. Connection to a writer
 * may be disrupted either by temporary network issue, or due to writer host unavailability during
 * cluster failover. This handler tries both approaches in parallel: 1) try to re-connect to the
 * same writer host, 2) try to update cluster topology and connect to a newly elected writer.
 */
public class ClusterAwareWriterFailoverHandler implements WriterFailoverHandler {

  private static final Logger LOGGER = Logger.getLogger(ClusterAwareReaderFailoverHandler.class.getName());

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected Properties initialConnectionProps;
  protected PluginService pluginService;
  protected ReaderFailoverHandler readerFailoverHandler;
  private static final WriterFailoverResult DEFAULT_RESULT =
      new WriterFailoverResult(false, false, null, null, "None");

  public ClusterAwareWriterFailoverHandler(
      PluginService pluginService,
      ReaderFailoverHandler readerFailoverHandler,
      Properties initialConnectionProps) {
    this.pluginService = pluginService;
    this.readerFailoverHandler = readerFailoverHandler;
    this.initialConnectionProps = initialConnectionProps;
  }

  public ClusterAwareWriterFailoverHandler(
      PluginService pluginService,
      ReaderFailoverHandler readerFailoverHandler,
      Properties initialConnectionProps,
      int failoverTimeoutMs,
      int readTopologyIntervalMs,
      int reconnectWriterIntervalMs) {
    this(
        pluginService,
        readerFailoverHandler,
        initialConnectionProps);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process.
   */
  @Override
  public WriterFailoverResult failover(List<HostSpec> currentTopology)
      throws SQLException {
    if (Utils.isNullOrEmpty(currentTopology)) {
      LOGGER.severe(() -> Messages.get("ClusterAwareWriterFailoverHandler.failoverCalledWithInvalidTopology"));
      return DEFAULT_RESULT;
    }

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CompletionService<WriterFailoverResult> completionService = new ExecutorCompletionService<>(executorService);
    submitTasks(currentTopology, executorService, completionService);

    try {
      WriterFailoverResult result = getNextResult(executorService, completionService);
      if (result.isConnected() || result.getException() != null) {
        return result;
      }

      result = getNextResult(executorService, completionService);
      if (result.isConnected() || result.getException() != null) {
        return result;
      }

      LOGGER.fine(() -> Messages.get("ClusterAwareWriterFailoverHandler.failedToConnectToWriterInstance"));
      return DEFAULT_RESULT;
    } finally {
      if (!executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  private HostSpec getWriter(List<HostSpec> topology) {
    if (topology == null || topology.isEmpty()) {
      return null;
    }

    for (HostSpec host : topology) {
      if (host.getRole() == HostRole.WRITER) {
        return host;
      }
    }
    return null;
  }

  private void submitTasks(
      List<HostSpec> currentTopology, ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService) {
    HostSpec writerHost = this.getWriter(currentTopology);
    this.pluginService.setAvailability(writerHost.asAliases(), HostAvailability.NOT_AVAILABLE);
    completionService.submit(new ReconnectToWriterHandler(writerHost));
    completionService.submit(new WaitForNewWriterHandler(
        currentTopology,
        writerHost));
    executorService.shutdown();
  }

  private WriterFailoverResult getNextResult(
      ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService) throws SQLException {
    try {
      Future<WriterFailoverResult> firstCompleted = completionService.poll(
          this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      if (firstCompleted == null) {
        // The task was unsuccessful and we have timed out
        return DEFAULT_RESULT;
      }
      WriterFailoverResult result = firstCompleted.get();
      if (result.isConnected()) {
        executorService.shutdownNow();
        logTaskSuccess(result);
        return result;
      }

      if (result.getException() != null) {
        executorService.shutdownNow();
        return result;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw createInterruptedException(e);
    } catch (ExecutionException e) {
      // return failure below
    }
    return DEFAULT_RESULT;
  }

  private void logTaskSuccess(WriterFailoverResult result) {
    List<HostSpec> topology = result.getTopology();
    if (Utils.isNullOrEmpty(topology)) {
      String taskName = result.getTaskName() == null ? "None" : result.getTaskName();
      LOGGER.severe(
          () -> Messages.get(
              "ClusterAwareWriterFailoverHandler.successfulConnectionInvalidTopology",
              new Object[] {taskName}));
      return;
    }

    if (!LOGGER.isLoggable(Level.FINE)) {
      return;
    }

    final HostSpec writerHost = getWriter(topology);
    String newWriterHost = writerHost == null ? null : writerHost.getUrl();
    if (result.isNewHost()) {
      LOGGER.fine(
          () -> Messages.get(
              "ClusterAwareWriterFailoverHandler.successfullyConnectedToNewWriterInstance",
              new Object[] {newWriterHost}));
    } else {
      LOGGER.fine(
          () -> Messages.get(
              "ClusterAwareWriterFailoverHandler.successfullyReconnectedToWriterInstance",
              new Object[] {newWriterHost}));
    }
  }

  private SQLException createInterruptedException(InterruptedException e) {
    // "Thread was interrupted"
    return new SQLException(
        Messages.get("ClusterAwareWriterFailoverHandler.interruptedThread"),
        "70100",
        e);
  }

  /**
   * Internal class responsible for re-connecting to the current writer (aka TaskA).
   */
  private class ReconnectToWriterHandler implements Callable<WriterFailoverResult> {

    private final HostSpec originalWriterHost;

    public ReconnectToWriterHandler(HostSpec originalWriterHost) {
      this.originalWriterHost = originalWriterHost;
    }

    public WriterFailoverResult call() {
      LOGGER.fine(
          () -> Messages.get(
              "ClusterAwareWriterFailoverHandler.taskAAttemptReconnectToWriterInstance",
              new Object[] {this.originalWriterHost.getUrl()}));

      Connection conn = null;
      List<HostSpec> latestTopology = null;
      boolean success = false;

      try {
        while (Utils.isNullOrEmpty(latestTopology)) {
          try {
            if (conn != null && !conn.isClosed()) {
              conn.close();
            }

            conn = pluginService.connect(this.originalWriterHost, initialConnectionProps);
            pluginService.forceRefreshHostList(conn);
            latestTopology = pluginService.getHosts();

          } catch (SQLException exception) {
            // Propagate exceptions that are not caused by network errors.
            if (!SqlState.isConnectionError(exception)) {
              LOGGER.finer(
                  () -> Messages.get(
                      "ClusterAwareWriterFailoverHandler.taskAEncounteredException",
                      new Object[] {exception}));
              return new WriterFailoverResult(false, false, null, null, "TaskA", exception);
            }
          }

          if (Utils.isNullOrEmpty(latestTopology)) {
            TimeUnit.MILLISECONDS.sleep(reconnectWriterIntervalMs);
          }
        }

        success = isCurrentHostWriter(latestTopology);
        pluginService.setAvailability(this.originalWriterHost.asAliases(), HostAvailability.AVAILABLE);
        return new WriterFailoverResult(success, false, latestTopology, success ? conn : null, "TaskA");
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(success, false, latestTopology, success ? conn : null, "TaskA");
      } catch (Exception ex) {
        LOGGER.severe(() -> ex.getMessage());
        return new WriterFailoverResult(false, false, null, null, "TaskA");
      } finally {
        try {
          if (conn != null && !success && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // ignore
        }
        LOGGER.finer(Messages.get("ClusterAwareWriterFailoverHandler.taskAFinished"));
      }
    }

    private boolean isCurrentHostWriter(List<HostSpec> latestTopology) {
      final Set<String> currentAliases = this.originalWriterHost.getAliases();
      final HostSpec latestWriter = getWriter(latestTopology);
      if (currentAliases == null) {
        return false;
      }
      Set<String> latestWriterAliases = latestWriter.getAliases();

      return latestWriterAliases.stream().anyMatch(currentAliases::contains);
    }
  }

  /**
   * Internal class responsible for getting the latest cluster topology and connecting to a newly
   * elected writer (aka TaskB).
   */
  private class WaitForNewWriterHandler implements Callable<WriterFailoverResult> {

    private Connection currentConnection = null;
    private final HostSpec originalWriterHost;
    private List<HostSpec> currentTopology;
    private HostSpec currentReaderHost;
    private Connection currentReaderConnection;

    public WaitForNewWriterHandler(
        List<HostSpec> currentTopology,
        HostSpec currentHost) {
      this.currentTopology = currentTopology;
      this.originalWriterHost = currentHost;
    }

    public WriterFailoverResult call() {
      LOGGER.finer(Messages.get("ClusterAwareWriterFailoverHandler.taskBAttemptConnectionToNewWriterInstance"));

      try {
        boolean success = false;
        while (!success) {
          connectToReader();
          success = refreshTopologyAndConnectToNewWriter();
          if (!success) {
            closeReaderConnection();
          }
        }
        return new WriterFailoverResult(
            true,
            true,
            this.currentTopology,
            this.currentConnection,
            "TaskB");
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, null, null, "TaskB");
      } catch (Exception ex) {
        LOGGER.severe(
            () -> Messages.get(
                "ClusterAwareWriterFailoverHandler.taskBEncounteredException",
                new Object[] {ex.getMessage()}));
        throw ex;
      } finally {
        performFinalCleanup();
        LOGGER.finer(Messages.get("ClusterAwareWriterFailoverHandler.taskBFinished"));
      }
    }

    private void connectToReader() throws InterruptedException {
      while (true) {
        try {
          ReaderFailoverResult connResult = readerFailoverHandler.getReaderConnection(this.currentTopology);
          if (isValidReaderConnection(connResult)) {
            this.currentReaderConnection = connResult.getConnection();
            this.currentReaderHost = connResult.getHost();
            LOGGER.fine(
                () -> Messages.get(
                    "ClusterAwareWriterFailoverHandler.taskBConnectedToReader",
                    new Object[] {this.currentReaderHost.getUrl()}));
            break;
          }
        } catch (SQLException e) {
          // ignore
        }
        LOGGER.fine(() -> Messages.get("ClusterAwareWriterFailoverHandler.taskBFailedToConnectToAnyReader"));
        TimeUnit.SECONDS.sleep(1);
      }
    }

    private boolean isValidReaderConnection(ReaderFailoverResult result) {
      if (!result.isConnected() || result.getConnection() == null || result.getHost() == null) {
        return false;
      }
      // Verify that connected host is presented in current topology
      return this.currentTopology.stream()
          .anyMatch(h -> h.getUrl().equals(result.getHost().getUrl()));
    }

    /**
     * Re-read topology and wait for a new writer.
     *
     * @return Returns true if successful.
     */
    private boolean refreshTopologyAndConnectToNewWriter() throws InterruptedException {
      while (true) {
        try {
          pluginService.forceRefreshHostList(this.currentReaderConnection);
          List<HostSpec> topology = pluginService.getHosts();

          if (!topology.isEmpty()) {
            this.currentTopology = topology;
            HostSpec writerCandidate = getWriter(this.currentTopology);

            if (!isSame(writerCandidate, this.originalWriterHost)) {
              // new writer is available, and it's different from the previous writer
              logTopology();
              if (connectToWriter(writerCandidate)) {
                return true;
              }
            }
          }
        } catch (SQLException e) {
          LOGGER.finer(
              () -> Messages.get(
                  "ClusterAwareWriterFailoverHandler.taskBEncounteredException",
                  new Object[] {e}));
          return false;
        }

        TimeUnit.MILLISECONDS.sleep(readTopologyIntervalMs);
      }
    }

    private boolean isSame(HostSpec hostSpec1, HostSpec hostSpec2) {
      if (hostSpec1 == null) {
        return false;
      }

      return hostSpec1.getAliases().stream().anyMatch(hostSpec2.getAliases()::contains);
    }

    private boolean connectToWriter(HostSpec writerCandidate) {
      try {
        LOGGER.fine(
            () -> Messages.get(
                "ClusterAwareWriterFailoverHandler.taskBAttemptConnectionToNewWriter",
                new Object[] {writerCandidate.getUrl()}));

        if (isSame(writerCandidate, this.currentReaderHost)) {
          this.currentConnection = this.currentReaderConnection;
        } else {
          // connect to the new writer
          this.currentConnection = pluginService.connect(writerCandidate, initialConnectionProps);
        }

        pluginService.setAvailability(writerCandidate.asAliases(), HostAvailability.AVAILABLE);
        return true;
      } catch (SQLException exception) {
        pluginService.setAvailability(writerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
        return false;
      }
    }

    /**
     * Close the reader connection if not done so already, and mark the relevant fields as null.
     */
    private void closeReaderConnection() {
      try {
        if (this.currentReaderConnection != null && !this.currentReaderConnection.isClosed()) {
          this.currentReaderConnection.close();
        }
      } catch (SQLException e) {
        // ignore
      } finally {
        this.currentReaderConnection = null;
        this.currentReaderHost = null;
      }
    }

    private void performFinalCleanup() {
      // Close the reader connection if it's not needed.
      if (this.currentReaderConnection != null
          && this.currentConnection != this.currentReaderConnection) {
        try {
          this.currentReaderConnection.close();
        } catch (SQLException e) {
          // ignore
        }
      }
    }

    private void logTopology() {
      if (!LOGGER.isLoggable(Level.FINER)) {
        return;
      }

      StringBuilder msg = new StringBuilder();
      for (int i = 0; i < this.currentTopology.size(); i++) {
        HostSpec hostInfo = this.currentTopology.get(i);
        msg.append("\n   [")
            .append(i)
            .append("]: ")
            .append(hostInfo == null ? "<null>" : hostInfo.getHost());
      }
      LOGGER.finer(
          () -> Messages.get(
              "ClusterAwareWriterFailoverHandler.taskBTopologyObtained",
              new Object[] {msg.toString()}));
    }
  }
}
