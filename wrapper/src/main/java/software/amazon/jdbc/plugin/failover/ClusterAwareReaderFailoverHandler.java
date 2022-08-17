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
import java.util.Set;
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
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.AuroraHostListProvider;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
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
  public static final ReaderFailoverResult FAILED_READER_FAILOVER_RESULT = new ReaderFailoverResult(null,
      FailoverConnectionPlugin.NO_CONNECTION_INDEX, false);
  protected Properties initialConnectionProps;
  protected int maxFailoverTimeoutMs;
  protected int timeoutMs;
  protected final PluginService pluginService;
  protected final AuroraHostListProvider hostListProvider;

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param hostListProvider An implementation of {@link HostListProvider} that obtains and caches a
   *     cluster's topology.
   * @param pluginService A provider for creating new connections.
   * @param initialConnectionProps The initial connection properties to copy over to the new reader.
   */
  public ClusterAwareReaderFailoverHandler(
      AuroraHostListProvider hostListProvider,
      PluginService pluginService,
      Properties initialConnectionProps) {
    this(
        hostListProvider,
        pluginService,
        initialConnectionProps,
        DEFAULT_FAILOVER_TIMEOUT,
        DEFAULT_READER_CONNECT_TIMEOUT);
  }

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param hostListProvider An implementation of {@link HostListProvider} that obtains and caches a
   *     cluster's topology.
   * @param pluginService A provider for creating new connections.
   * @param initialConnectionProps The initial connection properties to copy over to the new reader.
   * @param failoverTimeoutMs Maximum allowed time in milliseconds to attempt reconnecting to a new
   *     reader instance after a cluster failover is initiated.
   * @param timeoutMs Maximum allowed time for the entire reader failover process.
   */
  public ClusterAwareReaderFailoverHandler(
      AuroraHostListProvider hostListProvider,
      PluginService pluginService,
      Properties initialConnectionProps,
      int failoverTimeoutMs,
      int timeoutMs) {
    this.hostListProvider = hostListProvider;
    this.pluginService = pluginService;
    this.initialConnectionProps = initialConnectionProps;
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.timeoutMs = timeoutMs;
  }

  /**
   * Set process timeout in millis. Entire process of connecting to a reader will be limited by this
   * time duration.
   *
   * @param timeoutMs Process timeout in millis
   */
  protected void setTimeoutMs(int timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts Cluster current topology
   * @param currentHost The currently connected host that has failed.
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult failover(List<HostSpec> hosts, HostSpec currentHost)
      throws SQLException {
    if (Utils.isNullOrEmpty(hosts)) {
      LOGGER.fine(Messages.get("ClusterAwareReaderFailoverHandler.6", new Object[] {"failover"}));
      return FAILED_READER_FAILOVER_RESULT;
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ReaderFailoverResult> future = submitInternalFailoverTask(hosts, currentHost, executor);
    return getInternalFailoverResult(executor, future);
  }

  private Future<ReaderFailoverResult> submitInternalFailoverTask(
      List<HostSpec> hosts,
      HostSpec currentHost,
      ExecutorService executor) {
    Future<ReaderFailoverResult> future = executor.submit(() -> {
      ReaderFailoverResult result;
      while (true) {
        result = failoverInternal(hosts, currentHost);
        if (result != null && result.isConnected()) {
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }
      return result;
    });
    executor.shutdown();
    return future;
  }

  private ReaderFailoverResult getInternalFailoverResult(
      ExecutorService executor,
      Future<ReaderFailoverResult> future) throws SQLException {
    ReaderFailoverResult defaultResult = new ReaderFailoverResult(
        null, FailoverConnectionPlugin.NO_CONNECTION_INDEX, false);
    try {
      ReaderFailoverResult result = future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      return result == null ? defaultResult : result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException(Messages.get("ClusterAwareReaderFailoverHandler.1"), "70100", e);
    } catch (ExecutionException e) {
      return defaultResult;
    } catch (TimeoutException e) {
      future.cancel(true);
      return defaultResult;
    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  protected ReaderFailoverResult failoverInternal(
      List<HostSpec> hosts,
      HostSpec currentHost)
      throws SQLException {
    this.hostListProvider.addToDownHostList(currentHost);
    Set<String> downHosts = hostListProvider.getDownHosts();
    List<HostTuple> hostGroup = getHostTuplesByPriority(hosts, downHosts);
    return getConnectionFromHostGroup(hostGroup);
  }

  public List<HostTuple> getHostTuplesByPriority(List<HostSpec> hosts, Set<String> downHosts) {
    List<HostTuple> hostGroup = new ArrayList<>();
    addActiveReaders(hostGroup, hosts, downHosts);
    HostSpec writerHost = hosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    hostGroup.add(
        new HostTuple(
            writerHost,
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX));
    addDownHosts(hostGroup, hosts, downHosts);
    return hostGroup;
  }

  private void addActiveReaders(
      List<HostTuple> list,
      List<HostSpec> hosts,
      Set<String> downHosts) {
    List<HostTuple> activeReaders = new ArrayList<>();
    for (int i = FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
      HostSpec host = hosts.get(i);
      if (!downHosts.contains(host.getUrl())) {
        activeReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(activeReaders);
    list.addAll(activeReaders);
  }

  private void addDownHosts(
      List<HostTuple> list,
      List<HostSpec> hosts,
      Set<String> downHosts) {
    List<HostTuple> downHostList = new ArrayList<>();
    for (int i = 0; i < hosts.size(); i++) {
      HostSpec host = hosts.get(i);
      if (downHosts.contains(host.getUrl())) {
        downHostList.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(downHostList);
    list.addAll(downHostList);
  }

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer.
   *
   * @param hostList Cluster current topology
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult getReaderConnection(List<HostSpec> hostList)
      throws SQLException {
    if (Utils.isNullOrEmpty(hostList)) {
      LOGGER.fine(Messages.get("ClusterAwareReaderFailover.6", new Object[] {"getReaderConnection"}));
      return FAILED_READER_FAILOVER_RESULT;
    }

    Set<String> downHosts = hostListProvider.getDownHosts();
    List<HostTuple> tuples = getReaderTuplesByPriority(hostList, downHosts);
    return getConnectionFromHostGroup(tuples);
  }

  public List<HostTuple> getReaderTuplesByPriority(
      List<HostSpec> hostList,
      Set<String> downHosts) {
    List<HostTuple> tuples = new ArrayList<>();
    addActiveReaders(tuples, hostList, downHosts);
    addDownReaders(tuples, hostList, downHosts);
    return tuples;
  }

  private void addDownReaders(
      List<HostTuple> list,
      List<HostSpec> hosts,
      Set<String> downHosts) {
    List<HostTuple> downReaders = new ArrayList<>();
    for (int i = FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
      HostSpec host = hosts.get(i);
      if (downHosts.contains(host.getUrl())) {
        downReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(downReaders);
    list.addAll(downReaders);
  }

  private ReaderFailoverResult getConnectionFromHostGroup(List<HostTuple> hostGroup)
      throws SQLException {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<ReaderFailoverResult> completionService = new ExecutorCompletionService<>(executor);

    try {
      for (int i = 0; i < hostGroup.size(); i += 2) {
        // submit connection attempt tasks in batches of 2
        ReaderFailoverResult result = getResultFromNextTaskBatch(hostGroup, executor, completionService, i);
        if (result.isConnected() || result.getException() != null) {
          return result;
        }

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException(Messages.get("ClusterAwareReaderFailoverHandler.1"), "70100", e);
        }
      }

      return new ReaderFailoverResult(
          null,
          FailoverConnectionPlugin.NO_CONNECTION_INDEX,
          false);
    } finally {
      executor.shutdownNow();
    }
  }

  private ReaderFailoverResult getResultFromNextTaskBatch(
      List<HostTuple> hostGroup,
      ExecutorService executor,
      CompletionService<ReaderFailoverResult> completionService,
      int i) throws SQLException {
    ReaderFailoverResult result;
    int numTasks = i + 1 < hostGroup.size() ? 2 : 1;
    completionService.submit(new ConnectionAttemptTask(hostGroup.get(i)));
    if (numTasks == 2) {
      completionService.submit(new ConnectionAttemptTask(hostGroup.get(i + 1)));
    }
    for (int taskNum = 0; taskNum < numTasks; taskNum++) {
      result = getNextResult(completionService);
      if (result.isConnected()) {
        executor.shutdownNow();
        LOGGER.fine(
            Messages.get(
                "ClusterAwareReaderFailoverHandler.2",
                new Object[] {result.getConnectionIndex()}));
        return result;
      }
      if (result.getException() != null) {
        executor.shutdownNow();
        return result;
      }
    }
    return new ReaderFailoverResult(
        null,
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        false);
  }

  private ReaderFailoverResult getNextResult(CompletionService<ReaderFailoverResult> service)
      throws SQLException {
    try {
      Future<ReaderFailoverResult> future = service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (future == null) {
        return FAILED_READER_FAILOVER_RESULT;
      }
      ReaderFailoverResult result = future.get();
      return result == null ? FAILED_READER_FAILOVER_RESULT : result;
    } catch (ExecutionException e) {
      return FAILED_READER_FAILOVER_RESULT;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException(
          Messages.get("ClusterAwareReaderFailoverHandler.1"),
          "70100",
          e);
    }
  }

  private class ConnectionAttemptTask implements Callable<ReaderFailoverResult> {

    private final HostTuple newHostTuple;

    private ConnectionAttemptTask(HostTuple newHostTuple) {
      this.newHostTuple = newHostTuple;
    }

    /**
     * Call ConnectionAttemptResult.
     */
    @Override
    public ReaderFailoverResult call() {
      HostSpec newHost = this.newHostTuple.getHost();
      LOGGER.fine(
          Messages.get(
              "ClusterAwareReaderFailoverHandler.3",
              new Object[] {this.newHostTuple.getIndex(), newHost.getUrl()}));

      try {
        Connection conn = pluginService.connect(newHost, initialConnectionProps);
        hostListProvider.removeFromDownHostList(newHost);
        LOGGER.fine(
            Messages.get(
                "ClusterAwareReaderFailoverHandler.4",
                new Object[] {this.newHostTuple.getIndex(), newHost.getUrl()}));
        return new ReaderFailoverResult(conn, this.newHostTuple.getIndex(), true);
      } catch (SQLException e) {
        hostListProvider.addToDownHostList(newHost);
        LOGGER.fine(
            Messages.get(
                "ClusterAwareReaderFailoverHandler.5",
                new Object[] {this.newHostTuple.getIndex(), newHost.getUrl()}));
        // Propagate exceptions that are not caused by network errors.
        if (!SqlState.isConnectionError(e)) {
          return new ReaderFailoverResult(
              null,
              FailoverConnectionPlugin.NO_CONNECTION_INDEX,
              false,
              e);
        }

        return FAILED_READER_FAILOVER_RESULT;
      }
    }
  }

  /**
   * HostTuple class.
   */
  public static class HostTuple {

    private final HostSpec host;
    private final int index;

    public HostTuple(HostSpec host, int index) {
      this.host = host;
      this.index = index;
    }

    public HostSpec getHost() {
      return host;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public String toString() {
      return String.format("host: %s, index: %d", host, index);
    }
  }
}
