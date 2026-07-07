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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.balancer.LoadBalancingPolicy;
import software.amazon.jdbc.plugin.readwritesplitting.source.ReaderCandidateSource;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.Utils;

/**
 * Topology-aware {@link ReaderResolver}. Ports the legacy
 * {@code ReadWriteSplittingPlugin.initializeReaderConnection}/{@code openNewReaderConnection}:
 * single-host clusters fall back to the writer; otherwise a reader is selected via the
 * {@link LoadBalancingPolicy} with a bounded per-call retry loop (local candidate removal on
 * non-login failures, login fast-fail, writer fallback on exhaustion).
 */
public class TopologyReaderResolver implements ReaderResolver {

  private static final Logger LOGGER = Logger.getLogger(TopologyReaderResolver.class.getName());

  private final ReaderCandidateSource candidateSource;
  private final LoadBalancingPolicy loadBalancingPolicy;
  private final WriterResolver writerFallback;

  public TopologyReaderResolver(
      final ReaderCandidateSource candidateSource,
      final LoadBalancingPolicy loadBalancingPolicy,
      final WriterResolver writerFallback) {
    this.candidateSource = candidateSource;
    this.loadBalancingPolicy = loadBalancingPolicy;
    this.writerFallback = writerFallback;
  }

  @Override
  public void switchToReader(final RwSplitContext ctx) throws SQLException {
    final List<HostSpec> hosts = ctx.pluginService().getHosts();
    if (hosts.size() == 1) {
      if (!ctx.isConnectionUsable(ctx.writerConnection())) {
        final WriterResolution wr = this.writerFallback.resolveWriter(ctx);
        if (wr.isConnected() && wr.getConnection() != null && wr.getHostSpec() != null) {
          ctx.markWriterFromPool(Boolean.TRUE.equals(ctx.pluginService().isPooledConnection()));
          ctx.bindWriter(wr.getConnection(), wr.getHostSpec());
          ctx.switchCurrentConnectionTo(wr.getConnection(), wr.getHostSpec());
        }
      }
      final HostSpec writerHost = ctx.writerHostSpec();
      LOGGER.warning(() -> Messages.get("ReadWriteSplittingPlugin.noReadersFound",
          new Object[] {writerHost == null ? "" : writerHost.getHostAndPort()}));
      return;
    }
    openNewReaderConnection(ctx);
  }

  private void openNewReaderConnection(final RwSplitContext ctx) throws SQLException {
    Connection conn = null;
    HostSpec readerHost = null;

    final List<HostSpec> hostCandidates = new ArrayList<>(this.candidateSource.candidates(ctx));
    final int maxAttempts = hostCandidates.size();
    for (int i = 0; i < maxAttempts && !hostCandidates.isEmpty(); i++) {
      final HostSpec hostSpec = this.loadBalancingPolicy.pickReader(ctx, hostCandidates);
      if (hostSpec == null) {
        break;
      }

      try {
        conn = ctx.connect(hostSpec, ctx.properties());
        ctx.markReaderFromPool(Boolean.TRUE.equals(ctx.pluginService().isPooledConnection()));
        readerHost = hostSpec;
        break;
      } catch (final SQLException e) {
        if (ctx.pluginService().isLoginException(e, ctx.pluginService().getTargetDriverDialect())) {
          throw e;
        }
        hostCandidates.remove(hostSpec);
        if (LOGGER.isLoggable(Level.WARNING)) {
          LOGGER.log(Level.WARNING,
              Messages.get("ReadWriteSplittingPlugin.failedToConnectToReader",
                  new Object[] {hostSpec.getHostAndPort()}),
              e);
        }
      }
    }

    if (conn == null || readerHost == null) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.noReadersAvailable"),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
      return;
    }

    final HostSpec finalReaderHost = readerHost;
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
        new Object[] {finalReaderHost.getHostAndPort()}));
    ctx.bindReader(conn, readerHost);
    ctx.switchCurrentConnectionTo(conn, readerHost);
  }

  @Override
  public void closeStaleReaderIfNecessary(final RwSplitContext ctx) {
    final HostSpec readerHostSpec = ctx.readerHostSpec();
    final List<HostSpec> hosts = ctx.pluginService().getHosts();
    if (readerHostSpec != null && !Utils.containsHostAndPort(hosts, readerHostSpec.getHostAndPort())) {
      LOGGER.finest(Messages.get("ReadWriteSplittingPlugin.previousReaderNotAllowed",
          new Object[] {readerHostSpec, LogUtils.logTopology(hosts, "")}));
      ctx.closeReaderConnectionIfIdle();
    }
  }

  @Override
  public boolean isPerQuery() {
    return this.loadBalancingPolicy.isPerQuery();
  }
}
