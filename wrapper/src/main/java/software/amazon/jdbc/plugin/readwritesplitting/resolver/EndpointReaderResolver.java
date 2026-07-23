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
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.EndpointHostSpecs;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.handler.EndpointConnectionVerifier;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;

/**
 * {@link ReaderResolver} that connects to the configured read endpoint, optionally verifying the
 * connection role. Ports the legacy {@code SimpleReadWriteSplittingPlugin.initializeReaderConnection}.
 * Fails fast at construction if the endpoint is missing/blank.
 */
public class EndpointReaderResolver implements ReaderResolver {

  private static final Logger LOGGER = Logger.getLogger(EndpointReaderResolver.class.getName());

  private final String readEndpoint;
  private final boolean verifyNewConnections;
  private final EndpointConnectionVerifier verifier;
  private final boolean queryLevelLoadBalancing;

  public EndpointReaderResolver(
      final String readEndpoint,
      final boolean verifyNewConnections,
      final EndpointConnectionVerifier verifier) {
    this(readEndpoint, verifyNewConnections, verifier, false);
  }

  public EndpointReaderResolver(
      final String readEndpoint,
      final boolean verifyNewConnections,
      final EndpointConnectionVerifier verifier,
      final boolean queryLevelLoadBalancing) {
    if (readEndpoint == null || readEndpoint.trim().isEmpty()) {
      throw new RuntimeException(Messages.get(
          "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
          new Object[] {"srwReadEndpoint"}));
    }
    this.readEndpoint = readEndpoint;
    this.verifyNewConnections = verifyNewConnections;
    this.verifier = verifier;
    this.queryLevelLoadBalancing = queryLevelLoadBalancing;
  }

  @Override
  public boolean isPerQuery() {
    return this.queryLevelLoadBalancing;
  }

  @Override
  public void switchToReader(final RwSplitContext ctx) throws SQLException {
    HostSpec readerHost = ctx.readerHostSpec();
    if (readerHost == null) {
      readerHost = EndpointHostSpecs.create(
          ctx.hostListProviderService(), this.readEndpoint, HostRole.READER);
    }

    final @Nullable Connection conn;
    if (this.verifyNewConnections) {
      conn = this.verifier.getVerifiedConnection(ctx, ctx.properties(), readerHost, HostRole.READER, null);
    } else {
      conn = ctx.connect(readerHost, ctx.properties());
    }

    if (conn == null) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.failedToConnectToReader",
              new Object[] {this.readEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
      return;
    }

    final HostSpec finalReaderHost = readerHost;
    LOGGER.finest(() -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
        new Object[] {finalReaderHost.getHostAndPort()}));

    ctx.bindReader(conn, readerHost);
    ctx.switchCurrentConnectionTo(conn, readerHost);
    ctx.markReaderFromPool(Boolean.TRUE.equals(ctx.pluginService().isPooledConnection()));
    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
        new Object[] {this.readEndpoint}));
  }
}
