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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.EndpointHostSpecs;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.handler.EndpointConnectionVerifier;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;

/**
 * {@link WriterResolver} that connects to the configured write endpoint, optionally verifying the
 * connection role. Ports the legacy {@code SimpleReadWriteSplittingPlugin.initializeWriterConnection}.
 * Fails fast at construction if the endpoint is missing/blank.
 */
public class EndpointWriterResolver implements WriterResolver {

  private final String writeEndpoint;
  private final boolean verifyNewConnections;
  private final EndpointConnectionVerifier verifier;

  public EndpointWriterResolver(
      final String writeEndpoint,
      final boolean verifyNewConnections,
      final EndpointConnectionVerifier verifier) {
    if (writeEndpoint == null || writeEndpoint.trim().isEmpty()) {
      throw new RuntimeException(Messages.get(
          "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
          new Object[] {"srwWriteEndpoint"}));
    }
    this.writeEndpoint = writeEndpoint;
    this.verifyNewConnections = verifyNewConnections;
    this.verifier = verifier;
  }

  @Override
  public WriterResolution resolveWriter(final RwSplitContext ctx) throws SQLException {
    HostSpec writerHost = ctx.writerHostSpec();
    if (writerHost == null) {
      writerHost = EndpointHostSpecs.create(
          ctx.hostListProviderService(), this.writeEndpoint, HostRole.WRITER);
    }

    final @Nullable Connection conn;
    if (this.verifyNewConnections) {
      conn = this.verifier.getVerifiedConnection(ctx, ctx.properties(), writerHost, HostRole.WRITER, null);
    } else {
      conn = ctx.connect(writerHost, ctx.properties());
    }

    if (conn == null) {
      ctx.logAndThrow(
          Messages.get("SimpleReadWriteSplittingPlugin.failedToConnectToWriter",
              new Object[] {this.writeEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
      return WriterResolution.stay();
    }
    return WriterResolution.connected(conn, writerHost);
  }
}
