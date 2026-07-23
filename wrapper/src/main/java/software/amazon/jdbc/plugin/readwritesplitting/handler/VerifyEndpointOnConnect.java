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

package software.amazon.jdbc.plugin.readwritesplitting.handler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Endpoint (Simple) {@link InitialConnectionHandler}: verifies the initial connection role via a
 * retry loop, based on the RDS URL type or an explicit {@code verifyInitialConnectionType}. Ports
 * the legacy {@code SimpleReadWriteSplittingPlugin.connect}.
 */
public class VerifyEndpointOnConnect implements InitialConnectionHandler {

  private final boolean verifyNewConnections;
  private final @Nullable HostRole verifyOpenedConnectionType;
  private final RdsUtils rdsUtils;
  private final EndpointConnectionVerifier verifier;

  public VerifyEndpointOnConnect(
      final boolean verifyNewConnections,
      final @Nullable HostRole verifyOpenedConnectionType,
      final RdsUtils rdsUtils,
      final EndpointConnectionVerifier verifier) {
    this.verifyNewConnections = verifyNewConnections;
    this.verifyOpenedConnectionType = verifyOpenedConnectionType;
    this.rdsUtils = rdsUtils;
    this.verifier = verifier;
  }

  @Override
  public Connection onConnect(
      final RwSplitContext ctx,
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    if (!isInitialConnection || !this.verifyNewConnections) {
      return connectFunc.call();
    }

    final RdsUrlType type = this.rdsUtils.identifyRdsType(hostSpec.getHost());

    Connection conn = null;
    if (type == RdsUrlType.RDS_WRITER_CLUSTER
        || type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        || this.verifyOpenedConnectionType == HostRole.WRITER) {
      conn = this.verifier.getVerifiedConnection(ctx, ctx.properties(), hostSpec, HostRole.WRITER, connectFunc);
    } else if (type == RdsUrlType.RDS_READER_CLUSTER
        || this.verifyOpenedConnectionType == HostRole.READER) {
      conn = this.verifier.getVerifiedConnection(ctx, ctx.properties(), hostSpec, HostRole.READER, connectFunc);
    }

    if (conn == null) {
      conn = connectFunc.call();
    }
    setInitialConnectionHostSpec(ctx, conn, hostSpec);
    return conn;
  }

  private void setInitialConnectionHostSpec(
      final RwSplitContext ctx, final Connection conn, @Nullable HostSpec hostSpec) {
    if (hostSpec == null) {
      try {
        hostSpec = ctx.pluginService().identifyConnection(conn);
      } catch (final Exception e) {
        // ignore
      }
    }

    final HostListProviderService hostListProviderService = ctx.hostListProviderService();
    if (hostSpec != null && hostListProviderService != null) {
      hostListProviderService.setInitialConnectionHostSpec(hostSpec);
    }
  }
}
