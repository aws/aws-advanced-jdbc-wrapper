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

package software.amazon.jdbc.plugin.readwritesplitting.updater;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.RoleClassifier;

/**
 * Endpoint (Simple) {@link ConnectionUpdatePolicy}: adopt the current connection as the cached
 * writer/reader only when it is a new connection matching the endpoint role, optionally verifying
 * the role via the database. Ports the legacy
 * {@code SimpleReadWriteSplittingPlugin.shouldUpdate*Connection}.
 */
public class VerifiedEndpointUpdatePolicy implements ConnectionUpdatePolicy {

  private final RoleClassifier roleClassifier;
  private final boolean verifyNewConnections;

  public VerifiedEndpointUpdatePolicy(final RoleClassifier roleClassifier, final boolean verifyNewConnections) {
    this.roleClassifier = roleClassifier;
    this.verifyNewConnections = verifyNewConnections;
  }

  @Override
  public boolean shouldUpdateWriter(
      final RwSplitContext ctx, final Connection currentConnection, final HostSpec currentHost)
      throws SQLException {
    return this.roleClassifier.isWriter(currentHost)
        && !Objects.equals(currentConnection, ctx.writerConnection())
        && (!this.verifyNewConnections
            || ctx.pluginService().getHostRole(currentConnection) == HostRole.WRITER);
  }

  @Override
  public boolean shouldUpdateReader(
      final RwSplitContext ctx, final Connection currentConnection, final HostSpec currentHost)
      throws SQLException {
    return this.roleClassifier.isReader(currentHost)
        && !Objects.equals(currentConnection, ctx.readerConnection())
        && (!this.verifyNewConnections
            || ctx.pluginService().getHostRole(currentConnection) == HostRole.READER);
  }
}
