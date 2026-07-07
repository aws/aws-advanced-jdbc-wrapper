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

package software.amazon.jdbc;

import java.sql.Connection;
import javax.sql.XAConnection;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ConnectionInfo {
  private final Connection connection;
  private final boolean isPooled;
  private final @Nullable XAConnection xaConnection;

  public ConnectionInfo(Connection connection, boolean isPooled) {
    this(connection, isPooled, null);
  }

  public ConnectionInfo(Connection connection) {
    this(connection, false, null);
  }

  /**
   * Creates a {@link ConnectionInfo} that also carries the owning {@link XAConnection} for the XA
   * datasource path. The {@code connection} is the logical connection obtained from
   * {@code xaConnection.getConnection()}; {@code xaConnection} owns the physical session and the
   * {@code XAResource}.
   *
   * @param connection   the (logical) physical connection handed to the wrapper.
   * @param isPooled     whether the connection came from a pool.
   * @param xaConnection the owning XA connection, or null on the non-XA path.
   */
  public ConnectionInfo(Connection connection, boolean isPooled, @Nullable XAConnection xaConnection) {
    this.connection = connection;
    this.isPooled = isPooled;
    this.xaConnection = xaConnection;
  }

  public Connection getConnection() {
    return connection;
  }

  public boolean isPooled() {
    return isPooled;
  }

  /**
   * Returns the owning {@link XAConnection} when this info was produced on the XA datasource path,
   * or null otherwise.
   *
   * @return the owning XA connection, or null.
   */
  public @Nullable XAConnection getXaConnection() {
    return xaConnection;
  }
}
