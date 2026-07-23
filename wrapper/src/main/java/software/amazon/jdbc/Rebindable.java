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
import java.sql.SQLException;

/**
 * Implemented by a statement wrapper whose underlying (target) statement can be re-created on a
 * different physical {@link Connection}. Used by read/write splitting to reroute a plain
 * {@code Statement} whose SQL is only known at execute time: because the target statement is bound
 * to the connection that created it, the wrapper must re-create it on the routed connection.
 *
 * <p>A handle to the invoking wrapper is published on the per-call
 * {@link PluginCallContext} so a plugin (which only receives the raw target statement as
 * {@code methodInvokeOn}) can trigger the rebind.
 */
public interface Rebindable {

  /** The connection the current target statement is bound to. */
  Connection getBoundConnection() throws SQLException;

  /**
   * Whether this statement can currently be rebound to a different connection. Always {@code true}
   * for a plain {@code Statement}; a {@code PreparedStatement}/{@code CallableStatement} returns
   * {@code false} when it cannot be re-created (e.g. a one-shot stream/reader parameter was bound,
   * a batch is pending, or rebinding was not enabled for it).
   *
   * @return {@code true} if {@link #rebind(Connection)} can be applied
   */
  default boolean canRebind() {
    return true;
  }

  /**
   * Re-creates the underlying target statement on {@code newConnection}, transferring its
   * configuration, and closes the previous target. After this call, subsequent execution runs
   * against the new target statement.
   *
   * @param newConnection the underlying (target) connection to rebind to
   */
  void rebind(Connection newConnection) throws SQLException;
}
