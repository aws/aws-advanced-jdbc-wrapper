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

/**
 * Implemented by a wrapper object whose validity depends on the connection that was current when the
 * object was created (a {@code Statement}, {@code ResultSet} or {@code DatabaseMetaData} wrapper).
 *
 * <p>The wrapper records the internal connection it was created on. Before running a method that
 * would operate on the database, the wrapper pipeline compares the recorded connection against the
 * current one: a difference means the internal connection was swapped out (failover or read/write
 * splitting) after this object was created, so the object belongs to the previous session and must
 * not be used.
 *
 * <p>Recording the connection replaces asking the target driver which connection the underlying
 * object is bound to. Driver-reported bindings are not reliable for this purpose: a driver may hand
 * out an object bound to the physical connection while the wrapper holds a pooled or logical handle
 * that wraps that same physical connection (for example a {@code ResultSet} obtained from
 * {@code java.sql.Array#getResultSet}, or a statement from a pooled XA connection). Those are the
 * same session, but they are not reference-identical, which used to be reported as a stale object.
 * Recording it also avoids calling into the target driver ({@code Statement.getConnection()} may
 * block) on every guarded invocation.
 */
public interface ConnectionBoundObject {

  /**
   * The internal connection that was current when this object was created, or the connection its
   * underlying object was last re-created on.
   *
   * @return the recorded internal connection.
   */
  Connection getCreatedOnConnection();

  /**
   * Records the internal connection this object now belongs to. Called when the underlying object is
   * re-created on another connection (see {@link Rebindable#rebind(Connection)}), so the object is no
   * longer considered stale.
   *
   * @param connection the internal connection this object is now bound to.
   */
  void setCreatedOnConnection(Connection connection);
}
