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

package software.amazon.jdbc.states;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SessionStateHelper {

  /**
   * Transfers basic session state from one connection to another.
   *
   * @param sessionState Session state of from-connection
   * @param from The connection to transfer state from
   * @param to   The connection to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  public void transferSessionState(
      final EnumSet<SessionDirtyFlag> sessionState,
      final Connection from,
      final Connection to) throws SQLException {

    if (from == null || to == null) {
      return;
    }

    if (sessionState.contains(SessionDirtyFlag.READONLY)) {
      to.setReadOnly(from.isReadOnly());
    }
    if (sessionState.contains(SessionDirtyFlag.AUTO_COMMIT)) {
      to.setAutoCommit(from.getAutoCommit());
    }
    if (sessionState.contains(SessionDirtyFlag.TRANSACTION_ISOLATION)) {
      to.setTransactionIsolation(from.getTransactionIsolation());
    }
    if (sessionState.contains(SessionDirtyFlag.CATALOG)) {
      to.setCatalog(from.getCatalog());
    }
    if (sessionState.contains(SessionDirtyFlag.SCHEMA)) {
      to.setSchema(from.getSchema());
    }
    if (sessionState.contains(SessionDirtyFlag.TYPE_MAP)) {
      to.setTypeMap(from.getTypeMap());
    }
    if (sessionState.contains(SessionDirtyFlag.HOLDABILITY)) {
      to.setHoldability(from.getHoldability());
    }
    if (sessionState.contains(SessionDirtyFlag.NETWORK_TIMEOUT)) {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      to.setNetworkTimeout(executorService, from.getNetworkTimeout());
      executorService.shutdown();
    }
  }

  /**
   * Restores partial session state from saved values to a connection.
   *
   * @param to   The connection to transfer state to
   * @param readOnly ReadOnly flag to set to
   * @param autoCommit AutoCommit flag to set to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  public void restoreSessionState(final Connection to, final Boolean readOnly, final Boolean autoCommit)
      throws SQLException {

    if (to == null) {
      return;
    }

    if (readOnly != null) {
      to.setReadOnly(readOnly);
    }
    if (autoCommit != null) {
      to.setAutoCommit(autoCommit);
    }
  }

}
