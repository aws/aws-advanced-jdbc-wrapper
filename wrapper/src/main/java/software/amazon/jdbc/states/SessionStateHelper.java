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
   * Transfers session state from source connection to destination connection.
   *
   * @param sessionState Session state of source connection
   * @param src The source connection to transfer state from
   * @param dest   The destination connection to transfer state to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  public void transferSessionState(
      final EnumSet<SessionDirtyFlag> sessionState,
      final Connection src,
      final Connection dest) throws SQLException {

    if (src == null || dest == null) {
      return;
    }

    if (sessionState.contains(SessionDirtyFlag.READONLY)) {
      dest.setReadOnly(src.isReadOnly());
    }
    if (sessionState.contains(SessionDirtyFlag.AUTO_COMMIT)) {
      dest.setAutoCommit(src.getAutoCommit());
    }
    if (sessionState.contains(SessionDirtyFlag.TRANSACTION_ISOLATION)) {
      dest.setTransactionIsolation(src.getTransactionIsolation());
    }
    if (sessionState.contains(SessionDirtyFlag.CATALOG)) {
      dest.setCatalog(src.getCatalog());
    }
    if (sessionState.contains(SessionDirtyFlag.SCHEMA)) {
      dest.setSchema(src.getSchema());
    }
    if (sessionState.contains(SessionDirtyFlag.TYPE_MAP)) {
      dest.setTypeMap(src.getTypeMap());
    }
    if (sessionState.contains(SessionDirtyFlag.HOLDABILITY)) {
      dest.setHoldability(src.getHoldability());
    }
    if (sessionState.contains(SessionDirtyFlag.NETWORK_TIMEOUT)) {
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      dest.setNetworkTimeout(executorService, src.getNetworkTimeout());
      executorService.shutdown();
    }
  }

  /**
   * Restores partial session state from saved values to a connection.
   *
   * @param dest   The destination connection to transfer state to
   * @param readOnly ReadOnly flag to set to
   * @param autoCommit AutoCommit flag to set to
   * @throws SQLException if a database access error occurs, this method is called on a closed connection, this
   *                      method is called during a distributed transaction, or this method is called during a
   *                      transaction
   */
  public void restoreSessionState(final Connection dest, final Boolean readOnly, final Boolean autoCommit)
      throws SQLException {

    if (dest == null) {
      return;
    }

    if (readOnly != null) {
      dest.setReadOnly(readOnly);
    }
    if (autoCommit != null) {
      dest.setAutoCommit(autoCommit);
    }
  }

}
