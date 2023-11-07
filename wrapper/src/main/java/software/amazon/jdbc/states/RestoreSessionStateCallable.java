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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface RestoreSessionStateCallable {
  /**
   * Restores partial session state from saved values to a connection.
   *
   * @param sessionState Session state flags for from-connection
   * @param to   The connection to transfer state to
   * @param readOnly ReadOnly flag to set to
   * @param autoCommit AutoCommit flag to set to
   * @return true, if session state is restored successful and no default logic should be executed after.
   *          False, if default logic should be executed.
   */
  boolean restoreSessionState(
      final @NonNull EnumSet<SessionDirtyFlag> sessionState,
      final @NonNull Connection to,
      final @Nullable Boolean readOnly,
      final @Nullable Boolean autoCommit)
      throws SQLException;
}
