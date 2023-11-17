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
import software.amazon.jdbc.HostSpec;

public interface SessionStateTransferCallable {

  /**
   * Transfers session state from one connection to another.
   *
   * @param sessionState Session state flags for from-connection
   * @param src The source connection to transfer state from
   * @param srcHostSpec The source connection {@link HostSpec}
   * @param dest   The destination connection to transfer state to
   * @param destHostSpec The destination connection {@link HostSpec}
   * @return true, if session state transfer is successful and no default logic should be executed after.
   *          False, if default logic should be executed.
   */
  boolean transferSessionState(
      final @NonNull EnumSet<SessionDirtyFlag> sessionState,
      final @NonNull Connection src,
      final @Nullable HostSpec srcHostSpec,
      final @NonNull Connection dest,
      final @Nullable HostSpec destHostSpec) throws SQLException;
}
