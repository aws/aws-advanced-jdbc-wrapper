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

package software.amazon.jdbc.plugin.readwritesplitting.gate;

import java.sql.SQLException;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.signal.TargetRole;

/**
 * Decides whether a connection switch is permitted right now ("when to switch?"). Owns the
 * transaction-boundary rules and the pinning behavior.
 */
public interface SwitchGate {

  /**
   * Returns whether a switch to {@code desired} is allowed. When it returns {@code false} the
   * current connection is pinned (no switch is performed). Implementations may throw to reject an
   * illegal transition (e.g. {@code setReadOnly(false)} inside an active transaction).
   *
   * @param ctx     the read/write splitting context
   * @param desired the desired target role
   * @return {@code true} if switching is allowed, {@code false} to pin the current connection
   */
  boolean canSwitch(RwSplitContext ctx, TargetRole desired) throws SQLException;
}
