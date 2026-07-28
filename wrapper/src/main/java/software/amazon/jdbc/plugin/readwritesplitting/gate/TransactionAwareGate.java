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
import java.util.Optional;
import java.util.logging.Logger;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.signal.TargetRole;
import software.amazon.jdbc.util.Messages;

/**
 * {@link SwitchGate} that pins the current connection at transaction boundaries.
 *
 * <p>Pinning rules (a subset applies depending on the constructor flags, to match legacy behavior):
 * <ul>
 *   <li>an explicit {@code /*@keep* /} routing hint (when {@code honorKeepHint});</li>
 *   <li>a transaction is in progress;</li>
 *   <li>autocommit is explicitly disabled (when {@code pinOnAutoCommitOff}) — the next statement
 *       would implicitly start a transaction.</li>
 * </ul>
 *
 * <p>The legacy {@code setReadOnly(false)}-in-transaction throw is role-specific (it depends on
 * whether the current host is the writer) and is therefore enforced by the plugin orchestration,
 * which owns the role check; this gate only decides pinning.
 */
public class TransactionAwareGate implements SwitchGate {

  private static final Logger LOGGER = Logger.getLogger(TransactionAwareGate.class.getName());

  private final boolean honorKeepHint;
  private final boolean pinOnAutoCommitOff;

  /** Classic gate: pins only while a transaction is in progress. */
  public TransactionAwareGate() {
    this(false, false);
  }

  public TransactionAwareGate(final boolean honorKeepHint, final boolean pinOnAutoCommitOff) {
    this.honorKeepHint = honorKeepHint;
    this.pinOnAutoCommitOff = pinOnAutoCommitOff;
  }

  @Override
  public boolean canSwitch(final RwSplitContext ctx, final TargetRole desired) throws SQLException {
    if (desired == TargetRole.KEEP || desired == TargetRole.NO_DECISION) {
      return false;
    }

    if (this.honorKeepHint) {
      final PluginCallContext callContext = ctx.pluginService().getCallContext();
      if (callContext != null
          && callContext.getAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.class)
              == RoutingHint.KEEP) {
        return false;
      }
    }

    // An active XA transaction branch pins the connection just like a local transaction: the
    // physical session must not change until the transaction manager resolves the branch. This is
    // tracked separately from isInTransaction() because XA branches run without BEGIN/COMMIT SQL.
    if (ctx.pluginService().isXaTransactionActive()) {
      LOGGER.fine(() -> Messages.get("ReadWriteSplittingPlugin.stayedOnConnectionForXaTransaction"));
      return false;
    }

    if (ctx.pluginService().isInTransaction()) {
      return false;
    }

    if (this.pinOnAutoCommitOff) {
      try {
        final Optional<Boolean> autoCommit = ctx.pluginService().getSessionStateService().getAutoCommit();
        if (autoCommit.isPresent() && !autoCommit.get()) {
          return false;
        }
      } catch (final SQLException e) {
        // If autocommit state cannot be determined, fall back to allowing the switch.
        return true;
      }
    }

    return true;
  }
}
