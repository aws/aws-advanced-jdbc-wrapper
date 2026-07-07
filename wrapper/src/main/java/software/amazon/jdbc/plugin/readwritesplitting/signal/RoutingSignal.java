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

package software.amazon.jdbc.plugin.readwritesplitting.signal;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Determines the desired connection role for a given JDBC call ("what is the signal to switch?").
 * Implementations cover the {@code setReadOnly} signal, SQL-based routing, and their composition.
 */
public interface RoutingSignal {

  /**
   * Extra JDBC method names this signal needs subscribed beyond the plugin's defaults (e.g. the
   * statement-creation and execute methods for SQL routing).
   *
   * @return an unmodifiable set of method names; empty by default
   */
  default Set<String> extraSubscribedMethods() {
    return Collections.emptySet();
  }

  /**
   * Resolves the desired role for this call. {@link TargetRole#NO_DECISION} means the method is not
   * a routing trigger for this signal (the signal abstains).
   *
   * @param ctx        the read/write splitting context
   * @param methodName the JDBC method being invoked
   * @param args       the method arguments (may be {@code null})
   * @return the desired target role
   */
  TargetRole resolve(RwSplitContext ctx, String methodName, @Nullable Object[] args)
      throws SQLException;

  /**
   * Resolves the desired role for an already-bound plain {@code Statement} from the parsed SQL,
   * used only for the optional statement-rebinding path. Returns {@link TargetRole#NO_DECISION}
   * by default (signals that do not route on SQL do not reroute bound statements).
   *
   * @param ctx the read/write splitting context
   * @return the desired role, or {@link TargetRole#NO_DECISION} if this signal does not apply
   */
  default TargetRole resolveForBoundStatement(RwSplitContext ctx) throws SQLException {
    return TargetRole.NO_DECISION;
  }
}
