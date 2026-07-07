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
import java.util.HashSet;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Composes a primary and a fallback {@link RoutingSignal}. The primary is consulted first; when it
 * returns {@link TargetRole#NO_DECISION}, the fallback is consulted. This lets the auto plugin route
 * primarily on SQL (at prepare time) while still honoring {@code setReadOnly}.
 */
public class CompositeSignal implements RoutingSignal {

  private final RoutingSignal primary;
  private final RoutingSignal fallback;
  private final Set<String> subscribed;

  public CompositeSignal(final RoutingSignal primary, final RoutingSignal fallback) {
    this.primary = primary;
    this.fallback = fallback;
    final Set<String> combined = new HashSet<>(primary.extraSubscribedMethods());
    combined.addAll(fallback.extraSubscribedMethods());
    this.subscribed = Collections.unmodifiableSet(combined);
  }

  @Override
  public Set<String> extraSubscribedMethods() {
    return this.subscribed;
  }

  @Override
  public TargetRole resolve(
      final RwSplitContext ctx, final String methodName, final @Nullable Object[] args)
      throws SQLException {
    final TargetRole role = this.primary.resolve(ctx, methodName, args);
    if (role != TargetRole.NO_DECISION) {
      return role;
    }
    return this.fallback.resolve(ctx, methodName, args);
  }

  @Override
  public TargetRole resolveForBoundStatement(final RwSplitContext ctx) throws SQLException {
    final TargetRole role = this.primary.resolveForBoundStatement(ctx);
    if (role != TargetRole.NO_DECISION) {
      return role;
    }
    return this.fallback.resolveForBoundStatement(ctx);
  }
}
