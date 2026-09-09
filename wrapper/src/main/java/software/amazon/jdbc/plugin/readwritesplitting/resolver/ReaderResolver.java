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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Establishes a reader connection and switches the current connection to it ("how to reach a
 * reader?"). The default implementation composes a
 * {@link software.amazon.jdbc.plugin.readwritesplitting.source.ReaderCandidateSource} with a
 * {@link software.amazon.jdbc.plugin.readwritesplitting.balancer.LoadBalancingPolicy} and applies
 * the per-call connect/retry loop.
 */
public interface ReaderResolver {

  /**
   * Establishes a fresh reader connection (selecting a candidate, connecting, binding, and
   * switching the current connection to it). Mirrors the legacy {@code initializeReaderConnection}.
   *
   * @param ctx the read/write splitting context
   * @throws SQLException if no reader connection could be established
   */
  void switchToReader(RwSplitContext ctx) throws SQLException;

  /**
   * Closes the cached reader connection if it is no longer valid (e.g. its host left the allowed
   * host list). No-op by default; overridden by topology-aware resolvers.
   *
   * @param ctx the read/write splitting context
   */
  default void closeStaleReaderIfNecessary(RwSplitContext ctx) {
    // No-op by default.
  }

  /**
   * Whether this resolver performs query-level load balancing (a fresh reader is selected on each
   * read-routing decision rather than a single sticky reader being reused). No-op by default;
   * {@code false} keeps the sticky-reader behavior.
   *
   * @return {@code true} for per-query reader balancing; {@code false} (default) for sticky
   */
  default boolean isPerQuery() {
    return false;
  }

  /**
   * Returns the reader candidates that could serve a fresh reader route, for optional
   * replica-lag evaluation. The default has no topology-backed candidates.
   *
   * @param ctx the read/write splitting context
   * @return reader candidates eligible for this resolver's routing policy
   * @throws SQLException if candidate resolution fails
   */
  default List<HostSpec> getReaderCandidatesForLag(final RwSplitContext ctx) throws SQLException {
    return Collections.emptyList();
  }

  /**
   * Whether the resolver routes through an endpoint whose host does not identify the backend
   * database instance. Such resolvers must evaluate replica lag from cached Aurora topology.
   *
   * @return {@code true} when the route uses an opaque endpoint
   */
  default boolean routesThroughOpaqueEndpoint() {
    return false;
  }
}
