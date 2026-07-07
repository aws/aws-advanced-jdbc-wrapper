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

package software.amazon.jdbc.plugin.readwritesplitting.balancer;

import java.sql.SQLException;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Chooses the reader target for a request from a set of candidates ("sticky vs per-query
 * balancing"). Implementations cover a sticky reader (reuse the cached reader) and per-query
 * balancing (rotate readers within an established read-only phase).
 */
public interface LoadBalancingPolicy {

  /**
   * Selects the reader target for this request, or {@code null} when no eligible target is
   * available.
   *
   * @param ctx        the read/write splitting context
   * @param candidates the eligible reader candidates
   * @return the chosen reader host, or {@code null} if none is available
   */
  @Nullable HostSpec pickReader(RwSplitContext ctx, List<HostSpec> candidates) throws SQLException;

  /**
   * Whether this policy rotates readers per read query (query-level load balancing). When
   * {@code true}, the plugin selects a fresh reader on each read-routing decision within an
   * established read-only phase instead of reusing a single sticky reader.
   *
   * @return {@code true} for per-query balancing; {@code false} (default) for a sticky reader
   */
  default boolean isPerQuery() {
    return false;
  }
}
