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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * {@link LoadBalancingPolicy} that rotates readers per read query (query-level load balancing).
 * When {@code includeWriter} is set, the writer is also an eligible target (role {@code null}).
 * Rotation is confined by the plugin to an established read-only phase, so it never changes role.
 */
public class PerQueryBalancedReaderPolicy implements LoadBalancingPolicy {

  private final boolean includeWriter;
  private final String readerSelectorStrategy;

  public PerQueryBalancedReaderPolicy(final boolean includeWriter, final String readerSelectorStrategy) {
    this.includeWriter = includeWriter;
    this.readerSelectorStrategy = readerSelectorStrategy;
  }

  @Override
  public @Nullable HostSpec pickReader(final RwSplitContext ctx, final List<HostSpec> candidates)
      throws SQLException {
    final HostRole role = this.includeWriter ? null : HostRole.READER;
    return ctx.pluginService().getHostSpecByStrategy(candidates, role, this.readerSelectorStrategy);
  }

  @Override
  public boolean isPerQuery() {
    return true;
  }

  public boolean includesWriter() {
    return this.includeWriter;
  }
}
