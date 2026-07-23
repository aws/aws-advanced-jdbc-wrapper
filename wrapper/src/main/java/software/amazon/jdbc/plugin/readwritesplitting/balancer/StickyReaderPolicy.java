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
 * {@link LoadBalancingPolicy} that selects a reader using the configured host-selector strategy.
 * The unified plugin reuses a single cached reader (the sticky behavior) and only invokes this to
 * pick a new reader when none is cached.
 */
public class StickyReaderPolicy implements LoadBalancingPolicy {

  private final String readerSelectorStrategy;

  public StickyReaderPolicy(final String readerSelectorStrategy) {
    this.readerSelectorStrategy = readerSelectorStrategy;
  }

  @Override
  public @Nullable HostSpec pickReader(final RwSplitContext ctx, final List<HostSpec> candidates)
      throws SQLException {
    return ctx.pluginService().getHostSpecByStrategy(
        candidates, HostRole.READER, this.readerSelectorStrategy);
  }
}
