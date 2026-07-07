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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.readwritesplitting.cache.ConnectionCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.RoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.SwitchGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.InitialConnectionHandler;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.TopologyRefresher;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.ReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.RoutingSignal;
import software.amazon.jdbc.plugin.readwritesplitting.updater.ConnectionUpdatePolicy;

/**
 * Immutable bundle of the helpers that define a {@link UnifiedReadWriteSplittingPlugin} assembly.
 * Factories build one of these to compose an existing or new plugin code.
 */
public class RwSplitHelpers {

  public final RoleClassifier roleClassifier;
  public final RoutingSignal routingSignal;
  public final SwitchGate switchGate;
  public final TopologyRefresher topologyRefresher;
  public final WriterResolver writerResolver;
  public final ReaderResolver readerResolver;
  public final ConnectionCachePolicy cachePolicy;
  public final InitialConnectionHandler initialConnectionHandler;
  public final ConnectionUpdatePolicy connectionUpdatePolicy;
  private final List<SnapshotContributor> snapshotContributors;

  private RwSplitHelpers(final Builder b) {
    this.roleClassifier = b.roleClassifier;
    this.routingSignal = b.routingSignal;
    this.switchGate = b.switchGate;
    this.topologyRefresher = b.topologyRefresher;
    this.writerResolver = b.writerResolver;
    this.readerResolver = b.readerResolver;
    this.cachePolicy = b.cachePolicy;
    this.initialConnectionHandler = b.initialConnectionHandler;
    this.connectionUpdatePolicy = b.connectionUpdatePolicy;
    this.snapshotContributors = b.snapshotContributors;
  }

  public List<SnapshotContributor> snapshotContributors() {
    return this.snapshotContributors;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link RwSplitHelpers}. */
  public static class Builder {
    private @Nullable RoleClassifier roleClassifier;
    private @Nullable RoutingSignal routingSignal;
    private @Nullable SwitchGate switchGate;
    private @Nullable TopologyRefresher topologyRefresher;
    private @Nullable WriterResolver writerResolver;
    private @Nullable ReaderResolver readerResolver;
    private @Nullable ConnectionCachePolicy cachePolicy;
    private @Nullable InitialConnectionHandler initialConnectionHandler;
    private @Nullable ConnectionUpdatePolicy connectionUpdatePolicy;
    private final List<SnapshotContributor> snapshotContributors = new ArrayList<>();

    public Builder roleClassifier(final RoleClassifier v) {
      this.roleClassifier = v;
      return this;
    }

    public Builder routingSignal(final RoutingSignal v) {
      this.routingSignal = v;
      return this;
    }

    public Builder switchGate(final SwitchGate v) {
      this.switchGate = v;
      return this;
    }

    public Builder topologyRefresher(final TopologyRefresher v) {
      this.topologyRefresher = v;
      return this;
    }

    public Builder writerResolver(final WriterResolver v) {
      this.writerResolver = v;
      return this;
    }

    public Builder readerResolver(final ReaderResolver v) {
      this.readerResolver = v;
      return this;
    }

    public Builder cachePolicy(final ConnectionCachePolicy v) {
      this.cachePolicy = v;
      return this;
    }

    public Builder initialConnectionHandler(final InitialConnectionHandler v) {
      this.initialConnectionHandler = v;
      return this;
    }

    public Builder connectionUpdatePolicy(final ConnectionUpdatePolicy v) {
      this.connectionUpdatePolicy = v;
      return this;
    }

    public Builder addSnapshotContributor(final @Nullable SnapshotContributor v) {
      if (v != null) {
        this.snapshotContributors.add(v);
      }
      return this;
    }

    public RwSplitHelpers build() {
      if (roleClassifier == null || routingSignal == null || switchGate == null
          || topologyRefresher == null || writerResolver == null || readerResolver == null
          || cachePolicy == null || initialConnectionHandler == null
          || connectionUpdatePolicy == null) {
        throw new IllegalStateException("All read/write splitting helpers must be provided.");
      }
      return new RwSplitHelpers(this);
    }
  }
}
