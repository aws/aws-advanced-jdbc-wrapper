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

import java.util.Properties;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyRoleOnConnect;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.TopologyRefresherImpl;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.source.TopologyHostsCandidateSource;
import software.amazon.jdbc.plugin.readwritesplitting.updater.RoleBasedUpdatePolicy;

/**
 * Topology-aware read/write splitting plugin ({@code readWriteSplitting}): routes on
 * {@code setReadOnly}, selects a reader from the cluster topology using the configured host
 * selector strategy, and caches a sticky reader. Composed from read/write splitting helpers on top
 * of {@link UnifiedReadWriteSplittingPlugin}.
 */
public class ReadWriteSplittingPlugin extends UnifiedReadWriteSplittingPlugin {

  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "readerHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host.");

  public static final AwsWrapperProperty VERIFY_INITIAL_CONNECTION_ROLE =
      new AwsWrapperProperty(
          "verifyInitialConnectionRole",
          "true",
          "Whether to verify the role of the initial connection by querying the database.");

  static {
    PropertyDefinition.registerPluginProperties(ReadWriteSplittingPlugin.class);
  }

  public ReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, topology(properties));
  }

  /** Constructor for subclasses that supply their own helper assembly. */
  protected ReadWriteSplittingPlugin(
      final PluginService pluginService, final Properties properties, final RwSplitHelpers helpers) {
    super(pluginService, properties, helpers);
  }

  /** Builds the topology-aware, {@code setReadOnly}-driven, sticky-reader assembly. */
  protected static RwSplitHelpers topology(final Properties props) {
    final TopologyRoleClassifier roleClassifier = new TopologyRoleClassifier();
    final String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    final boolean verifyRole = VERIFY_INITIAL_CONNECTION_ROLE.getBoolean(props);

    final TopologyWriterResolver writerResolver = new TopologyWriterResolver();
    return RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(new ReadOnlyFlagSignal())
        .switchGate(new TransactionAwareGate())
        .topologyRefresher(new TopologyRefresherImpl())
        .writerResolver(writerResolver)
        .readerResolver(new TopologyReaderResolver(
            new TopologyHostsCandidateSource(), readerLoadBalancer(props, strategy), writerResolver))
        .cachePolicy(new DefaultCachePolicy(props))
        .initialConnectionHandler(new VerifyRoleOnConnect(strategy, verifyRole))
        .connectionUpdatePolicy(new RoleBasedUpdatePolicy(roleClassifier))
        .addSnapshotContributor(RwSplitSnapshots.readerStrategy(strategy))
        .build();
  }
}
