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
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyRoleOnConnect;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.TopologyRefresherImpl;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.CompositeSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.SqlRoutingSignal;
import software.amazon.jdbc.plugin.readwritesplitting.source.TopologyHostsCandidateSource;
import software.amazon.jdbc.plugin.readwritesplitting.updater.RoleBasedUpdatePolicy;

/**
 * Automatic read/write splitting plugin ({@code autoReadWriteSplitting}): routes queries based on
 * SQL analysis at statement-creation time (falling back to {@code setReadOnly}). Requires the
 * {@code sqlParser} plugin to be ordered before it.
 */
public class AutoReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  public AutoReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, auto(properties));
  }

  /** Builds the SQL-driven (prepare-time) routing assembly with a sticky reader. */
  protected static RwSplitHelpers auto(final Properties props) {
    final TopologyRoleClassifier roleClassifier = new TopologyRoleClassifier();
    final String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    final boolean verifyRole = VERIFY_INITIAL_CONNECTION_ROLE.getBoolean(props);

    final TopologyWriterResolver writerResolver = new TopologyWriterResolver();
    return RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal()))
        .switchGate(new TransactionAwareGate(true, true))
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
