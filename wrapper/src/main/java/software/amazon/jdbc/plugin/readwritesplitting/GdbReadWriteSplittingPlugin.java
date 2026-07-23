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
import java.util.Properties;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.GdbInitAndVerify;
import software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyRoleOnConnect;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.TopologyRefresherImpl;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.GdbWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.TopologyWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.WriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.source.GdbRegionFilteredHostsCandidateSource;
import software.amazon.jdbc.plugin.readwritesplitting.updater.RoleBasedUpdatePolicy;
import software.amazon.jdbc.util.Pair;

/**
 * Global Database read/write splitting plugin ({@code gdbReadWriteSplitting}): topology-aware with
 * accessible/home region rules and Global Write Forwarding. Region configuration is held by
 * {@link GdbSettings}.
 */
public class GdbReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  public GdbReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, gdb(properties));
  }

  /** Constructor for subclasses that supply their own helper assembly. */
  protected GdbReadWriteSplittingPlugin(
      final PluginService pluginService, final Properties properties, final RwSplitHelpers helpers) {
    super(pluginService, properties, helpers);
  }

  /** Builds the Global Database assembly (region rules + GWF) with {@code setReadOnly} routing. */
  protected static RwSplitHelpers gdb(final Properties props) {
    return gdbHelpers(props, new ReadOnlyFlagSignal(), new TransactionAwareGate());
  }

  /**
   * Shared Global Database assembly builder, parameterized by routing signal and switch gate so
   * the SQL-routed variant can reuse it.
   */
  protected static RwSplitHelpers gdbHelpers(
      final Properties props,
      final software.amazon.jdbc.plugin.readwritesplitting.signal.RoutingSignal routingSignal,
      final software.amazon.jdbc.plugin.readwritesplitting.gate.SwitchGate switchGate) {
    final TopologyRoleClassifier roleClassifier = new TopologyRoleClassifier();
    final String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    final boolean verifyRole = VERIFY_INITIAL_CONNECTION_ROLE.getBoolean(props);
    final GdbSettings settings = new GdbSettings(props);

    final WriterResolver writerResolver = new GdbWriterResolver(settings, new TopologyWriterResolver());
    return RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(routingSignal)
        .switchGate(switchGate)
        .topologyRefresher(new TopologyRefresherImpl())
        .writerResolver(writerResolver)
        .readerResolver(new TopologyReaderResolver(
            new GdbRegionFilteredHostsCandidateSource(settings), readerLoadBalancer(props, strategy),
            writerResolver))
        .cachePolicy(new DefaultCachePolicy(props))
        .initialConnectionHandler(new GdbInitAndVerify(settings, new VerifyRoleOnConnect(strategy, verifyRole)))
        .connectionUpdatePolicy(new RoleBasedUpdatePolicy(roleClassifier))
        .addSnapshotContributor(RwSplitSnapshots.readerStrategy(strategy))
        .addSnapshotContributor(() -> {
          final List<Pair<String, Object>> state = new ArrayList<>();
          settings.addSnapshotState(state);
          return state;
        })
        .build();
  }
}
