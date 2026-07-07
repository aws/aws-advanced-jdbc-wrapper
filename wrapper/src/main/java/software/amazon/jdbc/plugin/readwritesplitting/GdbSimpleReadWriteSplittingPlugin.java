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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.cache.NanoTimeSource;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.EndpointRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.SwitchGate;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.EndpointConnectionVerifier;
import software.amazon.jdbc.plugin.readwritesplitting.handler.GdbInitAndVerify;
import software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyEndpointOnConnect;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.EndpointWriterHostRefresher;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.EndpointReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.EndpointWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.GdbWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.RoutingSignal;
import software.amazon.jdbc.plugin.readwritesplitting.updater.VerifiedEndpointUpdatePolicy;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

/**
 * Global Database read/write splitting plugin using configured endpoints instead of cluster
 * topology ({@code gdbSimpleReadWriteSplitting}): routes on {@code setReadOnly} between the
 * configured write/read endpoints, while applying the Global Database region rules and Global Write
 * Forwarding to the write endpoint. Region configuration is held by {@link GdbSettings}.
 */
public class GdbSimpleReadWriteSplittingPlugin extends SimpleReadWriteSplittingPlugin {

  public GdbSimpleReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, gdbSimple(properties));
  }

  /** Constructor for subclasses that supply their own helper assembly. */
  protected GdbSimpleReadWriteSplittingPlugin(
      final PluginService pluginService, final Properties properties, final RwSplitHelpers helpers) {
    super(pluginService, properties, helpers);
  }

  /** Builds the endpoint-based Global Database assembly with {@code setReadOnly} routing. */
  protected static RwSplitHelpers gdbSimple(final Properties props) {
    return gdbEndpointHelpers(props, new ReadOnlyFlagSignal(), new TransactionAwareGate());
  }

  /**
   * Shared endpoint-based Global Database assembly builder, parameterized by routing signal and
   * switch gate so the SQL-routed variant can reuse it. Fails fast when a required endpoint is
   * missing. The write endpoint host is seeded before writer resolution (via
   * {@link EndpointWriterHostRefresher}) so the {@link GdbWriterResolver} can evaluate its region
   * for accessible-region / home-region rules and Global Write Forwarding.
   */
  protected static RwSplitHelpers gdbEndpointHelpers(
      final Properties props, final RoutingSignal routingSignal, final SwitchGate switchGate) {
    final String writeEndpoint = SRW_WRITE_ENDPOINT.getString(props);
    if (StringUtils.isNullOrEmpty(writeEndpoint)) {
      throw new RuntimeException(Messages.get(
          "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
          new Object[] {SRW_WRITE_ENDPOINT.name}));
    }
    final String readEndpoint = SRW_READ_ENDPOINT.getString(props);
    if (StringUtils.isNullOrEmpty(readEndpoint)) {
      throw new RuntimeException(Messages.get(
          "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
          new Object[] {SRW_READ_ENDPOINT.name}));
    }

    final boolean verifyNewConnections = VERIFY_NEW_SRW_CONNECTIONS.getBoolean(props);
    final HostRole verifyOpenedConnectionType =
        HostRole.verifyConnectionTypeFromValue(VERIFY_INITIAL_CONNECTION_TYPE.getString(props));
    final int retryIntervalMs = SRW_CONNECT_RETRY_INTERVAL_MS.getInteger(props);
    final long retryTimeoutMs = SRW_CONNECT_RETRY_TIMEOUT_MS.getInteger(props);

    final GdbSettings settings = new GdbSettings(props);
    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(retryTimeoutMs, retryIntervalMs, NanoTimeSource.SYSTEM);
    final EndpointRoleClassifier roleClassifier = new EndpointRoleClassifier(writeEndpoint, readEndpoint);

    return RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(routingSignal)
        .switchGate(switchGate)
        .topologyRefresher(new EndpointWriterHostRefresher(writeEndpoint))
        .writerResolver(new GdbWriterResolver(settings,
            new EndpointWriterResolver(writeEndpoint, verifyNewConnections, verifier)))
        .readerResolver(new EndpointReaderResolver(readEndpoint, verifyNewConnections, verifier,
            QUERY_LEVEL_LOAD_BALANCING.getBoolean(props)))
        .cachePolicy(new DefaultCachePolicy(props))
        .initialConnectionHandler(new GdbInitAndVerify(settings,
            new VerifyEndpointOnConnect(verifyNewConnections, verifyOpenedConnectionType, new RdsUtils(), verifier)))
        .connectionUpdatePolicy(new VerifiedEndpointUpdatePolicy(roleClassifier, verifyNewConnections))
        .addSnapshotContributor(RwSplitSnapshots.endpoint(
            verifyNewConnections, writeEndpoint, readEndpoint, verifyOpenedConnectionType,
            retryIntervalMs, retryTimeoutMs))
        .addSnapshotContributor(() -> {
          final List<Pair<String, Object>> state = new ArrayList<>();
          settings.addSnapshotState(state);
          return state;
        })
        .build();
  }
}
