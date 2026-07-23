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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.readwritesplitting.cache.DefaultCachePolicy;
import software.amazon.jdbc.plugin.readwritesplitting.cache.NanoTimeSource;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.EndpointRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.gate.SwitchGate;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.handler.EndpointConnectionVerifier;
import software.amazon.jdbc.plugin.readwritesplitting.handler.VerifyEndpointOnConnect;
import software.amazon.jdbc.plugin.readwritesplitting.refresher.NoOpTopologyRefresher;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.EndpointReaderResolver;
import software.amazon.jdbc.plugin.readwritesplitting.resolver.EndpointWriterResolver;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.RoutingSignal;
import software.amazon.jdbc.plugin.readwritesplitting.updater.VerifiedEndpointUpdatePolicy;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

/**
 * Simple (endpoint-based) read/write splitting plugin ({@code srw}): routes on {@code setReadOnly}
 * between a configured write endpoint and read endpoint, without relying on cluster topology, with
 * optional connection-role verification.
 */
public class SimpleReadWriteSplittingPlugin extends UnifiedReadWriteSplittingPlugin {

  public static final AwsWrapperProperty SRW_READ_ENDPOINT =
      new AwsWrapperProperty(
          "srwReadEndpoint",
          null,
          "The read-only endpoint that should be used to connect to a reader.");

  public static final AwsWrapperProperty SRW_WRITE_ENDPOINT =
      new AwsWrapperProperty(
          "srwWriteEndpoint",
          null,
          "The read-write/cluster endpoint that should be used to connect to the writer.");

  public static final AwsWrapperProperty VERIFY_NEW_SRW_CONNECTIONS =
      new AwsWrapperProperty(
          "verifyNewSrwConnections",
          "true",
          "Enables role verification for new connections made by the Simple Read/Write Splitting Plugin.",
          false,
          new String[] {"true", "false"});

  public static final AwsWrapperProperty SRW_CONNECT_RETRY_TIMEOUT_MS =
      new AwsWrapperProperty(
          "srwConnectRetryTimeoutMs",
          "60000",
          "Maximum allowed time for the retries opening a connection.");

  public static final AwsWrapperProperty SRW_CONNECT_RETRY_INTERVAL_MS =
      new AwsWrapperProperty(
          "srwConnectRetryIntervalMs",
          "1000",
          "Time between each retry of opening a connection.");

  public static final AwsWrapperProperty VERIFY_INITIAL_CONNECTION_TYPE =
      new AwsWrapperProperty(
          "verifyInitialConnectionType",
          null,
          "Force to verify the initial connection to be either a writer or a reader.");

  static {
    PropertyDefinition.registerPluginProperties(SimpleReadWriteSplittingPlugin.class);
  }

  public SimpleReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, simple(properties));
  }

  /** Constructor for subclasses that supply their own helper assembly. */
  protected SimpleReadWriteSplittingPlugin(
      final PluginService pluginService, final Properties properties, final RwSplitHelpers helpers) {
    super(pluginService, properties, helpers);
  }

  /** Builds the endpoint-based assembly with {@code setReadOnly} routing. */
  protected static RwSplitHelpers simple(final Properties props) {
    return endpointHelpers(props, new ReadOnlyFlagSignal(), new TransactionAwareGate(), new NoOpTopologyRefresher());
  }

  /**
   * Shared endpoint assembly builder, parameterized by routing signal and switch gate so the
   * SQL-routed variant can reuse it. Fails fast when a required endpoint is missing.
   */
  protected static RwSplitHelpers endpointHelpers(
      final Properties props,
      final RoutingSignal routingSignal,
      final SwitchGate switchGate,
      final NoOpTopologyRefresher topologyRefresher) {
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

    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(retryTimeoutMs, retryIntervalMs, NanoTimeSource.SYSTEM);
    final EndpointRoleClassifier roleClassifier = new EndpointRoleClassifier(writeEndpoint, readEndpoint);

    return RwSplitHelpers.builder()
        .roleClassifier(roleClassifier)
        .routingSignal(routingSignal)
        .switchGate(switchGate)
        .topologyRefresher(topologyRefresher)
        .writerResolver(new EndpointWriterResolver(writeEndpoint, verifyNewConnections, verifier))
        .readerResolver(new EndpointReaderResolver(readEndpoint, verifyNewConnections, verifier,
            QUERY_LEVEL_LOAD_BALANCING.getBoolean(props)))
        .cachePolicy(new DefaultCachePolicy(props))
        .initialConnectionHandler(
            new VerifyEndpointOnConnect(verifyNewConnections, verifyOpenedConnectionType, new RdsUtils(), verifier))
        .connectionUpdatePolicy(new VerifiedEndpointUpdatePolicy(roleClassifier, verifyNewConnections))
        .addSnapshotContributor(RwSplitSnapshots.endpoint(
            verifyNewConnections, writeEndpoint, readEndpoint, verifyOpenedConnectionType,
            retryIntervalMs, retryTimeoutMs))
        .build();
  }
}
