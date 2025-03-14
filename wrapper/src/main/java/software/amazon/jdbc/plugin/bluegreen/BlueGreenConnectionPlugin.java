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

package software.amazon.jdbc.plugin.bluegreen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.bluegreen.routing.ConnectRouting;
import software.amazon.jdbc.plugin.bluegreen.routing.ExecuteRouting;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class BlueGreenConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenConnectionPlugin.class.getName());
  public static final AwsWrapperProperty BG_CONNECT_TIMEOUT = new AwsWrapperProperty(
      "bgConnectTimeout", "30000",
      "Connect timeout (in msec) during Blue/Green Deployment switchover.");

  protected static final ReentrantLock providerInitLock = new ReentrantLock();
  protected static BlueGreenStatusProvider provider = null;

  private static final Set<String> CLOSING_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "Connection.close",
          "Connection.abort",
          "Statement.close",
          "CallableStatement.close",
          "PreparedStatement.close",
          "ResultSet.close"
      )));

  static {
    PropertyDefinition.registerPluginProperties(BlueGreenConnectionPlugin.class);
  }

  protected final PluginService pluginService;
  protected final Properties props;
  protected final Set<String> subscribedMethods;
  protected Supplier<BlueGreenStatusProvider> initProviderSupplier;

  protected final TelemetryFactory telemetryFactory;
  protected final RdsUtils rdsUtils = new RdsUtils();

  protected BlueGreenStatus bgStatus = null;

  protected boolean isIamInUse = false;

  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {
    this(pluginService, props, () -> new BlueGreenStatusProvider(pluginService, props));
  }

  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull Supplier<BlueGreenStatusProvider> initProviderSupplier) {

    this.pluginService = pluginService;
    this.props = props;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.initProviderSupplier = initProviderSupplier;

    final Set<String> methods = new HashSet<>();
    /*
     We should NOT to subscribe to "forceConnect" pipeline since it's used by
     BG monitoring, and we don't want to intercept/block those monitoring connections.
    */
    methods.add("connect");
    methods.addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return this.subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

    if (this.bgStatus == null) {
      return regularOpenConnection(connectFunc, isInitialConnection);
    }

    if (isInitialConnection) {
      this.isIamInUse = this.pluginService.isPluginInUse(IamAuthConnectionPlugin.class);
    }

    BlueGreenRole hostRole = this.bgStatus.getRole(hostSpec);

    if (hostRole == null) {
      // Connection to a host that isn't participating in BG switchover.
      return regularOpenConnection(connectFunc, isInitialConnection);
    }

    Connection conn = null;
    ConnectRouting routing = this.bgStatus.getConnectRouting().stream()
        .filter(r -> r.isMatch(hostSpec, hostRole))
        .findFirst()
        .orElse(null);

    while (routing != null && conn == null) {
      conn = routing.apply(
          this,
          hostSpec,
          props,
          isInitialConnection,
          connectFunc,
          this.pluginService);

      if (conn == null) {

        this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

        routing = this.bgStatus.getConnectRouting().stream()
            .filter(r -> r.isMatch(hostSpec, hostRole))
            .findFirst()
            .orElse(null);
      }
    }

    if (conn == null) {
      conn = connectFunc.call();

      if (conn == null) {
        throw new SQLException("Can't open connection"); // TODO
      }
    }

    if (isInitialConnection) {
      // Provider should be initialized after connection is open and a dialect is properly identified.
      this.initProvider();
    }

    return conn;

//     if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {
//       // Don't open any connections while BG is in active phase
//       this.holdOn("connect");
//     }
//
//     // DNS changes are in progress. Node aliases are not reliable. It's required to use IP address.
//     boolean needUseIpAddress = this.bgStatus.getCurrentPhase() == BlueGreenPhases.POST_SWITCH_OVER;
//     LOGGER.info("needUseIpAddress: " + needUseIpAddress);
//
//     // Override regular "connect" flow in some special cases:
//     // - connecting to blue or green node, not to "-old1" node
//     // - need to use IP address rather than blue or green DNS alia (since DNS alias is unreliable)
//     // - need to alter connection properties for IAM authentication (since green node, now new blue node, may already change its name)
//     if (needUseIpAddress) {
//
//       String greenHost = this.bgStatus.getCorrespondingNodes().get(hostSpec.getHost());
//       String greenHostIpAddress = null;
//
//       if (greenHost != null) {
//         greenHostIpAddress = this.bgStatus.getHostIpAddresses().get(greenHost);
//       }
//
//       if (greenHost == null || greenHostIpAddress == null) {
//         // Collected blue-to-green node mapping may not yet, or no longer, exist.
//         // DNS maybe not yet being updated, or is already updated/propagated.
//         // Go with regular "connect" flow since rerouting isn't possible.
//         LOGGER.info("regular flow");
//         return regularOpenConnection(connectFunc, isInitialConnection);
//       }
//       LOGGER.info("greenHost: " + greenHost);
//       LOGGER.info("greenHostIpAddress: " + greenHostIpAddress);
//
//       HostSpec reroutedHostSpec = needUseIpAddress
//           ? this.pluginService.getHostSpecBuilder().copyFrom(hostSpec)
//               .host(greenHostIpAddress)
//               .hostId(hostSpec.getHostId())
//               .availability(HostAvailability.AVAILABLE)
//               .build()
//           : hostSpec;
//
//       final Properties copy = PropertyUtils.copyProperties(props);
//       copy.setProperty(BG_PLUGIN_REROUTED_CONNECTION_CALL, "true");
//       //copy.setProperty(IamAuthConnectionPlugin.IAM_EXPIRATION.name, "0");
//
//       if (this.isIamInUse) {
//         LOGGER.info("Reroute (IAM authentication) " + hostSpec.getHost() + " to " + reroutedHostSpec.getHost());
//       } else {
//         LOGGER.info("Reroute " + hostSpec.getHost() + " to " + reroutedHostSpec.getHost());
//       }
//
//       boolean useBlueNodeName = this.bgStatus.getGreenNodeChangedName();
//
//       // This loop can do up to 2 rounds.
//       while (true) {
//         if (this.isIamInUse) {
//           LOGGER.info("useBlueNodeName: " + useBlueNodeName);
//           copy.setProperty(IamAuthConnectionPlugin.IAM_HOST.name,
//               useBlueNodeName ? hostSpec.getHost() : greenHost);
//           LOGGER.info("iamHost: " + IamAuthConnectionPlugin.IAM_HOST.getString(copy));
//         }
//
//         try {
//           Connection conn = this.pluginService.connect(reroutedHostSpec, copy);
//
//           // We've just successfully connected with IAM token with blue node
//           // and we need to notify about it.
//           if (useBlueNodeName && !this.bgStatus.getGreenNodeChangedName()) {
//             BlueGreenStatusProvider tmpProvider = provider;
//             if (tmpProvider != null) {
//               tmpProvider.notifyGreenNodeChangedName();
//             }
//           }
//
//           return conn;
//
//         } catch (SQLException sqlException) {
//           if (!this.pluginService.isLoginException(sqlException)) {
//             throw sqlException;
//           }
//
//           if (useBlueNodeName) {
//             LOGGER.info("Green (new-blue) node doesn't accept neither green nor blue IAM token.");
//             throw sqlException;
//           } else {
//             // It seems that green node has already changed its host name to blue, and now
//             // it requires an IAM token based on blue node name.
//             useBlueNodeName = true;
//             LOGGER.info("Reroute failed since green node " + reroutedHostSpec.getHost()
//                 + " has already changed its name to blue. Trying again with an adjusted IAM token.");
//           }
//         }
//       }
//     }
//
//     return regularOpenConnection(connectFunc, isInitialConnection);
  }

  protected Connection regularOpenConnection(
      final JdbcCallable<Connection, SQLException> connectFunc,
      final boolean isInitialConnection) throws SQLException {

    Connection conn = connectFunc.call();

    if (isInitialConnection) {
      // Provider should be initialized after connection is open and a dialect is properly identified.
      this.initProvider();
    }

    return conn;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    this.initProvider();

    if (CLOSING_METHOD_NAMES.contains(methodName)) {
      return jdbcMethodFunc.call();
    }

    this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

    if (this.bgStatus == null) {
      return jdbcMethodFunc.call();
    }

    final HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();
    BlueGreenRole hostRole = this.bgStatus.getRole(currentHostSpec);

    if (hostRole == null) {
      // Connection to a host that isn't participating in BG switchover.
      return jdbcMethodFunc.call();
    }

    Optional<T> result = Optional.empty();
    ExecuteRouting routing = this.bgStatus.getExecuteRouting().stream()
        .filter(r -> r.isMatch(currentHostSpec, hostRole))
        .findFirst()
        .orElse(null);

    while (routing != null && !result.isPresent()) {
      result = routing.apply(
        this,
        resultClass,
        exceptionClass,
        methodInvokeOn,
        methodName,
        jdbcMethodFunc,
        jdbcMethodArgs,
        this.pluginService,
        this.props);

      if (!result.isPresent()) {

        this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

        routing = this.bgStatus.getExecuteRouting().stream()
            .filter(r -> r.isMatch(currentHostSpec, hostRole))
            .findFirst()
            .orElse(null);
      }
    }

    if(result.isPresent()) {
      return result.get();
    }

    return jdbcMethodFunc.call();
  }

  protected void initProvider() {
    if (provider == null) {
      providerInitLock.lock();
      try {
        if (provider == null) {
          provider = this.initProviderSupplier.get();
        }
      } finally {
        providerInitLock.unlock();
      }
    }
  }
}
