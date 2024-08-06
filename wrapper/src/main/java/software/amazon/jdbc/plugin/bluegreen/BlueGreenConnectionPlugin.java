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
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class BlueGreenConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenConnectionPlugin.class.getName());
  private static final String TELEMETRY_SWITCHOVER = "Blue/Green switchover";

  private static final String BG_PLUGIN_REROUTED_CONNECTION_CALL = "bgReroutedConnectCall";

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

  protected long holdStartTime = 0;
  protected long holdEndTime;
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

    // Pass-through this call since it's originated by this plugin and doesn't require any additional handling.
    if (props.containsKey(BG_PLUGIN_REROUTED_CONNECTION_CALL)) {
      return regularOpenConnection(connectFunc, isInitialConnection);
    }

    this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

    if (this.bgStatus == null) {
      return regularOpenConnection(connectFunc, isInitialConnection);
    }

    if (isInitialConnection) {
      this.isIamInUse = this.pluginService.isPluginInUse(IamAuthConnectionPlugin.class);
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {
      // Don't open any connections while BG is in active phase
      this.holdOn("connect");
    }

    boolean connectingToBlueOrGreen = this.rdsUtils.isNoPrefixInstance(hostSpec.getHost())
        || this.rdsUtils.isGreenInstance(hostSpec.getHost());

    if (!connectingToBlueOrGreen) {
      return regularOpenConnection(connectFunc, isInitialConnection);
    }

    // DNS changes are in progress. Node aliases are not reliable. It's required to use IP address.
    boolean needUseIpAddress = this.bgStatus.getCurrentPhase() == BlueGreenPhases.POST_SWITCH_OVER;
    LOGGER.info("needUseIpAddress: " + needUseIpAddress);

    // Override regular "connect" flow in some special cases:
    // - connecting to blue or green node, not to "-old1" node
    // - need to use IP address rather than blue or green DNS alia (since DNS alias is unreliable)
    // - need to alter connection properties for IAM authentication (since green node, now new blue node, may already change its name)
    if (needUseIpAddress) {

      String greenHost = this.bgStatus.getCorrespondingNodes().get(hostSpec.getHost());
      String greenHostIpAddress = null;

      if (greenHost != null) {
        greenHostIpAddress = this.bgStatus.getHostIpAddresses().get(greenHost);
      }

      if (greenHost == null || greenHostIpAddress == null) {
        // Collected blue-to-green node mapping may not yet, or no longer, exist.
        // DNS maybe not yet being updated, or is already updated/propagated.
        // Go with regular "connect" flow since rerouting isn't possible.
        LOGGER.info("regular flow");
        return regularOpenConnection(connectFunc, isInitialConnection);
      }
      LOGGER.info("greenHost: " + greenHost);
      LOGGER.info("greenHostIpAddress: " + greenHostIpAddress);

      HostSpec reroutedHostSpec = needUseIpAddress
          ? this.pluginService.getHostSpecBuilder().copyFrom(hostSpec)
              .host(greenHostIpAddress)
              .hostId(hostSpec.getHostId())
              .availability(HostAvailability.AVAILABLE)
              .build()
          : hostSpec;

      final Properties copy = PropertyUtils.copyProperties(props);
      copy.setProperty(BG_PLUGIN_REROUTED_CONNECTION_CALL, "true");
      //copy.setProperty(IamAuthConnectionPlugin.IAM_EXPIRATION.name, "0");

      if (this.isIamInUse) {
        LOGGER.info("Reroute (IAM authentication) " + hostSpec.getHost() + " to " + reroutedHostSpec.getHost());
      } else {
        LOGGER.info("Reroute " + hostSpec.getHost() + " to " + reroutedHostSpec.getHost());
      }

      boolean useBlueNodeName = this.bgStatus.getGreenNodeChangedName();

      // This loop can do up to 2 rounds.
      while (true) {
        if (this.isIamInUse) {
          LOGGER.info("useBlueNodeName: " + useBlueNodeName);
          copy.setProperty(IamAuthConnectionPlugin.IAM_HOST.name,
              useBlueNodeName ? hostSpec.getHost() : greenHost);
          LOGGER.info("iamHost: " + IamAuthConnectionPlugin.IAM_HOST.getString(copy));
        }

        try {
          Connection conn = this.pluginService.connect(reroutedHostSpec, copy);

          // We've just successfully connected with IAM token with blue node
          // and we need to notify about it.
          if (useBlueNodeName && !this.bgStatus.getGreenNodeChangedName()) {
            BlueGreenStatusProvider tmpProvider = provider;
            if (tmpProvider != null) {
              tmpProvider.notifyGreenNodeChangedName();
            }
          }

          return conn;

        } catch (SQLException sqlException) {
          if (!this.pluginService.isLoginException(sqlException)) {
            throw sqlException;
          }

          if (useBlueNodeName) {
            LOGGER.info("Green (new-blue) node doesn't accept neither green nor blue IAM token.");
            throw sqlException;
          } else {
            // It seems that green node has already changed its host name to blue, and now
            // it requires an IAM token based on blue node name.
            useBlueNodeName = true;
            LOGGER.info("Reroute failed since green node " + reroutedHostSpec.getHost()
                + " has already changed its name to blue. Trying again with an adjusted IAM token.");
          }
        }
      }
    }

    return regularOpenConnection(connectFunc, isInitialConnection);
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

    if (!CLOSING_METHOD_NAMES.contains(methodName)) {
      this.resetHoldTime();
      this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
      try {
        this.rejectOrHoldExecute(methodName);
      } catch (SQLException ex) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    return jdbcMethodFunc.call();
  }

  protected void resetHoldTime() {
    this.holdStartTime = System.nanoTime();
    this.holdEndTime = this.holdStartTime;
  }

  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void rejectOrHoldExecute(final String methodName)
      throws SQLException {

    if (this.bgStatus == null) {
      return;
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

      if (this.rdsUtils.isNoPrefixInstance(this.pluginService.getCurrentHostSpec().getHost())) {

        if (this.pluginService.getCurrentConnection() != null
            && !this.pluginService.getCurrentConnection().isClosed()) {

          this.pluginService.getCurrentConnection().close();
        }
        throw new SQLException(
            "Connection to blue node has been closed since Blue/Green switchover is in progress.", "08001");

      } else if (this.rdsUtils.isGreenInstance(this.pluginService.getCurrentHostSpec().getHost())) {
        this.holdOn(methodName);
        // After holding this call, the current BG status is expected to be SWITCH_OVER_COMPLETED
      }
    }
  }

  protected void holdOn(final String methodName) throws SQLTimeoutException {

    LOGGER.finest(String.format(
        "Blue/Green Deployment switchover is in progress. Hold '%s' call until switchover is completed.", methodName));

    this.resetHoldTime();

    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_SWITCHOVER,
        TelemetryTraceLevel.NESTED);

    try {
      long endTime = this.getNanoTime() + TimeUnit.MILLISECONDS.toNanos(BG_CONNECT_TIMEOUT.getLong(props));

      while (this.getNanoTime() <= endTime
          && this.bgStatus != null
          && this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          this.holdEndTime = this.getNanoTime();
          throw new RuntimeException(e);
        }
        this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
        this.holdEndTime = this.getNanoTime();
      }

      this.holdEndTime = this.getNanoTime();

      if (this.bgStatus != null && this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {
        throw new SQLTimeoutException(
            String.format(
                "Blue/Green Deployment switchover is still in progress after %d ms. Try '%s' again later.",
                BG_CONNECT_TIMEOUT.getLong(props), methodName));
      }
      LOGGER.finest(String.format(
          "Blue/Green Deployment switchover is completed. Continue with '%s' call. The call was held for %d ms.",
          methodName,
          TimeUnit.NANOSECONDS.toMillis(this.getHoldTimeNano())));

    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
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

  // For testing purposes
  public long getHoldTimeNano() {
    return this.holdEndTime - this.holdStartTime;
  }
}
