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

import java.net.UnknownHostException;
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
      return connectFunc.call();
    }

    this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
    final boolean needReroute = this.rejectOrHoldConnect(hostSpec);

    if (needReroute) {

      final Properties copy = PropertyUtils.copyProperties(props);
      copy.setProperty(BG_PLUGIN_REROUTED_CONNECTION_CALL, "true");

      HostSpec routedHostSpec = this.routeToGreenEndpoint(hostSpec);
      if (routedHostSpec != null) {
        LOGGER.finest("Reroute " + hostSpec.getHost() + " to " + routedHostSpec.getHost());
        try {
          return this.pluginService.connect(routedHostSpec, copy);
        } catch (SQLException sqlException) {
          if (this.isCausedBy(sqlException, UnknownHostException.class, false)) {
            LOGGER.finest("Green node DNS doesn't exist: " + routedHostSpec.getHost());
            // let's continue and try IP address instead of green node endpoint...
          } else {
            throw sqlException;
          }
        }
      }

      // ...try green node IP address
      routedHostSpec = this.routeToGreenIpAddress(hostSpec);
      if (routedHostSpec != null) {
        LOGGER.finest("Reroute " + hostSpec.getHost() + " to " + routedHostSpec.getHost());
        return this.pluginService.connect(routedHostSpec, copy);
      }
    }

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

  protected boolean isCausedBy(
      final Throwable throwable,
      final Class<? extends Throwable> expectedCause,
      final boolean strictMatch) {

    if (throwable == null || expectedCause == null) {
      return false;
    }

    // We need to track processed exceptions to prevent infinitive/circular loop.
    final Set<Throwable> trackedExceptions = new HashSet<>();
    Throwable current = throwable;

    while (current != null
        && !trackedExceptions.contains(current)
        && !this.isMatch(current, expectedCause, strictMatch)) {
      trackedExceptions.add(current);
      current = current.getCause();
    }

    return this.isMatch(current, expectedCause, strictMatch);
  }

  protected boolean isMatch(
      final Throwable throwable,
      final Class<? extends Throwable> expectedThrowable,
      final boolean strictMatch) {

    if (throwable == null || expectedThrowable == null) {
      return false;
    }
    if (strictMatch) {
      return throwable.getClass() == expectedThrowable;
    }
    return throwable.getClass().isAssignableFrom(expectedThrowable);
  }

  protected long getNanoTime() {
    return System.nanoTime();
  }

  // Return: true, if it needs to reroute connect call to another endpoint or IP address
  protected boolean rejectOrHoldConnect(final HostSpec connectHostSpec)
      throws SQLTimeoutException {

    if(this.bgStatus == null) {
      return false;
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

      if (this.isBeforeSourceHostSpec(connectHostSpec) || this.isBeforeTargetHostSpec(connectHostSpec)) {
        // Hold this call till BG switchover is completed.
        this.holdOn("connect");

        // After holding this connect call, the current BG status is expected to be SWITCH_OVER_COMPLETED
        return true;
      }
    } else if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCH_OVER_COMPLETED) {

      if (this.isAfterSourceHostSpec(connectHostSpec) || this.isAfterDeprecatedHostSpec(connectHostSpec)) {
        return true;
      }
    }

    return false;
  }

  protected HostSpec routeToGreenEndpoint(final HostSpec connectHostSpec) {

    if(this.bgStatus == null) {
      return null;
    }

    if (this.rdsUtils.isIPv4(connectHostSpec.getHost()) || this.rdsUtils.isIPv6(connectHostSpec.getHost())) {
      return null;
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCH_OVER_COMPLETED) {

      if (this.isAfterSourceHostSpec(connectHostSpec)) {
        // Need to find a corresponding green node and return it

        String correspondingGreenHost = this.bgStatus.getCorrespondingNodes().get(connectHostSpec.getHost());
        if (correspondingGreenHost == null) {
          return null;
        }

        return this.pluginService.getHostSpecBuilder().copyFrom(connectHostSpec)
            .host(correspondingGreenHost)
            .hostId(this.getHostId(correspondingGreenHost))
            .availability(HostAvailability.AVAILABLE)
            .build();
      }
      if (this.isAfterDeprecatedHostSpec(connectHostSpec)) {
        // currentHostSpec is already a green node endpoint
        return null;
      }
    }

    return null;
  }

  protected HostSpec routeToGreenIpAddress(final HostSpec connectHostSpec) {

    if(this.bgStatus == null) {
      return null;
    }

    if (this.rdsUtils.isIPv4(connectHostSpec.getHost()) || this.rdsUtils.isIPv6(connectHostSpec.getHost())) {
      return null;
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCH_OVER_COMPLETED) {

      String replacementId = this.bgStatus.getHostIpAddresses().get(connectHostSpec.getHost());
      if (replacementId == null) {
        return null;
      }

      return this.pluginService.getHostSpecBuilder().copyFrom(connectHostSpec)
          .host(replacementId)
          .hostId(connectHostSpec.getHostId())
          .availability(HostAvailability.AVAILABLE)
          .build();
    }

    return null;
  }

  protected String getHostId(final String endpoint) {
    if (StringUtils.isNullOrEmpty(endpoint)) {
      return null;
    }
    final String[] parts = endpoint.split("\\.");
    return parts != null && parts.length > 0 ? parts[0] : null;
  }

  protected void rejectOrHoldExecute(final String methodName)
      throws SQLException {

    if (this.bgStatus == null) {
      return;
    }

    if (this.bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

      if (this.isBeforeSourceHostSpec(this.pluginService.getCurrentHostSpec())) {

        if (this.pluginService.getCurrentConnection() != null
            && !this.pluginService.getCurrentConnection().isClosed()) {

          this.pluginService.getCurrentConnection().close();
        }
        throw new SQLException(
            "Connection to blue node has been closed since Blue/Green switchover is in progress.", "08001");

      } else if (this.isBeforeTargetHostSpec(this.pluginService.getCurrentHostSpec())) {
        this.holdOn(methodName);
        // After holding this call, the current BG status is expected to be SWITCH_OVER_COMPLETED
      }
    }
  }

  protected boolean isBeforeSourceHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isNoPrefixInstance(host);
  }

  protected boolean isBeforeTargetHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isGreenInstance(host);
  }

  protected boolean isAfterSourceHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isNoPrefixInstance(host);
  }

  protected boolean isAfterDeprecatedHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isGreenInstance(host);
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
