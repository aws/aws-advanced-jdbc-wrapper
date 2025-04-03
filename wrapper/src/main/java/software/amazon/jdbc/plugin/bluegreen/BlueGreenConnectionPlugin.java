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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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

  public static final AwsWrapperProperty BGD_ID = new AwsWrapperProperty(
      "bgdId", "1",
      "Blue/Green Deployment identifier that helps the driver to distinguish different deployments.");

  protected static Map<String, BlueGreenStatusProvider> provider = new ConcurrentHashMap<>();

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
  protected BlueGreenProviderSupplier providerSupplier;

  protected final TelemetryFactory telemetryFactory;
  protected final RdsUtils rdsUtils = new RdsUtils();

  protected BlueGreenStatus bgStatus = null;
  protected String bgdId;

  protected boolean isIamInUse = false;

  protected final AtomicLong startTimeNano = new AtomicLong(0);
  protected final AtomicLong endTimeNano = new AtomicLong(0);

  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {
    this(pluginService, props, BlueGreenStatusProvider::new);
  }

  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull BlueGreenProviderSupplier providerSupplier) {

    this.pluginService = pluginService;
    this.props = props;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.providerSupplier = providerSupplier;
    this.bgdId = BGD_ID.getString(this.props).trim().toLowerCase();

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

    this.resetHoldTimeNano();

    try {

      this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);

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

      if (routing != null) {
        this.startTimeNano.set(this.getNanoTime());
      }

      while (routing != null && conn == null) {
        conn = routing.apply(
            this,
            hostSpec,
            props,
            isInitialConnection,
            connectFunc,
            this.pluginService);

        this.endTimeNano.set(this.getNanoTime());

        if (conn == null) {

          this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);

          routing = this.bgStatus.getConnectRouting().stream()
              .filter(r -> r.isMatch(hostSpec, hostRole))
              .findFirst()
              .orElse(null);
        }
      }

      if (conn == null) {
        this.endTimeNano.set(this.getNanoTime());
        conn = connectFunc.call();
      }

      if (isInitialConnection) {
        // Provider should be initialized after connection is open and a dialect is properly identified.
        this.initProvider();
      }

      return conn;

    } finally {
      if (this.startTimeNano.get() > 0) {
        this.endTimeNano.compareAndSet(0, this.getNanoTime());
      }
    }
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

    this.resetHoldTimeNano();

    try {
      this.initProvider();

      if (CLOSING_METHOD_NAMES.contains(methodName)) {
        return jdbcMethodFunc.call();
      }

      this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);

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

      if (routing != null) {
        this.startTimeNano.set(this.getNanoTime());
      }

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

        this.endTimeNano.set(this.getNanoTime());

        if (!result.isPresent()) {

          this.bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, this.bgdId);

          routing = this.bgStatus.getExecuteRouting().stream()
              .filter(r -> r.isMatch(currentHostSpec, hostRole))
              .findFirst()
              .orElse(null);
        }
      }

      this.endTimeNano.set(this.getNanoTime());

      if (result.isPresent()) {
        return result.get();
      }

      return jdbcMethodFunc.call();

    } finally {
      if (this.startTimeNano.get() > 0) {
        this.endTimeNano.compareAndSet(0, this.getNanoTime());
      }
    }
  }

  protected void initProvider() {
    provider.computeIfAbsent(this.bgdId,
        (key) -> this.providerSupplier.create(this.pluginService, this.props, this.bgdId));
  }

  // For testing purposes
  protected long getNanoTime() {
    return System.nanoTime();
  }

  public long getHoldTimeNano() {
    return this.startTimeNano.get() == 0
        ? 0
        : (this.endTimeNano.get() == 0
            ? (this.getNanoTime() - this.startTimeNano.get())
            : (this.endTimeNano.get() - this.startTimeNano.get()));
  }

  public void resetHoldTimeNano() {
    this.startTimeNano.set(0);
    this.endTimeNano.set(0);
  }
}
