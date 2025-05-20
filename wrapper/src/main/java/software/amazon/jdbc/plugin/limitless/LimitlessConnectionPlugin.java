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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.AuroraLimitlessDialect;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.Messages;

public class LimitlessConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(LimitlessConnectionPlugin.class.getName());

  public static final AwsWrapperProperty WAIT_FOR_ROUTER_INFO = new AwsWrapperProperty(
      "limitlessWaitForTransactionRouterInfo",
      "true",
      "If the cache of transaction router info is empty and a new connection is made, this property toggles whether "
          + "the plugin will wait and synchronously fetch transaction router info before selecting a transaction "
          + "router to connect to, or to fall back to using the provided DB Shard Group endpoint URL.");
  public static final AwsWrapperProperty GET_ROUTER_RETRY_INTERVAL_MILLIS = new AwsWrapperProperty(
      "limitlessGetTransactionRouterInfoRetryIntervalMs",
      "300",
      "Interval in millis between retries fetching Limitless Transaction Router information.");
  public static final AwsWrapperProperty GET_ROUTER_MAX_RETRIES = new AwsWrapperProperty(
      "limitlessGetTransactionRouterInfoMaxRetries",
      "5",
      "Max number of connection retries fetching Limitless Transaction Router information.");
  public static final AwsWrapperProperty INTERVAL_MILLIS = new AwsWrapperProperty(
      "limitlessTransactionRouterMonitorIntervalMs",
      "7500",
      "Interval in millis between polling for Limitless Transaction Routers to the database.");
  public static final AwsWrapperProperty MAX_RETRIES = new AwsWrapperProperty(
      "limitlessConnectMaxRetries",
      "5",
      "Max number of connection retries the Limitless Connection Plugin will attempt.");

  protected final PluginService pluginService;
  protected final Properties properties;
  private final Supplier<LimitlessRouterService> limitlessRouterServiceSupplier;
  private LimitlessRouterService limitlessRouterService;
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(LimitlessConnectionPlugin.class);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public LimitlessConnectionPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    this(pluginService,
        properties,
        () -> new LimitlessRouterServiceImpl(pluginService));
  }

  public LimitlessConnectionPlugin(
      final PluginService pluginService,
      final @NonNull Properties properties,
      final @NonNull Supplier<LimitlessRouterService> limitlessRouterServiceSupplier) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.limitlessRouterServiceSupplier = limitlessRouterServiceSupplier;
  }


  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    Connection conn = null;

    final Dialect dialect = this.pluginService.getDialect();
    if (!(dialect instanceof AuroraLimitlessDialect)) {
      conn = connectFunc.call();
      final Dialect refreshedDialect = this.pluginService.getDialect();
      if (!(refreshedDialect instanceof AuroraLimitlessDialect)) {
        throw new UnsupportedOperationException(Messages.get(
            "LimitlessConnectionPlugin.unsupportedDialectOrDatabase",
            new Object[] {refreshedDialect}));
      }
    }

    initLimitlessRouterMonitorService();
    if (isInitialConnection) {
      this.limitlessRouterService.startMonitoring(
          hostSpec, properties, INTERVAL_MILLIS.getInteger(properties));
    }

    final LimitlessConnectionContext context = new LimitlessConnectionContext(
        hostSpec,
        props,
        conn,
        connectFunc,
        null,
        this);
    this.limitlessRouterService.establishConnection(context);

    if (context.getConnection() != null) {
      return context.getConnection();
    }
    throw new SQLException(Messages.get(
        "LimitlessConnectionPlugin.failedToConnectToHost",
        new Object[] {hostSpec.getHost()}));
  }

  private void initLimitlessRouterMonitorService() {
    if (limitlessRouterService == null) {
      this.limitlessRouterService = this.limitlessRouterServiceSupplier.get();
    }
  }
}
