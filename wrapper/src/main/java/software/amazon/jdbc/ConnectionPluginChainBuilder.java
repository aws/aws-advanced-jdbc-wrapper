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

package software.amazon.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPluginFactory;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPluginFactory;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.DataCacheConnectionPluginFactory;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.DriverMetaDataConnectionPluginFactory;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.LogQueryConnectionPluginFactory;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPluginFactory;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPluginFactory;
import software.amazon.jdbc.plugin.dev.DeveloperConnectionPluginFactory;
import software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPluginFactory;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPluginFactory;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPluginFactory;
import software.amazon.jdbc.plugin.limitless.LimitlessConnectionPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsPluginFactory;
import software.amazon.jdbc.plugin.strategy.fastestresponse.FastestResponseStrategyPluginFactory;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class ConnectionPluginChainBuilder {

  private static final Logger LOGGER = Logger.getLogger(ConnectionPluginChainBuilder.class.getName());

  private static final int WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1;

  protected static final Map<String, Class<? extends ConnectionPluginFactory>> pluginFactoriesByCode =
      new HashMap<String, Class<? extends ConnectionPluginFactory>>() {
        {
          put("executionTime", ExecutionTimeConnectionPluginFactory.class);
          put("logQuery", LogQueryConnectionPluginFactory.class);
          put("dataCache", DataCacheConnectionPluginFactory.class);
          put("customEndpoint", CustomEndpointPluginFactory.class);
          put("efm", HostMonitoringConnectionPluginFactory.class);
          put("efm2", software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPluginFactory.class);
          put("failover", FailoverConnectionPluginFactory.class);
          put("failover2", software.amazon.jdbc.plugin.failover2.FailoverConnectionPluginFactory.class);
          put("iam", IamAuthConnectionPluginFactory.class);
          put("awsSecretsManager", AwsSecretsManagerConnectionPluginFactory.class);
          put("federatedAuth", FederatedAuthPluginFactory.class);
          put("okta", OktaAuthPluginFactory.class);
          put("auroraStaleDns", AuroraStaleDnsPluginFactory.class);
          put("readWriteSplitting", ReadWriteSplittingPluginFactory.class);
          put("auroraConnectionTracker", AuroraConnectionTrackerPluginFactory.class);
          put("driverMetaData", DriverMetaDataConnectionPluginFactory.class);
          put("connectTime", ConnectTimeConnectionPluginFactory.class);
          put("dev", DeveloperConnectionPluginFactory.class);
          put("fastestResponseStrategy", FastestResponseStrategyPluginFactory.class);
          put("initialConnection", AuroraInitialConnectionStrategyPluginFactory.class);
          put("limitless", LimitlessConnectionPluginFactory.class);
          put("bg", BlueGreenConnectionPluginFactory.class);
        }
      };

  /**
   * The final list of plugins will be sorted by weight, starting from the lowest values up to
   * the highest values. The first plugin of the list will have the lowest weight, and the
   * last one will have the highest weight.
   */
  protected static final Map<Class<? extends ConnectionPluginFactory>, Integer> pluginWeightByPluginFactory =
      new HashMap<Class<? extends ConnectionPluginFactory>, Integer>() {
        {
          put(DriverMetaDataConnectionPluginFactory.class, 100);
          put(DataCacheConnectionPluginFactory.class, 200);
          put(CustomEndpointPluginFactory.class, 380);
          put(AuroraInitialConnectionStrategyPluginFactory.class, 390);
          put(AuroraConnectionTrackerPluginFactory.class, 400);
          put(AuroraStaleDnsPluginFactory.class, 500);
          put(BlueGreenConnectionPluginFactory.class, 550);
          put(ReadWriteSplittingPluginFactory.class, 600);
          put(FailoverConnectionPluginFactory.class, 700);
          put(software.amazon.jdbc.plugin.failover2.FailoverConnectionPluginFactory.class, 710);
          put(HostMonitoringConnectionPluginFactory.class, 800);
          put(software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPluginFactory.class, 810);
          put(FastestResponseStrategyPluginFactory.class, 900);
          put(LimitlessConnectionPluginFactory.class, 950);
          put(IamAuthConnectionPluginFactory.class, 1000);
          put(AwsSecretsManagerConnectionPluginFactory.class, 1100);
          put(FederatedAuthPluginFactory.class, 1200);
          put(LogQueryConnectionPluginFactory.class, 1300);
          put(ConnectTimeConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(ExecutionTimeConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(DeveloperConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
        }
      };

  protected static final String DEFAULT_PLUGINS = "auroraConnectionTracker,failover,efm2";

  /*
   Internal class used for plugin factory sorting. It holds a reference to a plugin
   factory and an assigned weight.
  */
  private static class PluginFactoryInfo {
    public Class<? extends ConnectionPluginFactory> factory;
    public int weight;

    public PluginFactoryInfo(final Class<? extends ConnectionPluginFactory> factory, final int weight) {
      this.factory = factory;
      this.weight = weight;
    }
  }

  public List<ConnectionPlugin> getPlugins(
      final PluginService pluginService,
      final ConnectionProvider defaultConnProvider,
      final ConnectionProvider effectiveConnProvider,
      final PluginManagerService pluginManagerService,
      final Properties props,
      @Nullable ConfigurationProfile configurationProfile)
      throws SQLException {

    List<ConnectionPlugin> plugins;
    List<Class<? extends ConnectionPluginFactory>> pluginFactories;

    if (configurationProfile != null && configurationProfile.getPluginFactories() != null) {
      pluginFactories = configurationProfile.getPluginFactories();
    } else {

      final List<String> pluginCodeList = getPluginCodes(props);
      pluginFactories = new ArrayList<>(pluginCodeList.size());

      for (final String pluginCode : pluginCodeList) {
        if (!pluginFactoriesByCode.containsKey(pluginCode)) {
          throw new SQLException(
              Messages.get(
                  "ConnectionPluginManager.unknownPluginCode",
                  new Object[] {pluginCode}));
        }
        pluginFactories.add(pluginFactoriesByCode.get(pluginCode));
      }
    }

    if (!pluginFactories.isEmpty()) {

      if (PropertyDefinition.AUTO_SORT_PLUGIN_ORDER.getBoolean(props)) {
        pluginFactories = this.sortPluginFactories(pluginFactories);

        final List<Class<? extends ConnectionPluginFactory>> tempPluginFactories = pluginFactories;
        LOGGER.finest(() ->
            "Plugins order has been rearranged. The following order is in effect: "
                + tempPluginFactories.stream()
                  .map(Class::getSimpleName)
                  .collect(Collectors.joining(", ")));
      }

      try {
        final ConnectionPluginFactory[] factories =
            WrapperUtils.loadClasses(
                    pluginFactories,
                    ConnectionPluginFactory.class,
                    "ConnectionPluginManager.unableToLoadPlugin")
                .toArray(new ConnectionPluginFactory[0]);

        // make a chain of connection plugins

        plugins = new ArrayList<>(factories.length + 1);

        for (final ConnectionPluginFactory factory : factories) {
          plugins.add(factory.getInstance(pluginService, props));
        }

      } catch (final InstantiationException instEx) {
        throw new SQLException(instEx.getMessage(), SqlState.UNKNOWN_STATE.getState(), instEx);
      }
    } else {
      plugins = new ArrayList<>(1); // one spot for default connection plugin
    }

    // add default connection plugin to the tail
    final ConnectionPlugin defaultPlugin = new DefaultConnectionPlugin(
        pluginService,
        defaultConnProvider,
        effectiveConnProvider,
        pluginManagerService);

    plugins.add(defaultPlugin);

    return plugins;
  }

  public static List<String> getPluginCodes(final Properties props) {
    String pluginCodes = PropertyDefinition.PLUGINS.getString(props);
    if (pluginCodes == null) {
      pluginCodes = DEFAULT_PLUGINS;
    }
    return StringUtils.split(pluginCodes, ",", true);
  }

  protected List<Class<? extends ConnectionPluginFactory>> sortPluginFactories(
      final List<Class<? extends ConnectionPluginFactory>> unsortedPluginFactories) {

    final ArrayList<PluginFactoryInfo> weights = new ArrayList<>();
    int lastWeight = 0;
    for (Class<? extends ConnectionPluginFactory> pluginFactory : unsortedPluginFactories) {
      Integer pluginFactoryWeight = pluginWeightByPluginFactory.get(pluginFactory);

      if (pluginFactoryWeight == null || pluginFactoryWeight == WEIGHT_RELATIVE_TO_PRIOR_PLUGIN) {

        // This plugin factory is unknown, or it has relative (to prior plugin factory) weight.
        lastWeight++;
        weights.add(new PluginFactoryInfo(pluginFactory, lastWeight));

      } else {
        // Otherwise, use the wight assigned to this plugin factory
        weights.add(new PluginFactoryInfo(pluginFactory, pluginFactoryWeight));

        // Remember this weight for next plugin factories that might have
        // relative (to this plugin factory) weight.
        lastWeight = pluginFactoryWeight;
      }
    }

    return weights.stream()
        .sorted(Comparator.comparingInt(o -> o.weight))
        .map((info) -> info.factory)
        .collect(Collectors.toList());
  }
}
