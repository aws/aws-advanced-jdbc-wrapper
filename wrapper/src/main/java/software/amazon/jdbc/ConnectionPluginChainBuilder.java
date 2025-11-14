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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPluginFactory;
import software.amazon.jdbc.plugin.AwsSecretsManagerPluginFactory;
import software.amazon.jdbc.plugin.ConnectTimePluginFactory;
import software.amazon.jdbc.plugin.DataCachePluginFactory;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.DriverMetaDataPluginFactory;
import software.amazon.jdbc.plugin.ExecutionTimePluginFactory;
import software.amazon.jdbc.plugin.LogQueryPluginFactory;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenPluginFactory;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPluginFactory;
import software.amazon.jdbc.plugin.dev.DeveloperPluginFactory;
import software.amazon.jdbc.plugin.efm.HostMonitoringPluginFactory;
import software.amazon.jdbc.plugin.failover.FailoverPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPluginFactory;
import software.amazon.jdbc.plugin.iam.IamAuthPluginFactory;
import software.amazon.jdbc.plugin.limitless.LimitlessPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsPluginFactory;
import software.amazon.jdbc.plugin.strategy.fastestresponse.FastestResponseStrategyPluginFactory;
import software.amazon.jdbc.profile.ConfigurationProfile;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class ConnectionPluginChainBuilder {

  private static final Logger LOGGER = Logger.getLogger(ConnectionPluginChainBuilder.class.getName());

  private static final int WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1;

  protected static final Map<String, PluginFactory> pluginFactoriesByCode =
      new HashMap<String, PluginFactory>() {
        {
          put("executionTime", new ExecutionTimePluginFactory());
          put("logQuery", new LogQueryPluginFactory());
          put("dataCache", new DataCachePluginFactory());
          put("customEndpoint", new CustomEndpointPluginFactory());
          put("efm", new HostMonitoringPluginFactory());
          put("efm2", new HostMonitoringPluginFactory());
          put("failover", new FailoverPluginFactory());
          put("failover2", new FailoverPluginFactory());
          put("iam", new IamAuthPluginFactory());
          put("awsSecretsManager", new AwsSecretsManagerPluginFactory());
          put("federatedAuth", new FederatedAuthPluginFactory());
          put("okta", new OktaAuthPluginFactory());
          put("auroraStaleDns", new AuroraStaleDnsPluginFactory());
          put("readWriteSplitting", new ReadWriteSplittingPluginFactory());
          put("auroraConnectionTracker", new AuroraConnectionTrackerPluginFactory());
          put("driverMetaData", new DriverMetaDataPluginFactory());
          put("connectTime", new ConnectTimePluginFactory());
          put("dev", new DeveloperPluginFactory());
          put("fastestResponseStrategy", new FastestResponseStrategyPluginFactory());
          put("initialConnection", new AuroraInitialConnectionStrategyPluginFactory());
          put("limitless", new LimitlessPluginFactory());
          put("bg", new BlueGreenPluginFactory());
        }
      };

  /**
   * The final list of plugins will be sorted by weight, starting from the lowest values up to
   * the highest values. The first plugin of the list will have the lowest weight, and the
   * last one will have the highest weight.
   */
  protected static final Map<Class<? extends PluginFactory>, Integer> pluginWeightByPluginFactory =
      new HashMap<Class<? extends PluginFactory>, Integer>() {
        {
          put(DriverMetaDataPluginFactory.class, 100);
          put(DataCachePluginFactory.class, 200);
          put(CustomEndpointPluginFactory.class, 380);
          put(AuroraInitialConnectionStrategyPluginFactory.class, 390);
          put(AuroraConnectionTrackerPluginFactory.class, 400);
          put(AuroraStaleDnsPluginFactory.class, 500);
          put(BlueGreenPluginFactory.class, 550);
          put(ReadWriteSplittingPluginFactory.class, 600);
          put(FailoverPluginFactory.class, 700);
          put(software.amazon.jdbc.plugin.failover2.FailoverPluginFactory.class, 710);
          put(HostMonitoringPluginFactory.class, 800);
          put(software.amazon.jdbc.plugin.efm2.HostMonitoringPluginFactory.class, 810);
          put(FastestResponseStrategyPluginFactory.class, 900);
          put(LimitlessPluginFactory.class, 950);
          put(IamAuthPluginFactory.class, 1000);
          put(AwsSecretsManagerPluginFactory.class, 1100);
          put(FederatedAuthPluginFactory.class, 1200);
          put(LogQueryPluginFactory.class, 1300);
          put(ConnectTimePluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(ExecutionTimePluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(DeveloperPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
        }
      };

  protected static final ConcurrentMap<Class<? extends PluginFactory>, PluginFactory>
      pluginFactoriesByClass = new ConcurrentHashMap<>();

  protected static final String DEFAULT_PLUGINS = "initialConnection,auroraConnectionTracker,failover2,efm2";

  /*
   Internal class used for plugin factory sorting. It holds a reference to a plugin
   factory and an assigned weight.
  */
  private static class PluginFactoryInfo {
    public PluginFactory factory;
    public int weight;

    public PluginFactoryInfo(final PluginFactory factory, final int weight) {
      this.factory = factory;
      this.weight = weight;
    }
  }

  public List<ConnectionPlugin> getPlugins(
      final FullServicesContainer servicesContainer,
      final ConnectionProvider defaultConnProvider,
      final ConnectionProvider effectiveConnProvider,
      final Properties props,
      @Nullable ConfigurationProfile configurationProfile) throws SQLException {

    List<ConnectionPlugin> plugins;
    List<PluginFactory> pluginFactories;

    if (configurationProfile != null && configurationProfile.getPluginFactories() != null) {
      List<Class<? extends PluginFactory>> pluginFactoryClasses = configurationProfile.getPluginFactories();
      pluginFactories = new ArrayList<>(pluginFactoryClasses.size());

      for (final Class<? extends PluginFactory> factoryClazz : pluginFactoryClasses) {
        final AtomicReference<InstantiationException> lastException = new AtomicReference<>(null);
        final PluginFactory factory = pluginFactoriesByClass.computeIfAbsent(factoryClazz, (key) -> {
          try {
            return WrapperUtils.createInstance(
                factoryClazz,
                PluginFactory.class,
                null,
                (Object) null);

          } catch (InstantiationException ex) {
            lastException.set(ex);
          }
          return null;
        });

        if (lastException.get() != null) {
          throw new SQLException(
              Messages.get(
                  "ConnectionPluginManager.unableToLoadPlugin",
                  new Object[] {factoryClazz.getName()}),
              SqlState.UNKNOWN_STATE.getState(),
              lastException.get());
        }

        if (factory != null) {
          pluginFactories.add(factory);
        }
      }
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

        final List<PluginFactory> tempPluginFactories = pluginFactories;
        LOGGER.finest(() ->
            "Plugins order has been rearranged. The following order is in effect: "
                + tempPluginFactories.stream()
                  .map(x -> x.getClass().getSimpleName())
                  .collect(Collectors.joining(", ")));
      }

      // make a chain of connection plugins
      plugins = new ArrayList<>(pluginFactories.size() + 1);
      for (final PluginFactory factory : pluginFactories) {
        plugins.add(factory.getInstance(servicesContainer, props));
      }
    } else {
      plugins = new ArrayList<>(1); // one spot for default connection plugin
    }

    // add default connection plugin to the tail
    final ConnectionPlugin defaultPlugin = new DefaultConnectionPlugin(
        servicesContainer.getPluginService(),
        defaultConnProvider,
        effectiveConnProvider,
        servicesContainer.getPluginManagerService());

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

  protected List<PluginFactory> sortPluginFactories(
      final List<PluginFactory> unsortedPluginFactories) {

    final ArrayList<PluginFactoryInfo> weights = new ArrayList<>();
    int lastWeight = 0;
    for (PluginFactory pluginFactory : unsortedPluginFactories) {
      Integer pluginFactoryWeight = pluginWeightByPluginFactory.get(pluginFactory.getClass());

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
