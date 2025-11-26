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
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPluginFactory;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.cache.DataLocalCacheConnectionPluginFactory;
import software.amazon.jdbc.plugin.cache.DataRemoteCachePluginFactory;
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
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class ConnectionPluginChainBuilder {

  private static final Logger LOGGER = Logger.getLogger(ConnectionPluginChainBuilder.class.getName());

  private static final int WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1;

  protected static final Map<String, ConnectionPluginFactory> pluginFactoriesByCode =
      new HashMap<String, ConnectionPluginFactory>() {
        {
          put("executionTime", new ExecutionTimeConnectionPluginFactory());
          put("logQuery", new LogQueryConnectionPluginFactory());
          put("dataCache", new DataLocalCacheConnectionPluginFactory());
          put("dataRemoteCache", new DataRemoteCachePluginFactory());
          put("customEndpoint", new CustomEndpointPluginFactory());
          put("efm", new HostMonitoringConnectionPluginFactory());
          put("efm2", new software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPluginFactory());
          put("failover", new FailoverConnectionPluginFactory());
          put("failover2", new software.amazon.jdbc.plugin.failover2.FailoverConnectionPluginFactory());
          put("iam", new IamAuthConnectionPluginFactory());
          put("awsSecretsManager", new AwsSecretsManagerConnectionPluginFactory());
          put("federatedAuth", new FederatedAuthPluginFactory());
          put("okta", new OktaAuthPluginFactory());
          put("auroraStaleDns", new AuroraStaleDnsPluginFactory());
          put("readWriteSplitting", new ReadWriteSplittingPluginFactory());
          put("auroraConnectionTracker", new AuroraConnectionTrackerPluginFactory());
          put("driverMetaData", new DriverMetaDataConnectionPluginFactory());
          put("connectTime", new ConnectTimeConnectionPluginFactory());
          put("dev", new DeveloperConnectionPluginFactory());
          put("fastestResponseStrategy", new FastestResponseStrategyPluginFactory());
          put("initialConnection", new AuroraInitialConnectionStrategyPluginFactory());
          put("limitless", new LimitlessConnectionPluginFactory());
          put("bg", new BlueGreenConnectionPluginFactory());
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
          put(DataLocalCacheConnectionPluginFactory.class, 200);
          put(DataRemoteCachePluginFactory.class, 250);
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

  protected static final ConcurrentMap<Class<? extends ConnectionPluginFactory>, ConnectionPluginFactory>
      pluginFactoriesByClass = new ConcurrentHashMap<>();

  protected static final String DEFAULT_PLUGINS = "auroraConnectionTracker,failover2,efm2";

  /*
   Internal class used for plugin factory sorting. It holds a reference to a plugin
   factory and an assigned weight.
  */
  private static class PluginFactoryInfo {
    public ConnectionPluginFactory factory;
    public int weight;

    public PluginFactoryInfo(final ConnectionPluginFactory factory, final int weight) {
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
    List<ConnectionPluginFactory> pluginFactories;

    if (configurationProfile != null && configurationProfile.getPluginFactories() != null) {
      List<Class<? extends ConnectionPluginFactory>> pluginFactoryClasses = configurationProfile.getPluginFactories();
      pluginFactories = new ArrayList<>(pluginFactoryClasses.size());

      for (final Class<? extends ConnectionPluginFactory> factoryClazz : pluginFactoryClasses) {
        final AtomicReference<InstantiationException> lastException = new AtomicReference<>(null);
        final ConnectionPluginFactory factory = pluginFactoriesByClass.computeIfAbsent(factoryClazz, (key) -> {
          try {
            return WrapperUtils.createInstance(
                factoryClazz,
                ConnectionPluginFactory.class,
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

        final List<ConnectionPluginFactory> tempPluginFactories = pluginFactories;
        LOGGER.finest(() ->
            "Plugins order has been rearranged. The following order is in effect: "
                + tempPluginFactories.stream()
                  .map(x -> x.getClass().getSimpleName())
                  .collect(Collectors.joining(", ")));
      }

      // make a chain of connection plugins
      plugins = new ArrayList<>(pluginFactories.size() + 1);
      for (final ConnectionPluginFactory factory : pluginFactories) {
        if (factory instanceof ServicesContainerPluginFactory) {
          ServicesContainerPluginFactory servicesContainerPluginFactory = (ServicesContainerPluginFactory) factory;
          plugins.add(servicesContainerPluginFactory.getInstance(servicesContainer, props));
        } else {
          plugins.add(factory.getInstance(servicesContainer.getPluginService(), props));
        }
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

  protected List<ConnectionPluginFactory> sortPluginFactories(
      final List<ConnectionPluginFactory> unsortedPluginFactories) {

    final ArrayList<PluginFactoryInfo> weights = new ArrayList<>();
    int lastWeight = 0;
    for (ConnectionPluginFactory pluginFactory : unsortedPluginFactories) {
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
