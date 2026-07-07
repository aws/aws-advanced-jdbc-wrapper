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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPluginFactory;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin2Factory;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPluginFactory;
import software.amazon.jdbc.plugin.ConnectTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.DefaultConnectionPlugin;
import software.amazon.jdbc.plugin.DriverMetaDataConnectionPluginFactory;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPluginFactory;
import software.amazon.jdbc.plugin.LogQueryConnectionPluginFactory;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenConnectionPluginFactory;
import software.amazon.jdbc.plugin.cache.DataLocalCacheConnectionPluginFactory;
import software.amazon.jdbc.plugin.cache.RemoteQueryCachePluginFactory;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointPluginFactory;
import software.amazon.jdbc.plugin.dev.DeveloperConnectionPluginFactory;
import software.amazon.jdbc.plugin.efm.v1.HostMonitoringConnectionPluginV1Factory;
import software.amazon.jdbc.plugin.efm.v2.HostMonitoringConnectionPluginV2Factory;
import software.amazon.jdbc.plugin.encryption.KmsEncryptionConnectionPluginFactory;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.FederatedAuthPluginFactory;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPluginFactory;
import software.amazon.jdbc.plugin.gdbfailover.GlobalDbFailoverConnectionPluginFactory;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPluginFactory;
import software.amazon.jdbc.plugin.limitless.LimitlessConnectionPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.AutoReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.AutoSimpleReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.GdbAutoReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.GdbAutoSimpleReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.GdbReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSimpleReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.SimpleReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.sqlparser.SqlParserConnectionPluginFactory;
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
          put("remoteQueryCache", new RemoteQueryCachePluginFactory());
          put("customEndpoint", new CustomEndpointPluginFactory());
          put("efm", new HostMonitoringConnectionPluginV1Factory());
          put("efm2", new HostMonitoringConnectionPluginV2Factory());
          put("failover", new FailoverConnectionPluginFactory());
          put("failover2", new software.amazon.jdbc.plugin.failover2.FailoverConnectionPluginFactory());
          put("gdbFailover", new GlobalDbFailoverConnectionPluginFactory());
          put("iam", new IamAuthConnectionPluginFactory());
          put("awsSecretsManager", new AwsSecretsManagerConnectionPluginFactory());
          put("awsSecretsManager2", new AwsSecretsManagerConnectionPlugin2Factory());
          put("federatedAuth", new FederatedAuthPluginFactory());
          put("okta", new OktaAuthPluginFactory());
          put("auroraStaleDns", new AuroraStaleDnsPluginFactory());
          put("readWriteSplitting", new ReadWriteSplittingPluginFactory());
          put("autoReadWriteSplitting", new AutoReadWriteSplittingPluginFactory());
          put("srw", new SimpleReadWriteSplittingPluginFactory());
          put("gdbReadWriteSplitting", new GdbReadWriteSplittingPluginFactory());
          put("gdbAutoReadWriteSplitting", new GdbAutoReadWriteSplittingPluginFactory());
          put("autoSimpleReadWriteSplitting", new AutoSimpleReadWriteSplittingPluginFactory());
          put("gdbSimpleReadWriteSplitting", new GdbSimpleReadWriteSplittingPluginFactory());
          put("gdbAutoSimpleReadWriteSplitting", new GdbAutoSimpleReadWriteSplittingPluginFactory());
          put("auroraConnectionTracker", new AuroraConnectionTrackerPluginFactory());
          put("driverMetaData", new DriverMetaDataConnectionPluginFactory());
          put("connectTime", new ConnectTimeConnectionPluginFactory());
          put("dev", new DeveloperConnectionPluginFactory());
          put("fastestResponseStrategy", new FastestResponseStrategyPluginFactory());
          put("initialConnection", new AuroraInitialConnectionStrategyPluginFactory());
          put("limitless", new LimitlessConnectionPluginFactory());
          put("bg", new BlueGreenConnectionPluginFactory());
          put("kmsEncryption", new KmsEncryptionConnectionPluginFactory());
          put("sqlParser", new SqlParserConnectionPluginFactory());
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
          put(BlueGreenConnectionPluginFactory.class, 200);
          put(DataLocalCacheConnectionPluginFactory.class, 300);
          put(RemoteQueryCachePluginFactory.class, 350);
          put(CustomEndpointPluginFactory.class, 400);
          put(AuroraInitialConnectionStrategyPluginFactory.class, 500);
          put(AuroraConnectionTrackerPluginFactory.class, 600);
          put(AuroraStaleDnsPluginFactory.class, 700);
          put(FailoverConnectionPluginFactory.class, 800);
          put(software.amazon.jdbc.plugin.failover2.FailoverConnectionPluginFactory.class, 900);
          put(GlobalDbFailoverConnectionPluginFactory.class, 1000);
          put(SqlParserConnectionPluginFactory.class, 1050);
          put(ReadWriteSplittingPluginFactory.class, 1100);
          put(AutoReadWriteSplittingPluginFactory.class, 1150);
          put(SimpleReadWriteSplittingPluginFactory.class, 1200);
          put(GdbReadWriteSplittingPluginFactory.class, 1300);
          put(GdbAutoReadWriteSplittingPluginFactory.class, 1310);
          put(AutoSimpleReadWriteSplittingPluginFactory.class, 1320);
          put(GdbSimpleReadWriteSplittingPluginFactory.class, 1330);
          put(GdbAutoSimpleReadWriteSplittingPluginFactory.class, 1340);
          put(HostMonitoringConnectionPluginV1Factory.class, 1400);
          put(HostMonitoringConnectionPluginV2Factory.class, 1500);
          put(FastestResponseStrategyPluginFactory.class, 1600);
          put(LimitlessConnectionPluginFactory.class, 1700);
          put(IamAuthConnectionPluginFactory.class, 1800);
          put(AwsSecretsManagerConnectionPluginFactory.class, 1900);
          put(AwsSecretsManagerConnectionPlugin2Factory.class, 1910);
          put(FederatedAuthPluginFactory.class, 2000);
          put(KmsEncryptionConnectionPluginFactory.class, 2050);
          put(LogQueryConnectionPluginFactory.class, 2100);
          put(ConnectTimeConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(ExecutionTimeConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
          put(DeveloperConnectionPluginFactory.class, WEIGHT_RELATIVE_TO_PRIOR_PLUGIN);
        }
      };

  protected static final ConcurrentMap<Class<? extends ConnectionPluginFactory>, ConnectionPluginFactory>
      pluginFactoriesByClass = new ConcurrentHashMap<>();

  protected static final String DEFAULT_PLUGINS = "initialConnection,auroraConnectionTracker,failover2,efm2";

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
      final @Nullable ConnectionProvider effectiveConnProvider,
      final Properties props,
      @Nullable ConfigurationProfile configurationProfile) throws SQLException {

    List<ConnectionPlugin> plugins;
    List<ConnectionPluginFactory> pluginFactories;

    final List<Class<? extends ConnectionPluginFactory>> pluginFactoryClasses =
        configurationProfile == null ? null : configurationProfile.getPluginFactories();
    if (pluginFactoryClasses != null) {
      pluginFactories = new ArrayList<>(pluginFactoryClasses.size());

      for (final Class<? extends ConnectionPluginFactory> factoryClazz : pluginFactoryClasses) {
        // Resolve (and cache) the factory instance. This replaces a previous computeIfAbsent call
        // whose mapping lambda returned null on failure; here the checked InstantiationException is
        // rethrown directly, preserving the same SQLException on error. The factory instances are
        // stateless singletons, so a rare concurrent double-create is harmless.
        ConnectionPluginFactory factory = pluginFactoriesByClass.get(factoryClazz);
        if (factory == null) {
          try {
            factory = WrapperUtils.createInstance(
                factoryClazz,
                ConnectionPluginFactory.class,
                (Class<?>[]) null);
          } catch (final InstantiationException ex) {
            throw new SQLException(
                Messages.get(
                    "ConnectionPluginManager.unableToLoadPlugin",
                    new Object[] {factoryClazz.getName()}),
                SqlState.UNKNOWN_STATE.getState(),
                ex);
          }
          pluginFactoriesByClass.put(factoryClazz, factory);
        }

        pluginFactories.add(factory);
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
        plugins.add(factory.getInstance(servicesContainer, props));
      }
    } else {
      plugins = new ArrayList<>(1); // one spot for default connection plugin
    }

    // add default connection plugin to the tail
    final ConnectionPlugin defaultPlugin = new DefaultConnectionPlugin(
        servicesContainer,
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
