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

package software.amazon.jdbc.profile;

import com.zaxxer.hikari.HikariConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.plugin.AuroraConnectionTrackerPluginFactory;
import software.amazon.jdbc.plugin.AuroraInitialConnectionStrategyPluginFactory;
import software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPlugin;
import software.amazon.jdbc.plugin.efm2.HostMonitoringConnectionPluginFactory;
import software.amazon.jdbc.plugin.failover.FailoverConnectionPluginFactory;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingPluginFactory;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsPluginFactory;

public class DriverConfigurationProfiles {

  private static final Map<String, ConfigurationProfile> presets;

  private static final Map<String, ConfigurationProfile> activeProfiles =
      new ConcurrentHashMap<>();

  private static final String MONITORING_CONNECTION_PREFIX = "monitoring-";

  static {
    presets = getConfigurationProfilePresets();
  }

  public static void clear() {
    activeProfiles.clear();
  }

  public static void addOrReplaceProfile(
      @NonNull final String profileName,
      @NonNull final ConfigurationProfile configurationProfile) {
    activeProfiles.put(profileName, configurationProfile);
  }

  public static void remove(@NonNull final String profileName) {
    activeProfiles.remove(profileName);
  }

  public static boolean contains(@NonNull final String profileName) {
    return activeProfiles.containsKey(profileName);
  }

  public static ConfigurationProfile getProfileConfiguration(@NonNull final String profileName) {
    ConfigurationProfile profile = activeProfiles.get(profileName);

    if (profile != null) {
      return profile;
    }
    return presets.get(profileName);
  }

  private static Map<String, ConfigurationProfile> getConfigurationProfilePresets() {
    Map<String, ConfigurationProfile> presets = new ConcurrentHashMap<>();

    presets.put(ConfigurationProfilePresetCodes.A0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.A0,
            Collections.emptyList(), // empty list is important here! it shouldn't be a null.
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.A1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.A1,
            Collections.emptyList(), // empty list is important here! it shouldn't be a null.
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "30000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "30000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "30000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.A2,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.A2,
            Collections.emptyList(), // empty list is important here! it shouldn't be a null.
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "3000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "3000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "3000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.B,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.B,
            Collections.emptyList(), // empty list is important here! it shouldn't be a null.
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "true"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.C0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.C0,
            Collections.singletonList(HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "60000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "5",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "15000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.C1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.C1,
            Collections.singletonList(HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "30000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "3",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "3000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.D0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.D0,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                ReadWriteSplittingPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.D1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.D1,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                ReadWriteSplittingPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "30000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "30000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "30000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.E,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.E,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                ReadWriteSplittingPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "true"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.F0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.F0,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                ReadWriteSplittingPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "60000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "5",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "15000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.F1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.F1,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                ReadWriteSplittingPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "30000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "3",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "3000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.G0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.G0,
            Arrays.asList(
                AuroraConnectionTrackerPluginFactory.class,
                AuroraStaleDnsPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.G1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.G1,
            Arrays.asList(
                AuroraConnectionTrackerPluginFactory.class,
                AuroraStaleDnsPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "30000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "30000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "30000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.H,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.H,
            Arrays.asList(
                AuroraConnectionTrackerPluginFactory.class,
                AuroraStaleDnsPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "true"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.I0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.I0,
            Arrays.asList(
                AuroraConnectionTrackerPluginFactory.class,
                AuroraStaleDnsPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "60000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "5",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "15000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    presets.put(ConfigurationProfilePresetCodes.I1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.I1,
            Arrays.asList(
                AuroraConnectionTrackerPluginFactory.class,
                AuroraStaleDnsPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "30000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "3",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "3000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            (Dialect) null,
            null,
            null,
            null,
            null));

    // Spring Framework / Spring Boot optimized presets

    presets.put(ConfigurationProfilePresetCodes.SF_D0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.SF_D0,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.SF_D1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.SF_D1,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "30000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "30000",
                PropertyDefinition.LOGIN_TIMEOUT.name, "30000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.SF_E,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.SF_E,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                FailoverConnectionPluginFactory.class),
            getProperties(
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "true"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.SF_F0,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.SF_F0,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "60000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "5",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "15000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    presets.put(ConfigurationProfilePresetCodes.SF_F1,
        new ConfigurationProfile(
            ConfigurationProfilePresetCodes.SF_F1,
            Arrays.asList(
                AuroraInitialConnectionStrategyPluginFactory.class,
                AuroraConnectionTrackerPluginFactory.class,
                FailoverConnectionPluginFactory.class,
                HostMonitoringConnectionPluginFactory.class),
            getProperties(
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME.name, "30000",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT.name, "3",
                HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL.name, "5000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.CONNECT_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.SOCKET_TIMEOUT.name, "3000",
                MONITORING_CONNECTION_PREFIX + PropertyDefinition.LOGIN_TIMEOUT.name, "3000",
                PropertyDefinition.CONNECT_TIMEOUT.name, "10000",
                PropertyDefinition.SOCKET_TIMEOUT.name, "0",
                PropertyDefinition.LOGIN_TIMEOUT.name, "10000",
                PropertyDefinition.TCP_KEEP_ALIVE.name, "false"),
            null,
            null,
            null,
            () -> new HikariPooledConnectionProvider(
                (HostSpec hostSpec, Properties originalProps) -> {
                  final HikariConfig config = new HikariConfig();
                  config.setMaximumPoolSize(30);
                  // holds few extra connections in case of sudden traffic peak
                  config.setMinimumIdle(2);
                  // close idle connection in 15min; helps to get back to normal pool size after load peak
                  config.setIdleTimeout(TimeUnit.MINUTES.toMillis(15));
                  // verify pool configuration and creates no connections during initialization phase
                  config.setInitializationFailTimeout(-1);
                  config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(10));
                  // validate idle connections at least every 3 min
                  config.setKeepaliveTime(TimeUnit.MINUTES.toMillis(3));
                  // allows to quickly validate connection in the pool and move on to another connection if needed
                  config.setValidationTimeout(TimeUnit.SECONDS.toMillis(1));
                  config.setMaxLifetime(TimeUnit.DAYS.toMillis(1));
                  return config;
                },
                null
            ),
            null));

    return presets;
  }

  private static Properties getProperties(String... args) {
    if (args == null) {
      return null;
    }

    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Properties should be passed by pairs: property name and property value.");
    }

    final Properties props = new Properties();

    for (int i = 0; i < args.length; i += 2) {
      props.put(args[i], args[i + 1]);
    }

    return props;
  }
}
