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

package software.amazon.jdbc.plugin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.WrapperUtils;

public class AuroraInitialConnectionStrategyPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AuroraInitialConnectionStrategyPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("initHostProvider");
          add("connect");
        }
      });

  // Deprecated. Use "initialConnectionHostSelectorStrategy" instead.
  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "readerInitialConnectionHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host while opening a new connection.");

  public static final AwsWrapperProperty HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "initialConnectionHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a host while opening a new connection.");

  public static final AwsWrapperProperty OPEN_CONNECTION_RETRY_TIMEOUT_MS =
      new AwsWrapperProperty(
          "openConnectionRetryTimeoutMs",
          "30000",
          "Maximum allowed time for the retries opening a connection.");

  public static final AwsWrapperProperty OPEN_CONNECTION_RETRY_INTERVAL_MS =
      new AwsWrapperProperty(
          "openConnectionRetryIntervalMs",
          "1000",
          "Time between each retry of opening a connection.");

  public static final AwsWrapperProperty ENDPOINT_SUBSTITUTION_ROLE =
      new AwsWrapperProperty(
          "endpointSubstitutionRole",
          null,
          "Defines whether or not the initial connection URL should be replaced with an instance URL from the"
              + " topology info when available, and if so, the role of the instance URL that should be selected.",
          false,
          new String[] {
              "writer", "reader", "any", "none"
          });

  public static final AwsWrapperProperty VERIFY_OPENED_CONNECTION_ROLE =
      new AwsWrapperProperty(
          "verifyOpenedConnectionType",
          null,
          "Defines whether an opened connection should be verified to be a writer or reader, "
              + "or if no role verification should be performed.",
          false,
          new String[] {
              "writer", "reader", "none"
          });

  private final RdsUtils rdsUtils = new RdsUtils();
  private final PluginService pluginService;
  private final int retryDelayMs;
  private final long openConnectionRetryTimeoutNano;
  private final @Nullable String selectionStrategyPropValue;
  private final @Nullable String verifyRolePropValue;
  private HostListProviderService hostListProviderService;

  static {
    PropertyDefinition.registerPluginProperties(AuroraInitialConnectionStrategyPlugin.class);
  }

  public AuroraInitialConnectionStrategyPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.retryDelayMs = OPEN_CONNECTION_RETRY_INTERVAL_MS.getInteger(properties);
    this.openConnectionRetryTimeoutNano =
        TimeUnit.MILLISECONDS.toNanos(OPEN_CONNECTION_RETRY_TIMEOUT_MS.getInteger(properties));
    String verifyRolePropValue = VERIFY_OPENED_CONNECTION_ROLE.getString(properties);
    this.verifyRolePropValue = verifyRolePropValue == null ? null : verifyRolePropValue.toUpperCase();

    // The HOST_SELECTOR_STRATEGY property should override the deprecated READER_HOST_SELECTOR_STRATEGY if it is set.
    if (properties.containsKey(HOST_SELECTOR_STRATEGY.name)) {
      this.selectionStrategyPropValue = HOST_SELECTOR_STRATEGY.getString(properties);
    } else {
      this.selectionStrategyPropValue = READER_HOST_SELECTOR_STRATEGY.getString(properties);
    }
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties props,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc) throws SQLException {

    this.hostListProviderService = hostListProviderService;
    initHostProviderFunc.call();
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec originalConnectHost,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final RdsUrlType urlType = this.rdsUtils.identifyRdsType(originalConnectHost.getHost());
    final InstanceSubstitutionStrategy substitutionStrategy =
        this.getInstanceSubstitutionStrategy(props, urlType, isInitialConnection);
    final HostRole roleToVerify = this.getRoleToVerify(urlType, isInitialConnection);
    final long endTimeNano = System.nanoTime() + this.openConnectionRetryTimeoutNano;

    while (System.nanoTime() < endTimeNano) {
      Connection candidateConn = null;
      HostSpec candidateHost = null;

      try {
        if (substitutionStrategy.equals(InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE)) {
          candidateHost = originalConnectHost;
          candidateConn = connectFunc.call();
        } else {
          candidateHost = this.getCandidateHost(originalConnectHost, urlType, substitutionStrategy);
          if (candidateHost == null || this.rdsUtils.isRdsClusterDns(candidateHost.getHost())) {
            // Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
            candidateConn = connectFunc.call();
            candidateHost = originalConnectHost;
            this.pluginService.forceRefreshHostList();
          } else {
            candidateConn = this.pluginService.connect(candidateHost, props, this);
          }
        }

        if (roleToVerify == null) {
          // No verification required.
          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(candidateHost);
          }

          return candidateConn;
        }

        HostRole connRole = this.pluginService.getHostRole(candidateConn);
        if (connRole == roleToVerify) {
          // Verification succeeded.
          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(candidateHost);
          }

          return candidateConn;
        }

        // Verification failed.
        // We will try again, unless a reader was requested but the cluster has no readers, which is a special case.
        this.pluginService.forceRefreshHostList();
        List<HostSpec> allHosts = this.pluginService.getAllHosts();
        if (roleToVerify == HostRole.READER && !Utils.isNullOrEmpty(allHosts) && !this.hasReaders(allHosts)) {
          // A reader was requested but the cluster has no readers.
          // Simulate the reader cluster endpoint logic and return the current (writer) connection.
          if (RoleVerificationSetting.READER.name().equals(this.verifyRolePropValue)) {
            LOGGER.finest(Messages.get(
                "AuroraInitialConnectionStrategyPlugin.verifyReaderConfiguredButNoReadersExist",
                new Object[] {VERIFY_OPENED_CONNECTION_ROLE.name}));
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(candidateHost);
          }

          return candidateConn;
        }

        // Failed to verify the connection. We will try to get a verified connection again on the next iteration.
        LOGGER.finest(Messages.get(
            "AuroraInitialConnectionStrategyPlugin.incorrectRole",
            new Object[]{candidateHost.getHost(), roleToVerify}));
        this.closeConnection(candidateConn);
        this.delay(this.retryDelayMs);
      } catch (SQLException ex) {
        this.closeConnection(candidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        }

        if (candidateHost != null) {
          this.pluginService.setAvailability(candidateHost.asAliases(), HostAvailability.NOT_AVAILABLE);
        }

        if (this.pluginService.isNetworkException(ex, this.pluginService.getTargetDriverDialect())) {
          // Retry connection.
          continue;
        }

        if (this.pluginService.isReadOnlyConnectionException(ex, this.pluginService.getTargetDriverDialect())
            && (roleToVerify == HostRole.WRITER
            || substitutionStrategy == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)) {
          // Retry connection.
          continue;
        }

        throw ex;
      } catch (Throwable ex) {
        this.closeConnection(candidateConn);
        throw ex;
      }
    }

    throw new SQLException(Messages.get(
        "AuroraInitialConnectionStrategyPlugin.timeout",
        new Object[] {
            TimeUnit.NANOSECONDS.toMillis(this.openConnectionRetryTimeoutNano),
            VERIFY_OPENED_CONNECTION_ROLE.name}));
  }

  protected @NonNull InstanceSubstitutionStrategy getInstanceSubstitutionStrategy(
      Properties props, RdsUrlType urlType, boolean isInitialConnection)
      throws SQLException {
    if (isInitialConnection) {
      InstanceSubstitutionStrategy strategy =
          InstanceSubstitutionStrategy.fromPropertyValue(ENDPOINT_SUBSTITUTION_ROLE.getString(props));
      if (strategy != null) {
        validateSubstitutionStrategy(strategy, urlType);
        return strategy;
      }
    }

    // This is not an initial connection, or INSTANCE_SUBSTITUTION_ROLE was not set.
    // We will pick a strategy according to the default behavior.
    if (urlType == RdsUrlType.RDS_WRITER_CLUSTER
        || urlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) {
      return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER;
    } else if (urlType == RdsUrlType.RDS_READER_CLUSTER) {
      return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER;
    }

    return InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE;
  }

  protected void validateSubstitutionStrategy(@NonNull InstanceSubstitutionStrategy setting, RdsUrlType urlType)
      throws SQLException {
    if (setting == InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE) {
      return;
    }

    if (urlType == RdsUrlType.RDS_INSTANCE) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForInstanceEndpoint",
          new Object[] {ENDPOINT_SUBSTITUTION_ROLE.name}));
    }

    if (!urlType.isRdsCluster()) {
      return;
    }

    // When you create a custom cluster, it can only be of type "reader" or type "any",
    // so SUBSTITUTE_WRITER is not allowed.
    if (setting == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER
        && (urlType == RdsUrlType.RDS_READER_CLUSTER || urlType == RdsUrlType.RDS_CUSTOM_CLUSTER)) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForEndpoint",
          new Object[] {ENDPOINT_SUBSTITUTION_ROLE.name, "writer", "reader cluster or custom cluster"}));
    }

    if (setting == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER
        && (urlType == RdsUrlType.RDS_WRITER_CLUSTER || urlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER)) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForEndpoint",
          new Object[] {ENDPOINT_SUBSTITUTION_ROLE.name, "reader", "writer cluster or global cluster"}));
    }

    if (setting == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_ANY && urlType != RdsUrlType.RDS_CUSTOM_CLUSTER) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForEndpoint",
          new Object[] {ENDPOINT_SUBSTITUTION_ROLE.name, "any", "writer cluster, reader cluster, or global cluster"}));
    }
  }

  protected @Nullable HostRole getRoleToVerify(RdsUrlType urlType, boolean isInitialConnection) throws SQLException {
    if (!isInitialConnection) {
      return null;
    }

    RoleVerificationSetting setting = RoleVerificationSetting.fromPropertyValue(this.verifyRolePropValue);
    if (setting != null) {
      validateVerificationSetting(setting, urlType);
    }

    if (setting == RoleVerificationSetting.NO_VERIFICATION) {
      return null;
    }

    if (setting == RoleVerificationSetting.WRITER) {
      return HostRole.WRITER;
    }

    if (setting == RoleVerificationSetting.READER) {
      return HostRole.READER;
    }

    // Role verification setting is not set.
    // We still should verify the correct role if we are using a writer/reader cluster.
    if (urlType == RdsUrlType.RDS_WRITER_CLUSTER
        || urlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) {
      return HostRole.WRITER;
    }

    if (urlType == RdsUrlType.RDS_READER_CLUSTER) {
      return HostRole.READER;
    }

    return null;
  }

  protected void validateVerificationSetting(@NonNull RoleVerificationSetting setting, RdsUrlType urlType)
      throws SQLException {
    if (setting == RoleVerificationSetting.READER
        && (urlType == RdsUrlType.RDS_WRITER_CLUSTER || urlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER)) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForEndpoint",
          new Object[]{VERIFY_OPENED_CONNECTION_ROLE.name, "reader", "writer cluster or global cluster"}));
    }

    // When you create a custom cluster, it can only be of type "reader" or type "any".
    if (setting == RoleVerificationSetting.WRITER
        && (urlType == RdsUrlType.RDS_READER_CLUSTER || urlType == RdsUrlType.RDS_CUSTOM_CLUSTER)) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidSettingForEndpoint",
          new Object[]{VERIFY_OPENED_CONNECTION_ROLE.name, "writer", "reader cluster or custom cluster"}));
    }
  }

  private void closeConnection(final @Nullable Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (final SQLException ex) {
        // ignore
      }
    }
  }

  private void delay(final long delayMs) {
    try {
      TimeUnit.MILLISECONDS.sleep(delayMs);
    } catch (InterruptedException ex) {
      // ignore
    }
  }

  protected HostSpec getCandidateHost(
      final @NonNull HostSpec originalConnectHost,
      final @NonNull RdsUrlType urlType,
      final @NonNull InstanceSubstitutionStrategy substitutionStrategy)
      throws SQLException {
    if (substitutionStrategy == InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE) {
      return originalConnectHost;
    }

    if (substitutionStrategy == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER) {
      return Utils.getWriter(this.pluginService.getAllHosts());
    }

    HostRole targetRole = InstanceSubstitutionStrategy.toTargetRole(substitutionStrategy);
    if (!this.pluginService.acceptsStrategy(targetRole, this.selectionStrategyPropValue)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "AuroraInitialConnectionStrategyPlugin.unsupportedStrategy",
              new Object[] {this.selectionStrategyPropValue}));
    }

    try {
      final String awsRegion = urlType.isRdsCluster()
          ? this.rdsUtils.getRdsRegion(originalConnectHost.getHost()) : null;
      if (!StringUtils.isNullOrEmpty(awsRegion)) {
        final List<HostSpec> hostsInRegion = this.pluginService.getHosts()
            .stream()
            .filter(x -> awsRegion.equalsIgnoreCase(this.rdsUtils.getRdsRegion(x.getHost())))
            .collect(Collectors.toList());
        return this.pluginService.getHostSpecByStrategy(hostsInRegion, targetRole, this.selectionStrategyPropValue);
      } else {
        return this.pluginService.getHostSpecByStrategy(targetRole, this.selectionStrategyPropValue);
      }
    } catch (SQLException ex) {
      // Unable to find candidate host.
      return null;
    }
  }

  protected boolean hasReaders(@NonNull List<HostSpec> hosts) {
    for (HostSpec hostSpec : hosts) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        continue;
      }

      // Found a reader instance.
      return true;
    }

    // Went through all hosts but no readers were found.
    return false;
  }

  protected enum InstanceSubstitutionStrategy {
    SUBSTITUTE_WITH_WRITER,
    SUBSTITUTE_WITH_READER,
    SUBSTITUTE_WITH_ANY,
    DO_NOT_SUBSTITUTE;

    private static final Map<String, InstanceSubstitutionStrategy> strategiesByKey =
        new HashMap<String, InstanceSubstitutionStrategy>() {
          {
            put("writer", SUBSTITUTE_WITH_WRITER);
            put("reader", SUBSTITUTE_WITH_READER);
            put("any", SUBSTITUTE_WITH_ANY);
            put("none", DO_NOT_SUBSTITUTE);
          }
        };

    public static @Nullable InstanceSubstitutionStrategy fromPropertyValue(String value)
        throws SQLException {
      if (value == null) {
        return null;
      }

      InstanceSubstitutionStrategy strategy = strategiesByKey.get(value.toLowerCase());
      if (strategy == null) {
        throw new SQLException(Messages.get(
            "AuroraInitialConnectionStrategyPlugin.invalidPropertyValue",
            new Object[]{ENDPOINT_SUBSTITUTION_ROLE.name, value, ENDPOINT_SUBSTITUTION_ROLE.getChoices()}));
      }

      return strategy;
    }

    public static @Nullable HostRole toTargetRole(InstanceSubstitutionStrategy substitutionStrategy) {
      if (substitutionStrategy == InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER) {
        return HostRole.WRITER;
      }

      if (substitutionStrategy == SUBSTITUTE_WITH_READER) {
        return HostRole.READER;
      }

      return null;
    }
  }

  protected enum RoleVerificationSetting {
    WRITER,
    READER,
    NO_VERIFICATION;

    private static final Map<String, RoleVerificationSetting> settingsByKey =
        new HashMap<String, RoleVerificationSetting>() {
          {
            // Note that there is no entry for HostRole.UNKNOWN, as that is not a valid role verification setting.
            put("writer", WRITER);
            put("reader", READER);
            put("none", NO_VERIFICATION);
          }
        };

    public static @Nullable RoleVerificationSetting fromPropertyValue(String value) throws SQLException {
      if (value == null) {
        return null;
      }

      RoleVerificationSetting verification = settingsByKey.get(value.toLowerCase());
      if (verification == null) {
        throw new SQLException(Messages.get(
            "AuroraInitialConnectionStrategyPlugin.invalidPropertyValue",
            new Object[]{VERIFY_OPENED_CONNECTION_ROLE.name, value, VERIFY_OPENED_CONNECTION_ROLE.getChoices()}));
      }

      return verification;
    }
  }
}
