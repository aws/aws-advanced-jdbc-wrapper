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

  public static final AwsWrapperProperty VERIFY_OPENED_CONNECTION_TYPE =
      new AwsWrapperProperty(
          "verifyOpenedConnectionType",
          null,
          "Force to verify an opened connection to be either a writer or a reader.",
          false,
          new String[] {
              "writer", "reader", "no"
          });

  private final @NonNull RoleVerificationSetting verifyOpenedConnectionType;
  private final RdsUtils rdsUtils = new RdsUtils();
  private final PluginService pluginService;
  private final String hostSelectionStrategy;
  private final int retryDelayMs;
  private final long openConnectionRetryTimeoutNano;
  private HostListProviderService hostListProviderService;

  static {
    PropertyDefinition.registerPluginProperties(AuroraInitialConnectionStrategyPlugin.class);
  }

  public AuroraInitialConnectionStrategyPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.verifyOpenedConnectionType =
        RoleVerificationSetting.getRoleVerificationSetting(VERIFY_OPENED_CONNECTION_TYPE.getString(properties));
    this.retryDelayMs = OPEN_CONNECTION_RETRY_INTERVAL_MS.getInteger(properties);
    this.openConnectionRetryTimeoutNano =
        TimeUnit.MILLISECONDS.toNanos(OPEN_CONNECTION_RETRY_TIMEOUT_MS.getInteger(properties));

    // The HOST_SELECTOR_STRATEGY property should override the deprecated READER_HOST_SELECTOR_STRATEGY if it is set.
    if (properties.containsKey(HOST_SELECTOR_STRATEGY.name)) {
      this.hostSelectionStrategy = HOST_SELECTOR_STRATEGY.getString(properties);
    } else {
      this.hostSelectionStrategy = READER_HOST_SELECTOR_STRATEGY.getString(properties);
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
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    final RdsUrlType type = this.rdsUtils.identifyRdsType(hostSpec.getHost());
    if (isInitialConnection) {
      validateVerificationSetting(type);
    }

    return this.getConnection(connectFunc, hostSpec, type, props, isInitialConnection);
  }

  private void validateVerificationSetting(RdsUrlType type) throws SQLException {
    if (type == RdsUrlType.RDS_WRITER_CLUSTER
        && this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidVerifyConfiguration",
          new Object[]{"reader", "cluster writer"}));
    }

    if (type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        && this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidVerifyConfiguration",
          new Object[]{"reader", "global cluster"}));
    }

    if (type == RdsUrlType.RDS_READER_CLUSTER
        && this.verifyOpenedConnectionType == RoleVerificationSetting.WRITER) {
      throw new SQLException(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.invalidVerifyConfiguration",
          new Object[]{"writer", "cluster reader"}));
    }
  }

  private Connection getConnection(
      JdbcCallable<Connection, SQLException> connectFunc,
      HostSpec originalConnectHost,
      RdsUrlType urlType,
      Properties props,
      boolean isInitialConnection)
      throws SQLException {
    final UrlSubstitutionStrategy substitutionStrategy = this.getUrlSubstitutionStrategy(urlType, isInitialConnection);
    final HostRole roleToVerify = this.getRoleToVerify(urlType, isInitialConnection);
    final long endTimeNano = System.nanoTime() + this.openConnectionRetryTimeoutNano;

    while (System.nanoTime() < endTimeNano) {
      Connection candidateConn = null;
      HostSpec candidateHost = null;

      try {
        if (substitutionStrategy.equals(UrlSubstitutionStrategy.DO_NOT_SUBSTITUTE)) {
          candidateHost = originalConnectHost;
        } else {
          candidateHost = this.getCandidateHost(originalConnectHost, urlType, props, roleToVerify);
        }

        if (candidateHost == null || this.rdsUtils.isRdsClusterDns(candidateHost.getHost())) {
          // Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
          candidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList();
        } else {
          candidateConn = this.pluginService.connect(candidateHost, props, this);
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
        if (roleToVerify == HostRole.WRITER || Utils.isNullOrEmpty(allHosts) || this.hasReaders(allHosts)) {
          this.closeConnection(candidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        // A reader was requested but the cluster has no readers.
        // Simulate the reader cluster endpoint logic and return the current (writer) connection.
        if (this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
          LOGGER.finest("AuroraInitialConnectionStrategyPlugin.verifyReaderConfiguredButNoReadersExist");
        }

        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(candidateHost);
        }

        return candidateConn;
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
            && (roleToVerify == HostRole.WRITER || substitutionStrategy == UrlSubstitutionStrategy.SUBSTITUTE_WRITER)) {
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
        "AuroraInitialConnectionStrategyPlugin.timeout", new Object[] {this.openConnectionRetryTimeoutNano}));
  }

  private UrlSubstitutionStrategy getUrlSubstitutionStrategy(RdsUrlType urlType, boolean isInitialConnection) {
    if (urlType == RdsUrlType.RDS_WRITER_CLUSTER
        || urlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.WRITER) {
      return UrlSubstitutionStrategy.SUBSTITUTE_WRITER;
    } else if (urlType == RdsUrlType.RDS_READER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
      return UrlSubstitutionStrategy.SUBSTITUTE_READER;
    } else if (urlType == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      return UrlSubstitutionStrategy.SUBSTITUTE_ANY;
    }

    return UrlSubstitutionStrategy.DO_NOT_SUBSTITUTE;
  }

  private HostRole getRoleToVerify(RdsUrlType urlType, boolean isInitialConnection) {
    if (this.verifyOpenedConnectionType == RoleVerificationSetting.NO_VERIFICATION) {
      return null;
    }

    if (isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.WRITER) {
      return HostRole.WRITER;
    }

    if (isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
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

  private HostSpec getCandidateHost(
      final HostSpec originalConnectHost,
      final RdsUrlType urlType,
      final Properties props,
      final @Nullable HostRole roleToVerify)
      throws SQLException {
    if (!this.pluginService.acceptsStrategy(roleToVerify, this.hostSelectionStrategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "AuroraInitialConnectionStrategyPlugin.unsupportedStrategy",
              new Object[] {this.hostSelectionStrategy}));
    }

    try {
      // TODO: is it correct that we need to filter by region for all writer/reader/custom/global endpoints?
      final String awsRegion = urlType.isRdsCluster()
          ? this.rdsUtils.getRdsRegion(originalConnectHost.getHost()) : null;
      if (!StringUtils.isNullOrEmpty(awsRegion)) {
        final List<HostSpec> hostsInRegion = this.pluginService.getHosts()
            .stream()
            .filter(x -> awsRegion.equalsIgnoreCase(this.rdsUtils.getRdsRegion(x.getHost())))
            .collect(Collectors.toList());
        return this.pluginService.getHostSpecByStrategy(hostsInRegion, roleToVerify, this.hostSelectionStrategy);
      } else {
        return this.pluginService.getHostSpecByStrategy(roleToVerify, this.hostSelectionStrategy);
      }
    } catch (SQLException ex) {
      // Unable to find candidate host.
      return null;
    }
  }

  private boolean hasReaders(@NonNull List<HostSpec> hosts) {
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

  private enum UrlSubstitutionStrategy {
    SUBSTITUTE_WRITER,
    SUBSTITUTE_READER,
    SUBSTITUTE_ANY,
    DO_NOT_SUBSTITUTE
  }

  private enum RoleVerificationSetting {
    WRITER,
    READER,
    NO_VERIFICATION,
    NOT_SET;

    private static final Map<String, RoleVerificationSetting> roleVerificationSettingsByKey =
        new HashMap<String, RoleVerificationSetting>() {
          {
            // Note that there is no entry for HostRole.UNKNOWN, as that is not a valid role verification setting.
            put("writer", WRITER);
            put("reader", READER);
            put("no", NO_VERIFICATION);
          }
        };

    public static @NonNull RoleVerificationSetting getRoleVerificationSetting(String value) {
      if (value == null) {
        return NOT_SET;
      }

      RoleVerificationSetting verification = roleVerificationSettingsByKey.get(value.toLowerCase());
      return verification == null ? NOT_SET : verification;
    }
  }
}
