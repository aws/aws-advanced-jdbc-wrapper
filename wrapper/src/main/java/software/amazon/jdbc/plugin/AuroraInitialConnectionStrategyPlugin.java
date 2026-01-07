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

  private final PluginService pluginService;
  private HostListProviderService hostListProviderService;
  private final RdsUtils rdsUtils = new RdsUtils();

  private final @NonNull RoleVerificationSetting verifyOpenedConnectionType;
  private final int retryDelayMs;
  private final long openConnectionRetryTimeoutNano;

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

    Connection candidateConn = null;
    if (!type.isRdsCluster()) {
      // The URL is not a cluster URL, so we can continue with the regular connect workflow.
      // TODO: still need to verify role
      return connectFunc.call();
    } else if (type == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      candidateConn = this.getInstanceConnection(props, isInitialConnection, connectFunc);
    } else if (type == RdsUrlType.RDS_WRITER_CLUSTER
        || type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.WRITER) {
      candidateConn = this.getWriterInstanceConnection(type, props, isInitialConnection, connectFunc);
    } else if (type == RdsUrlType.RDS_READER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
      candidateConn = this.getReaderInstanceConnection(type, hostSpec, props, isInitialConnection, connectFunc);
    }

    if (candidateConn == null) {
      LOGGER.finest(Messages.get(
          "AuroraInitialConnectionStrategyPlugin.connectingToOriginalUrl",
          new Object[]{hostSpec.getHost()}));
      return connectFunc.call();
    }

    return candidateConn;
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

  private Connection getWriterInstanceConnection(
      final RdsUrlType rdsUrlType,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    boolean isWriterClusterUrl =
        rdsUrlType == RdsUrlType.RDS_WRITER_CLUSTER || rdsUrlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER;
    // If verification is not set, we should only verify the writer if the original URL was a writer cluster URL.
    boolean isWriterVerificationRequired = this.verifyOpenedConnectionType == RoleVerificationSetting.WRITER
        || (this.verifyOpenedConnectionType == RoleVerificationSetting.NOT_SET && isWriterClusterUrl);

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;
    while (this.getTime() < endTimeNano) {
      Connection writerCandidateConn = null;
      HostSpec writerCandidate = Utils.getWriter(this.pluginService.getAllHosts());

      try {
        if (writerCandidate == null
            || this.rdsUtils.isRdsClusterDns(writerCandidate.getHost())
            || this.rdsUtils.isGlobalDbWriterClusterDns(writerCandidate.getHost())) {
          // Unable to find a writer instance host. Topology may not exist yet, or may be outdated.
          writerCandidateConn = connectFunc.call();
          if (!isWriterVerificationRequired) {
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
            }

            return writerCandidateConn;
          }

          this.pluginService.forceRefreshHostList();
          writerCandidate = this.pluginService.identifyConnection(writerCandidateConn);
          if (writerCandidate == null || writerCandidate.getRole() != HostRole.WRITER) {
            // We do not expect to hit this branch, but if we do, we should try again in the next iteration.
            this.closeConnection(writerCandidateConn);
            this.delay(this.retryDelayMs);
            continue;
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
          }

          return writerCandidateConn;
        }

        writerCandidateConn = this.pluginService.connect(writerCandidate, props, this);
        if (isWriterVerificationRequired && this.pluginService.getHostRole(writerCandidateConn) != HostRole.WRITER) {
          // The new connection resolved to a reader instance, which means the topology is outdated.
          // Force a topology refresh and try again in the next iteration.
          this.pluginService.forceRefreshHostList();
          this.closeConnection(writerCandidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
        }

        return writerCandidateConn;
      } catch (SQLException ex) {
        this.closeConnection(writerCandidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        }

        if (writerCandidate != null) {
          this.pluginService.setAvailability(writerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
        }

        if (this.pluginService.isNetworkException(ex, this.pluginService.getTargetDriverDialect())
            || this.pluginService.isReadOnlyConnectionException(ex, this.pluginService.getTargetDriverDialect())) {
          // Retry connection.
          continue;
        }

        throw ex;
      } catch (Throwable ex) {
        this.closeConnection(writerCandidateConn);
        throw ex;
      }
    }

    return null;
  }

  private Connection getReaderInstanceConnection(
      final RdsUrlType rdsUrlType,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    // If verification is not set, we should only verify the reader if the original URL was a reader cluster URL.
    final boolean isReaderVerificationRequired = this.verifyOpenedConnectionType == RoleVerificationSetting.READER
        || (this.verifyOpenedConnectionType == RoleVerificationSetting.NOT_SET
        && rdsUrlType == RdsUrlType.RDS_READER_CLUSTER);
    final HostRole requiredRole = isReaderVerificationRequired ? HostRole.READER : null;
    final String awsRegion = rdsUrlType == RdsUrlType.RDS_READER_CLUSTER
        ? this.rdsUtils.getRdsRegion(hostSpec.getHost()) : null;

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;
    while (this.getTime() < endTimeNano) {
      Connection readerCandidateConn = null;
      HostSpec readerCandidate = null;

      try {
        readerCandidate = this.getCandidateHost(props, requiredRole, awsRegion);
        if (readerCandidate == null || this.rdsUtils.isRdsClusterDns(readerCandidate.getHost())) {
          // Unable to find a reader instance host. Topology may not exist yet, or may be outdated.
          readerCandidateConn = connectFunc.call();
          if (!isReaderVerificationRequired) {
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
            }

            return readerCandidateConn;
          }

          this.pluginService.forceRefreshHostList();
          readerCandidate = this.pluginService.identifyConnection(readerCandidateConn);
          if (readerCandidate == null) {
            this.closeConnection(readerCandidateConn);
            this.delay(this.retryDelayMs);
            continue;
          }

          if (readerCandidate.getRole() != HostRole.READER) {
            if (this.hasNoReaders()) {
              // The cluster has no readers.
              // Simulate the reader cluster endpoint logic and return the current (writer) connection.
              if (this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
                LOGGER.finest("AuroraInitialConnectionStrategyPlugin.verifyReaderConfiguredButNoReadersExist");
              }

              if (isInitialConnection) {
                hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
              }
              return readerCandidateConn;
            }

            this.closeConnection(readerCandidateConn);
            this.delay(this.retryDelayMs);
            continue;
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
          }
          return readerCandidateConn;
        }

        readerCandidateConn = this.pluginService.connect(readerCandidate, props, this);
        if (isReaderVerificationRequired && this.pluginService.getHostRole(readerCandidateConn) != HostRole.READER) {
          // The new connection resolved to a writer instance, which means the topology is outdated.
          // Force a topology refresh.
          this.pluginService.forceRefreshHostList();
          if (this.hasNoReaders()) {
            // The cluster has no readers.
            // Simulate the reader cluster endpoint logic and return the current (writer) connection.
            if (this.verifyOpenedConnectionType == RoleVerificationSetting.READER) {
              LOGGER.finest("AuroraInitialConnectionStrategyPlugin.verifyReaderConfiguredButNoReadersExist");
            }

            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
            }
            return readerCandidateConn;
          }

          this.closeConnection(readerCandidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
        }

        return readerCandidateConn;
      } catch (SQLException ex) {
        this.closeConnection(readerCandidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        }

        if (readerCandidate != null) {
          this.pluginService.setAvailability(readerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
        }
      } catch (Throwable ex) {
        this.closeConnection(readerCandidateConn);
        throw ex;
      }
    }

    return null;
  }

  private Connection getInstanceConnection(
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;
    final HostRole requiredRole = RoleVerificationSetting.getRequiredHostRole(this.verifyOpenedConnectionType);
    while (this.getTime() < endTimeNano) {
      Connection candidateConn = null;
      HostSpec candidateHost = null;

      try {
        candidateHost = this.getCandidateHost(props, requiredRole, null);
        if (candidateHost == null || this.rdsUtils.isRdsClusterDns(candidateHost.getHost())) {
          // Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
          candidateConn = connectFunc.call();
          if (requiredRole == null) {
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(candidateHost);
            }

            return candidateConn;
          }

          this.pluginService.forceRefreshHostList();
          candidateHost = this.pluginService.identifyConnection(candidateConn);
          if (candidateHost == null || candidateHost.getRole() != requiredRole) {
            this.closeConnection(candidateConn);
            this.delay(this.retryDelayMs);
            continue;
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(candidateHost);
          }

          return candidateConn;
        }

        candidateConn = this.pluginService.connect(candidateHost, props, this);
        if (requiredRole != null && this.pluginService.getHostRole(candidateConn) != requiredRole) {
          this.pluginService.forceRefreshHostList();
          if (requiredRole == HostRole.READER && this.hasNoReaders()) {
            // The cluster has no readers.
            // Simulate the reader cluster endpoint logic and return the current (writer) connection.
            LOGGER.finest("AuroraInitialConnectionStrategyPlugin.verifyReaderConfiguredButNoReadersExist");
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(candidateHost);
            }

            return candidateConn;
          }

          this.closeConnection(candidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(candidateHost);
        }

        return candidateConn;
      } catch (SQLException ex) {
        this.closeConnection(candidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        } else {
          if (candidateHost != null) {
            this.pluginService.setAvailability(candidateHost.asAliases(), HostAvailability.NOT_AVAILABLE);
          }
        }
      } catch (Throwable ex) {
        this.closeConnection(candidateConn);
        throw ex;
      }
    }

    return null;
  }

  private void closeConnection(final Connection connection) {
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
      final Properties props, final @Nullable HostRole hostRole, final @Nullable String awsRegion)
      throws SQLException {
    String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    // The HOST_SELECTOR_STRATEGY property should override the deprecated READER_HOST_SELECTOR_STRATEGY if it is set.
    if (props.containsKey(HOST_SELECTOR_STRATEGY.name)) {
      strategy = HOST_SELECTOR_STRATEGY.getString(props);
    }

    if (!this.pluginService.acceptsStrategy(hostRole, strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "AuroraInitialConnectionStrategyPlugin.unsupportedStrategy",
              new Object[] {strategy}));
    }

    try {
      if (!StringUtils.isNullOrEmpty(awsRegion)) {
        final List<HostSpec> hostsInRegion = this.pluginService.getHosts()
            .stream()
            .filter(x -> awsRegion.equalsIgnoreCase(this.rdsUtils.getRdsRegion(x.getHost())))
            .collect(Collectors.toList());
        return this.pluginService.getHostSpecByStrategy(hostsInRegion, hostRole, strategy);
      } else {
        return this.pluginService.getHostSpecByStrategy(hostRole, strategy);
      }
    } catch (SQLException ex) {
      // Unable to find candidate host.
      return null;
    }
  }

  private boolean hasNoReaders() {
    if (this.pluginService.getAllHosts().isEmpty()) {
      // Topology inconclusive/corrupted.
      return false;
    }

    for (HostSpec hostSpec : this.pluginService.getAllHosts()) {
      if (hostSpec.getRole() == HostRole.WRITER) {
        continue;
      }

      // Found a reader instance.
      return false;
    }

    // Went through all hosts but no readers were found.
    return true;
  }

  // Method implemented to simplify unit testing.
  protected long getTime() {
    return System.nanoTime();
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

    public static HostRole getRequiredHostRole(RoleVerificationSetting roleVerificationSetting) {
      switch (roleVerificationSetting) {
        case WRITER:
          return HostRole.WRITER;
        case READER:
          return HostRole.READER;
        default:
          // If NOT_SET or NO_VERIFICATION, return null to indicate there is no host role requirement.
          return null;
      }
    }
  }
}
