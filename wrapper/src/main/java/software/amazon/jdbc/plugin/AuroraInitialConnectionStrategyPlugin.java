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

  private final HostRoleVerification verifyOpenedConnectionType;
  private final int retryDelayMs;
  private final long openConnectionRetryTimeoutNano;

  static {
    PropertyDefinition.registerPluginProperties(AuroraInitialConnectionStrategyPlugin.class);
  }

  public AuroraInitialConnectionStrategyPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.verifyOpenedConnectionType =
        HostRoleVerification.verifyConnectionTypeFromValue(VERIFY_OPENED_CONNECTION_TYPE.getString(properties));
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

    if (!type.isRdsCluster()) {
      // It's not a cluster endpoint. Continue with a normal workflow.
      return connectFunc.call();
    }

    if (isInitialConnection) {
      if (type == RdsUrlType.RDS_WRITER_CLUSTER
          && this.verifyOpenedConnectionType == HostRoleVerification.READER) {
        throw new SQLException(
            "Wrong configuration. When using an Aurora Cluster Writer Endpoint parameter "
            + "'verifyOpenedConnectionType' can't be 'reader'.");
      }
      if (type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
          && this.verifyOpenedConnectionType == HostRoleVerification.READER) {
        throw new SQLException(
            "Wrong configuration. When using an Aurora Global Cluster Endpoint parameter "
            + "'verifyOpenedConnectionType' can't be 'reader'.");
      }
      if (type == RdsUrlType.RDS_READER_CLUSTER
          && this.verifyOpenedConnectionType == HostRoleVerification.WRITER) {
        throw new SQLException(
            "Wrong configuration. When using an Aurora Cluster Reader Endpoint parameter "
            + "'verifyOpenedConnectionType' can't be 'writer'.");
      }
    }

    if (type == RdsUrlType.RDS_CUSTOM_CLUSTER) {
      Connection candidateConn =
          this.getVerifiedConnection(props, isInitialConnection, connectFunc);
      if (candidateConn == null) {
        // Can't get a connection. Continue with a normal workflow.
        LOGGER.finest("Continue with normal workflow.");
        return connectFunc.call();
      }
      return candidateConn;
    }

    if (type == RdsUrlType.RDS_WRITER_CLUSTER
        || type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == HostRoleVerification.WRITER) {
      Connection writerCandidateConn = this.getVerifiedWriterConnection(type, props, isInitialConnection, connectFunc);
      if (writerCandidateConn == null) {
        // Can't get writer connection. Continue with a normal workflow.
        return connectFunc.call();
      }
      return writerCandidateConn;
    }

    if (type == RdsUrlType.RDS_READER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == HostRoleVerification.READER) {
      Connection readerCandidateConn =
          this.getVerifiedReaderConnection(type, hostSpec, props, isInitialConnection, connectFunc);
      if (readerCandidateConn == null) {
        // Can't get a reader connection. Continue with a normal workflow.
        LOGGER.finest("Continue with normal workflow.");
        return connectFunc.call();
      }
      return readerCandidateConn;
    }

    // Continue with a normal workflow.
    return connectFunc.call();
  }

  private Connection getVerifiedWriterConnection(
      final RdsUrlType rdsUrlType,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;

    final HostRoleVerification hostRoleVerification =
        this.verifyOpenedConnectionType == null
            && (rdsUrlType == RdsUrlType.RDS_WRITER_CLUSTER || rdsUrlType == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER)
        ? HostRoleVerification.WRITER
        : this.verifyOpenedConnectionType;

    Connection writerCandidateConn;
    HostSpec writerCandidate;

    while (this.getTime() < endTimeNano) {

      writerCandidateConn = null;
      writerCandidate = null;

      try {
        writerCandidate = Utils.getWriter(this.pluginService.getAllHosts());

        if (writerCandidate == null
            || this.rdsUtils.isRdsClusterDns(writerCandidate.getHost())
            || this.rdsUtils.isGlobalDbWriterClusterDns(writerCandidate.getHost())) {

          // Writer is not found. It seems that topology is outdated.
          writerCandidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList();
          writerCandidate = this.pluginService.identifyConnection(writerCandidateConn);

          if (writerCandidate == null || writerCandidate.getRole() != HostRole.WRITER) {
            // Shouldn't be here. But let's try again.
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

        if (hostRoleVerification != HostRoleVerification.NO_VERIFICATION
            && this.pluginService.getHostRole(writerCandidateConn) != HostRole.WRITER) {

          // If the new connection resolves to a reader instance, this means the topology is outdated.
          // Force refresh to update the topology.
          this.pluginService.forceRefreshHostList();
          this.closeConnection(writerCandidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        // Writer connection is valid and verified.
        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
        }
        return writerCandidateConn;

      } catch (SQLException ex) {
        this.closeConnection(writerCandidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        } else {
          if (writerCandidate != null) {
            this.pluginService.setAvailability(writerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
          }

          if (this.pluginService.isNetworkException(ex, this.pluginService.getTargetDriverDialect())
              || this.pluginService.isReadOnlyConnectionException(ex, this.pluginService.getTargetDriverDialect())) {
            // Retry connection.
            continue;
          }

          throw ex;
        }
      } catch (Throwable ex) {
        this.closeConnection(writerCandidateConn);
        throw ex;
      }
    }

    return null;
  }

  private Connection getVerifiedReaderConnection(
      final RdsUrlType rdsUrlType,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;

    final HostRoleVerification hostRoleVerification =
        this.verifyOpenedConnectionType == null && rdsUrlType == RdsUrlType.RDS_READER_CLUSTER
            ? HostRoleVerification.READER
            : this.verifyOpenedConnectionType;

    final HostRole hostRole = HostRoleVerification.convertToHostRole(hostRoleVerification);

    Connection readerCandidateConn;
    HostSpec readerCandidate;
    final String awsRegion = rdsUrlType == RdsUrlType.RDS_READER_CLUSTER
        ? this.rdsUtils.getRdsRegion(hostSpec.getHost())
        : null;

    while (this.getTime() < endTimeNano) {

      readerCandidateConn = null;
      readerCandidate = null;

      try {
        readerCandidate = this.getCandidateHost(props, hostRole, awsRegion);

        if (readerCandidate == null || this.rdsUtils.isRdsClusterDns(readerCandidate.getHost())) {

          // Reader is not found. It seems that topology is outdated.
          readerCandidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList();
          readerCandidate = this.pluginService.identifyConnection(readerCandidateConn);

          if (readerCandidate == null) {
            this.closeConnection(readerCandidateConn);
            this.delay(this.retryDelayMs);
            continue;
          }

          if (readerCandidate.getRole() != hostRole) {
            if (this.hasNoReaders()) {
              // It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
              // and return the current (writer) connection.
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

        if (hostRoleVerification != HostRoleVerification.NO_VERIFICATION
            && this.pluginService.getHostRole(readerCandidateConn) != hostRole) {

          // If the new connection resolves to a writer instance, this means the topology is outdated.
          // Force refresh to update the topology.
          this.pluginService.forceRefreshHostList();

          if (this.hasNoReaders()) {
            // It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
            // and return the current (writer) connection.
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
            }
            return readerCandidateConn;
          }

          this.closeConnection(readerCandidateConn);
          this.delay(this.retryDelayMs);
          continue;
        }

        // Reader connection is valid and verified.
        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
        }
        return readerCandidateConn;

      } catch (SQLException ex) {
        this.closeConnection(readerCandidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        } else {
          if (readerCandidate != null) {
            this.pluginService.setAvailability(readerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
          }
        }
      } catch (Throwable ex) {
        this.closeConnection(readerCandidateConn);
        throw ex;
      }
    }

    return null;
  }

  private Connection getVerifiedConnection(
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = this.getTime() + this.openConnectionRetryTimeoutNano;

    final HostRole hostRole = HostRoleVerification.convertToHostRole(this.verifyOpenedConnectionType);

    Connection candidateConn;
    HostSpec candidateHost;

    while (this.getTime() < endTimeNano) {

      candidateConn = null;
      candidateHost = null;

      try {
        candidateHost = this.getCandidateHost(props, hostRole, null);

        if (candidateHost == null || this.rdsUtils.isRdsClusterDns(candidateHost.getHost())) {

          // Reader is not found. It seems that topology is outdated.
          candidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList();
          candidateHost = this.pluginService.identifyConnection(candidateConn);

          if (candidateHost == null) {
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

        // Verify connection if requested
        if (this.verifyOpenedConnectionType != null
            && this.pluginService.getHostRole(candidateConn) != hostRole) {
          // Force refresh to update the topology.
          this.pluginService.forceRefreshHostList();

          if (hostRole == HostRole.READER && this.hasNoReaders()) {
            // It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
            // and return the current (writer) connection.
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

    // New configuration parameter HOST_SELECTOR_STRATEGY overrides deprecated READER_HOST_SELECTOR_STRATEGY.
    String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    if (props.containsKey(HOST_SELECTOR_STRATEGY.name)) {
      strategy = HOST_SELECTOR_STRATEGY.getString(props);
    }

    if (this.pluginService.acceptsStrategy(hostRole, strategy)) {
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
        // host isn't found
        return null;
      }
    }

    throw new UnsupportedOperationException(
        Messages.get(
            "AuroraInitialConnectionStrategyPlugin.unsupportedStrategy",
            new Object[] {strategy}));
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

      // Found a reader node
      return false;
    }

    // Went through all hosts and found no reader.
    return true;
  }

  // Method implemented to simplify unit testing.
  protected long getTime() {
    return System.nanoTime();
  }

  private enum HostRoleVerification {
    NO_VERIFICATION,
    WRITER,
    READER;

    private static final Map<String, HostRoleVerification> nameToVerifyConnectionTypeValue =
        // Does not map to UNKNOWN, as is not a valid verification type option.
        new HashMap<String, HostRoleVerification>() {
          {
            put("writer", WRITER);
            put("reader", READER);
            put("no", NO_VERIFICATION);
          }
        };

    public static HostRoleVerification verifyConnectionTypeFromValue(String value) {
      if (value == null) {
        return null;
      }
      return nameToVerifyConnectionTypeValue.get(value.toLowerCase());
    }

    public static HostRole convertToHostRole(HostRoleVerification hostRoleVerification) {
      HostRole hostRole = null;
      if (hostRoleVerification == null) {
        hostRoleVerification = HostRoleVerification.NO_VERIFICATION;
      }
      switch (hostRoleVerification) {
        case WRITER:
          hostRole = HostRole.WRITER;
          break;
        case READER:
          hostRole = HostRole.READER;
          break;
        default:
          break;
      }
      return hostRole;
    }
  }

}
