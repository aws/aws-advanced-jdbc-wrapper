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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
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

  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "readerInitialConnectionHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host while opening a new connection.");

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
          "Force to verify an opened connection to be either a writer or a reader.");

  private enum VerifyOpenedConnectionType {
    WRITER,
    READER;

    private static final Map<String, VerifyOpenedConnectionType> nameToValue =
        new HashMap<String, VerifyOpenedConnectionType>() {
          {
            put("writer", WRITER);
            put("reader", READER);
          }
        };

    public static VerifyOpenedConnectionType fromValue(String value) {
      if (value == null) {
        return null;
      }
      return nameToValue.get(value.toLowerCase());
    }
  }

  private final PluginService pluginService;
  private HostListProviderService hostListProviderService;
  private final RdsUtils rdsUtils = new RdsUtils();

  private VerifyOpenedConnectionType verifyOpenedConnectionType = null;

  static {
    PropertyDefinition.registerPluginProperties(AuroraInitialConnectionStrategyPlugin.class);
  }

  public AuroraInitialConnectionStrategyPlugin(final PluginService pluginService, final Properties properties) {
    this.pluginService = pluginService;
    this.verifyOpenedConnectionType =
        VerifyOpenedConnectionType.fromValue(VERIFY_OPENED_CONNECTION_TYPE.getString(properties));
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

    final RdsUrlType type = this.rdsUtils.identifyRdsType(initialUrl);
    this.hostListProviderService = hostListProviderService;
    if ((type.isRdsCluster() || this.verifyOpenedConnectionType != null)
        && hostListProviderService.isStaticHostListProvider()) {
      throw new SQLException(Messages.get("AuroraInitialConnectionStrategyPlugin.requireDynamicProvider"));
    }
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

    if (type == RdsUrlType.RDS_WRITER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.WRITER) {
      Connection writerCandidateConn = this.getVerifiedWriterConnection(props, isInitialConnection, connectFunc);
      if (writerCandidateConn == null) {
        // Can't get writer connection. Continue with a normal workflow.
        return connectFunc.call();
      }
      return writerCandidateConn;
    }

    if (type == RdsUrlType.RDS_READER_CLUSTER
        || isInitialConnection && this.verifyOpenedConnectionType == VerifyOpenedConnectionType.READER) {
      Connection readerCandidateConn = this.getVerifiedReaderConnection(props, isInitialConnection, connectFunc);
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
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final int retryDelayMs = OPEN_CONNECTION_RETRY_INTERVAL_MS.getInteger(props);

    final long endTimeNano = this.getTime()
        + TimeUnit.MILLISECONDS.toNanos(OPEN_CONNECTION_RETRY_TIMEOUT_MS.getInteger(props));

    Connection writerCandidateConn;
    HostSpec writerCandidate;

    while (this.getTime() < endTimeNano) {

      writerCandidateConn = null;
      writerCandidate = null;

      try {
        writerCandidate = this.getWriter();

        if (writerCandidate == null || this.rdsUtils.isRdsClusterDns(writerCandidate.getHost())) {

          // Writer is not found. It seems that topology is outdated.
          writerCandidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList(writerCandidateConn);
          writerCandidate = this.pluginService.identifyConnection(writerCandidateConn);

          if (writerCandidate.getRole() != HostRole.WRITER) {
            // Shouldn't be here. But let's try again.
            this.closeConnection(writerCandidateConn);
            this.delay(retryDelayMs);
            continue;
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
          }
          return writerCandidateConn;
        }

        writerCandidateConn = this.pluginService.connect(writerCandidate, props, this);

        if (this.pluginService.getHostRole(writerCandidateConn) != HostRole.WRITER) {
          // If the new connection resolves to a reader instance, this means the topology is outdated.
          // Force refresh to update the topology.
          this.pluginService.forceRefreshHostList(writerCandidateConn);
          this.closeConnection(writerCandidateConn);
          this.delay(retryDelayMs);
          continue;
        }

        // Writer connection is valid and verified.
        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(writerCandidate);
        }
        return writerCandidateConn;

      } catch (SQLException ex) {
        this.closeConnection(writerCandidateConn);
        if (this.pluginService.isLoginException(ex)) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        } else {
          if (writerCandidate != null) {
            this.pluginService.setAvailability(writerCandidate.asAliases(), HostAvailability.NOT_AVAILABLE);
          }
        }
      } catch (Throwable ex) {
        this.closeConnection(writerCandidateConn);
        throw ex;
      }
    }

    return null;
  }

  private Connection getVerifiedReaderConnection(
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final int retryDelayMs = OPEN_CONNECTION_RETRY_INTERVAL_MS.getInteger(props);

    final long endTimeNano = this.getTime()
        + TimeUnit.MILLISECONDS.toNanos(OPEN_CONNECTION_RETRY_TIMEOUT_MS.getInteger(props));

    Connection readerCandidateConn;
    HostSpec readerCandidate;

    while (this.getTime() < endTimeNano) {

      readerCandidateConn = null;
      readerCandidate = null;

      try {
        readerCandidate = this.getReader(props);

        if (readerCandidate == null || this.rdsUtils.isRdsClusterDns(readerCandidate.getHost())) {

          // Reader is not found. It seems that topology is outdated.
          readerCandidateConn = connectFunc.call();
          this.pluginService.forceRefreshHostList(readerCandidateConn);
          readerCandidate = this.pluginService.identifyConnection(readerCandidateConn);

          if (readerCandidate.getRole() != HostRole.READER) {
            if (this.hasNoReaders()) {
              // It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
              // and return the current (writer) connection.
              if (isInitialConnection) {
                hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
              }
              return readerCandidateConn;
            }
            this.closeConnection(readerCandidateConn);
            this.delay(retryDelayMs);
            continue;
          }

          if (isInitialConnection) {
            hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
          }
          return readerCandidateConn;
        }

        readerCandidateConn = this.pluginService.connect(readerCandidate, props, this);

        if (this.pluginService.getHostRole(readerCandidateConn) != HostRole.READER) {
          // If the new connection resolves to a writer instance, this means the topology is outdated.
          // Force refresh to update the topology.
          this.pluginService.forceRefreshHostList(readerCandidateConn);

          if (this.hasNoReaders()) {
            // It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
            // and return the current (writer) connection.
            if (isInitialConnection) {
              hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
            }
            return readerCandidateConn;
          }

          this.closeConnection(readerCandidateConn);
          this.delay(retryDelayMs);
          continue;
        }

        // Reader connection is valid and verified.
        if (isInitialConnection) {
          hostListProviderService.setInitialConnectionHostSpec(readerCandidate);
        }
        return readerCandidateConn;

      } catch (SQLException ex) {
        this.closeConnection(readerCandidateConn);
        if (this.pluginService.isLoginException(ex)) {
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

  private HostSpec getWriter() {
    for (final HostSpec host : this.pluginService.getAllHosts()) {
      if (host.getRole() == HostRole.WRITER) {
        return host;
      }
    }
    return null;
  }

  private HostSpec getReader(final Properties props) throws SQLException {

    final String strategy = READER_HOST_SELECTOR_STRATEGY.getString(props);
    if (this.pluginService.acceptsStrategy(HostRole.READER, strategy)) {
      try {
        return this.pluginService.getHostSpecByStrategy(HostRole.READER, strategy);
      } catch (UnsupportedOperationException ex) {
        throw ex;
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
}
