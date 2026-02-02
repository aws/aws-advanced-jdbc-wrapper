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

package software.amazon.jdbc.plugin.srw;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.AbstractReadWriteSplittingPlugin;
import software.amazon.jdbc.util.CacheItem;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;

public class SimpleReadWriteSplittingPlugin extends AbstractReadWriteSplittingPlugin
    implements CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(SimpleReadWriteSplittingPlugin.class.getName());
  private final RdsUtils rdsUtils = new RdsUtils();
  private final boolean verifyNewConnections;
  private final String writeEndpoint;
  private final String readEndpoint;
  private final HostRole verifyOpenedConnectionType;
  private final int connectRetryIntervalMs;
  private final long connectRetryTimeoutMs;

  public static final AwsWrapperProperty SRW_READ_ENDPOINT =
      new AwsWrapperProperty(
          "srwReadEndpoint",
          null,
          "The read-only endpoint that should be used to connect to a reader.");

  public static final AwsWrapperProperty SRW_WRITE_ENDPOINT =
      new AwsWrapperProperty(
          "srwWriteEndpoint",
          null,
          "The read-write/cluster endpoint that should be used to connect to the writer.");

  public static final AwsWrapperProperty VERIFY_NEW_SRW_CONNECTIONS =
      new AwsWrapperProperty(
          "verifyNewSrwConnections",
          "true",
          "Enables role verification for new connections made by the Simple Read/Write Splitting Plugin.",
          false,
          new String[] {
              "true", "false"
          });

  public static final AwsWrapperProperty SRW_CONNECT_RETRY_TIMEOUT_MS =
      new AwsWrapperProperty(
          "srwConnectRetryTimeoutMs",
          "60000",
          "Maximum allowed time for the retries opening a connection.");

  public static final AwsWrapperProperty SRW_CONNECT_RETRY_INTERVAL_MS =
      new AwsWrapperProperty(
          "srwConnectRetryIntervalMs",
          "1000",
          "Time between each retry of opening a connection.");

  public static final AwsWrapperProperty VERIFY_INITIAL_CONNECTION_TYPE =
      new AwsWrapperProperty(
          "verifyInitialConnectionType",
          null,
          "Force to verify the initial connection to be either a writer or a reader.");

  static {
    PropertyDefinition.registerPluginProperties(
        SimpleReadWriteSplittingPlugin.class);
  }

  public SimpleReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties);
    this.writeEndpoint = SRW_WRITE_ENDPOINT.getString(properties);
    if (StringUtils.isNullOrEmpty(writeEndpoint)) {
      throw new
          RuntimeException(
          Messages.get(
              "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
              new Object[] {SRW_WRITE_ENDPOINT.name}));
    }
    this.readEndpoint = SRW_READ_ENDPOINT.getString(properties);
    if (StringUtils.isNullOrEmpty(readEndpoint)) {
      throw new
          RuntimeException(
          Messages.get(
              "SimpleReadWriteSplittingPlugin.missingRequiredConfigParameter",
              new Object[] {SRW_READ_ENDPOINT.name}));
    }
    this.verifyNewConnections = VERIFY_NEW_SRW_CONNECTIONS.getBoolean(properties);
    this.verifyOpenedConnectionType =
        HostRole.verifyConnectionTypeFromValue(
            VERIFY_INITIAL_CONNECTION_TYPE.getString(properties));
    this.connectRetryIntervalMs = SRW_CONNECT_RETRY_INTERVAL_MS.getInteger(properties);
    this.connectRetryTimeoutMs = SRW_CONNECT_RETRY_TIMEOUT_MS.getInteger(properties);
  }

  /**
   * For testing purposes only.
   */
  public SimpleReadWriteSplittingPlugin(
      final PluginService pluginService,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final Connection writerConnection,
      final Connection readerConnection,
      final HostSpec writeEndpointHostSpec,
      final HostSpec readEndpointHostSpec) {
    this(pluginService, properties);
    this.hostListProviderService = hostListProviderService;
    this.writerConnection = writerConnection;
    this.readerCacheItem = new CacheItem<>(readerConnection, this.getKeepAliveTimeout(false));
    this.writerHostSpec = writeEndpointHostSpec;
    this.readerHostSpec = readEndpointHostSpec;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    if (!isInitialConnection || !this.verifyNewConnections) {
      // No verification required. Continue with a normal workflow.
      return connectFunc.call();
    }

    final RdsUrlType type = this.rdsUtils.identifyRdsType(hostSpec.getHost());

    Connection conn = null;
    if (type == RdsUrlType.RDS_WRITER_CLUSTER
        || type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        || this.verifyOpenedConnectionType == HostRole.WRITER) {
      conn = this.getVerifiedConnection(this.properties, hostSpec, HostRole.WRITER, connectFunc);
    } else if (type == RdsUrlType.RDS_READER_CLUSTER
        || this.verifyOpenedConnectionType == HostRole.READER) {
      conn = this.getVerifiedConnection(this.properties, hostSpec, HostRole.READER, connectFunc);
    }

    if (conn == null) {
      // Continue with a normal workflow.
      conn = connectFunc.call();
    }
    this.setInitialConnectionHostSpec(conn, hostSpec);
    return conn;
  }

  @Override
  protected boolean isWriter(final @NonNull HostSpec hostSpec) {
    return this.writeEndpoint.equalsIgnoreCase(hostSpec.getHost())
        || this.writeEndpoint.equalsIgnoreCase(hostSpec.getHostAndPort());
  }

  @Override
  protected boolean isReader(final @NonNull HostSpec hostSpec) {
    return this.readEndpoint.equalsIgnoreCase(hostSpec.getHost())
        || this.readEndpoint.equalsIgnoreCase(hostSpec.getHostAndPort());
  }

  @Override
  protected void refreshAndStoreTopology(Connection currentConnection) {
    // Simple Read/Write Splitting does not rely on topology.
  }

  private Connection getVerifiedConnection(
      final Properties props,
      final HostSpec hostSpec,
      final HostRole hostRole,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(this.connectRetryTimeoutMs);

    Connection candidateConn;

    while (System.nanoTime() < endTimeNano) {

      candidateConn = null;

      try {
        if (connectFunc != null) {
          candidateConn = connectFunc.call();
        } else if (hostSpec != null) {
          candidateConn = this.pluginService.connect(hostSpec, props, this);
        } else {
          // Unable to verify.
          break;
        }

        if (candidateConn == null || this.pluginService.getHostRole(candidateConn) != hostRole) {
          // The connection does not have the desired role. Retry.
          this.closeConnection(candidateConn);
          this.delay();
          continue;
        }

        // Connection is valid and verified.
        return candidateConn;
      } catch (SQLException ex) {
        this.closeConnection(candidateConn);
        if (this.pluginService.isLoginException(ex, this.pluginService.getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        }
        this.delay();
      } catch (Throwable ex) {
        this.closeConnection(candidateConn);
        throw ex;
      }
    }

    LOGGER.fine(
        () -> Messages.get("SimpleReadWriteSplittingPlugin.verificationFailed",
            new Object[] {hostRole, this.connectRetryTimeoutMs}));
    return null;
  }

  private void setInitialConnectionHostSpec(Connection conn, HostSpec hostSpec) {
    if (hostSpec == null) {
      try {
        hostSpec = this.pluginService.identifyConnection(conn);
      } catch (Exception e) {
        // Ignore error
      }
    }

    if (hostSpec != null && hostListProviderService != null) {
      hostListProviderService.setInitialConnectionHostSpec(hostSpec);
    }
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

  private void delay() {
    try {
      TimeUnit.MILLISECONDS.sleep(this.connectRetryIntervalMs);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void initializeWriterConnection() throws SQLException {
    if (this.writerHostSpec == null) {
      this.writerHostSpec = createHostSpec(this.writeEndpoint, HostRole.WRITER);
    }
    final Connection conn;
    if (this.verifyNewConnections) {
      conn = this.getVerifiedConnection(
          this.properties, this.writerHostSpec, HostRole.WRITER, null);
    } else {
      conn = this.pluginService.connect(this.writerHostSpec, this.properties, this);
    }

    if (conn == null) {
      logAndThrowException(
          Messages.get("SimpleReadWriteSplittingPlugin.failedToConnectToWriter",
              new Object[]{this.writeEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
    }

    setWriterConnection(conn, this.writerHostSpec);
    switchCurrentConnectionTo(this.writerConnection, this.writerHostSpec);
    this.isWriterConnFromInternalPool = Boolean.TRUE.equals(this.pluginService.isPooledConnection());
  }

  @Override
  protected void initializeReaderConnection() throws SQLException {
    if (this.readerHostSpec == null) {
      this.readerHostSpec = createHostSpec(this.readEndpoint, HostRole.READER);
    }
    final Connection conn;

    if (this.verifyNewConnections) {
      conn = this.getVerifiedConnection(
          this.properties, this.readerHostSpec, HostRole.READER, null);
    } else {
      conn = this.pluginService.connect(this.readerHostSpec, this.properties, this);
    }

    if (conn == null) {
      logAndThrowException(Messages.get("ReadWriteSplittingPlugin.failedToConnectToReader",
              new Object[]{this.readEndpoint}),
          SqlState.CONNECTION_UNABLE_TO_CONNECT);
    }

    LOGGER.finest(
        () -> Messages.get("ReadWriteSplittingPlugin.successfullyConnectedToReader",
            new Object[]{readerHostSpec.getHostAndPort()}));

    // Store reader connection for reuse.
    setReaderConnection(conn, readerHostSpec);
    switchCurrentConnectionTo(conn, this.readerHostSpec);
    this.isReaderConnFromInternalPool = Boolean.TRUE.equals(this.pluginService.isPooledConnection());
    LOGGER.finer(() -> Messages.get("ReadWriteSplittingPlugin.switchedFromWriterToReader",
        new Object[] {readEndpoint}));
  }

  private HostSpec createHostSpec(String endpoint, HostRole role) {
    endpoint = endpoint.trim();

    String host = endpoint;
    int port = this.hostListProviderService.getCurrentHostSpec().getPort();
    int colonIndex = endpoint.lastIndexOf(":");
    if (colonIndex != -1 && endpoint.substring(colonIndex + 1).matches("\\d+")) {
      host = endpoint.substring(0, colonIndex);
      port = Integer.parseInt(endpoint.substring(colonIndex + 1));
    }

    return new HostSpecBuilder(this.hostListProviderService.getHostSpecBuilder())
        .host(host)
        .port(port)
        .role(role)
        .availability(HostAvailability.AVAILABLE)
        .build();
  }

  @Override
  protected void closeReaderIfNecessary() {
    // Simple Read/Write will connect to the reader endpoint regardless.
  }

  @Override
  protected boolean shouldUpdateReaderConnection(
      Connection currentConnection, HostSpec currentHost) throws SQLException {
    final Connection cachedReader = this.readerCacheItem == null ? null : this.readerCacheItem.get();
    return isReader(currentHost) && !currentConnection.equals(cachedReader)
        && (!this.verifyNewConnections || this.pluginService.getHostRole(currentConnection) == HostRole.READER);
  }

  @Override
  protected boolean shouldUpdateWriterConnection(
      Connection currentConnection, HostSpec currentHost) throws SQLException {
    return isWriter(currentHost) && !currentConnection.equals(this.writerConnection)
        && (!this.verifyNewConnections || this.pluginService.getHostRole(currentConnection) == HostRole.WRITER);
  }
}
