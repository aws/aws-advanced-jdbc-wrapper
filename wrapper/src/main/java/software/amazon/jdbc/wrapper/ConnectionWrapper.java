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

package software.amazon.jdbc.wrapper;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.DefaultTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

public class ConnectionWrapper implements Connection, CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(ConnectionWrapper.class.getName());

  protected ConnectionPluginManager pluginManager;
  protected TelemetryFactory telemetryFactory;
  protected PluginService pluginService;
  protected HostListProviderService hostListProviderService;

  protected PluginManagerService pluginManagerService;
  protected String targetDriverProtocol; // TODO: consider moving to PluginService
  protected String originalUrl; // TODO: consider moving to PluginService

  protected @Nullable Throwable openConnectionStacktrace;

  public ConnectionWrapper(
      @NonNull final Properties props,
      @NonNull final String url,
      @NonNull final ConnectionProvider connectionProvider)
      throws SQLException {

    if (StringUtils.isNullOrEmpty(url)) {
      throw new IllegalArgumentException("url");
    }

    this.originalUrl = url;
    this.targetDriverProtocol = getProtocol(url);

    final TelemetryFactory telemetryFactory = new DefaultTelemetryFactory(props);
    final ConnectionPluginManager pluginManager =
        new ConnectionPluginManager(connectionProvider, this, telemetryFactory);
    final PluginServiceImpl pluginService = new PluginServiceImpl(pluginManager, props, url, this.targetDriverProtocol);

    init(props, pluginManager, telemetryFactory, pluginService, pluginService, pluginService);

    if (PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.getBoolean(props)) {
      this.openConnectionStacktrace = new Throwable(Messages.get("ConnectionWrapper.unclosedConnectionInstantiated"));
    }
  }

  ConnectionWrapper(
      @NonNull final Properties props,
      @NonNull final String url,
      @NonNull final ConnectionPluginManager connectionPluginManager,
      @NonNull final TelemetryFactory telemetryFactory,
      @NonNull final PluginService pluginService,
      @NonNull final HostListProviderService hostListProviderService,
      @NonNull final PluginManagerService pluginManagerService)
      throws SQLException {

    if (StringUtils.isNullOrEmpty(url)) {
      throw new IllegalArgumentException("url");
    }

    init(props,
        connectionPluginManager, telemetryFactory, pluginService, hostListProviderService, pluginManagerService);
  }

  protected void init(
      final Properties props,
      final ConnectionPluginManager connectionPluginManager,
      final TelemetryFactory telemetryFactory,
      final PluginService pluginService,
      final HostListProviderService hostListProviderService,
      final PluginManagerService pluginManagerService) throws SQLException {
    this.pluginManager = connectionPluginManager;
    this.telemetryFactory = telemetryFactory;
    this.pluginService = pluginService;
    this.hostListProviderService = hostListProviderService;
    this.pluginManagerService = pluginManagerService;

    this.pluginManager.init(this.pluginService, props, pluginManagerService);

    this.hostListProviderService.setHostListProvider(
        new ConnectionStringHostListProvider(props, this.originalUrl, this.hostListProviderService));

    this.pluginManager.initHostProvider(
        this.targetDriverProtocol, this.originalUrl, props, this.hostListProviderService);

    this.pluginService.refreshHostList();

    if (this.pluginService.getCurrentConnection() == null) {
      final Connection conn =
          this.pluginManager.connect(
              this.targetDriverProtocol, this.pluginService.getInitialConnectionHostSpec(), props, true);

      if (conn == null) {
        throw new SQLException(Messages.get("ConnectionWrapper.connectionNotOpen"), SqlState.UNKNOWN_STATE.getState());
      }

      this.pluginService.setCurrentConnection(conn, this.pluginService.getInitialConnectionHostSpec());
    }
  }

  protected String getProtocol(final String url) {
    final int index = url.indexOf("//");
    if (index < 0) {
      throw new IllegalArgumentException(
          Messages.get(
              "ConnectionWrapper.protocolNotFound",
              new Object[] {url}));
    }
    return url.substring(0, index + 2);
  }

  public void releaseResources() {
    this.pluginManager.releaseResources();
    if (this.pluginService instanceof CanReleaseResources) {
      ((CanReleaseResources) this.pluginService).releaseResources();
    }
  }

  @Override
  public void abort(final Executor executor) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.abort",
        () -> {
          this.pluginService.getCurrentConnection().abort(executor);
          this.pluginManagerService.setInTransaction(false);
        },
        executor);
  }

  @Override
  public synchronized void clearWarnings() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.clearWarnings",
        () -> this.pluginService.getCurrentConnection().clearWarnings());
  }

  @Override
  public void close() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.close",
        () -> {
          this.pluginService.getCurrentConnection().close();
          this.openConnectionStacktrace = null;
          this.pluginManagerService.setInTransaction(false);
        });
    this.releaseResources();
  }

  @Override
  public void commit() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.commit",
        () -> {
          this.pluginService.getCurrentConnection().commit();
          this.pluginManagerService.setInTransaction(false);
        });
  }

  @Override
  public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createArrayOf",
        () -> this.pluginService.getCurrentConnection().createArrayOf(typeName, elements),
        typeName,
        elements);
  }

  @Override
  public Blob createBlob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createBlob",
        () -> this.pluginService.getCurrentConnection().createBlob());
  }

  @Override
  public Clob createClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createClob",
        () -> this.pluginService.getCurrentConnection().createClob());
  }

  @Override
  public NClob createNClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createNClob",
        () -> this.pluginService.getCurrentConnection().createNClob());
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        SQLXML.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createSQLXML",
        () -> this.pluginService.getCurrentConnection().createSQLXML());
  }

  @Override
  public Statement createStatement() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Statement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createStatement",
        () -> this.pluginService.getCurrentConnection().createStatement());
  }

  @Override
  public Statement createStatement(final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Statement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createStatement",
        () ->
            this.pluginService
                .getCurrentConnection()
                .createStatement(resultSetType, resultSetConcurrency),
        resultSetType,
        resultSetConcurrency);
  }

  @Override
  public Statement createStatement(
      final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Statement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createStatement",
        () ->
            this.pluginService
                .getCurrentConnection()
                .createStatement(resultSetType, resultSetConcurrency, resultSetHoldability),
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Struct.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.createStruct",
        () -> this.pluginService.getCurrentConnection().createStruct(typeName, attributes),
        typeName,
        attributes);
  }

  @Override
  public void setReadOnly(final boolean readOnly) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setReadOnly",
        () -> {
          this.pluginService.getCurrentConnection().setReadOnly(readOnly);
          this.pluginManagerService.setReadOnly(readOnly);
        },
        readOnly);
  }

  @Override
  public String getCatalog() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getCatalog",
        () -> this.pluginService.getCurrentConnection().getCatalog());
  }

  @Override
  public String getClientInfo(final String name) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getClientInfo",
        () -> this.pluginService.getCurrentConnection().getClientInfo(name),
        name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Properties.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getClientInfo",
        () -> this.pluginService.getCurrentConnection().getClientInfo());
  }

  @Override
  public int getHoldability() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getHoldability",
        () -> this.pluginService.getCurrentConnection().getHoldability());
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        DatabaseMetaData.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getMetaData",
        () -> this.pluginService.getCurrentConnection().getMetaData());
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getNetworkTimeout",
        () -> this.pluginService.getCurrentConnection().getNetworkTimeout());
  }

  @Override
  public String getSchema() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getSchema",
        () -> this.pluginService.getCurrentConnection().getSchema());
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    // noinspection MagicConstant
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getTransactionIsolation",
        () -> this.pluginService.getCurrentConnection().getTransactionIsolation());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // noinspection unchecked
    return WrapperUtils.executeWithPlugins(
        Map.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getTypeMap",
        () -> this.pluginService.getCurrentConnection().getTypeMap());
  }

  @Override
  public synchronized SQLWarning getWarnings() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        SQLWarning.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getWarnings",
        () -> this.pluginService.getCurrentConnection().getWarnings());
  }

  @Override
  public boolean isClosed() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.isClosed",
        () -> this.pluginService.getCurrentConnection().isClosed());
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.isReadOnly",
        () -> this.pluginService.getCurrentConnection().isReadOnly());
  }

  @Override
  public boolean isValid(final int timeout) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.isValid",
        () -> this.pluginService.getCurrentConnection().isValid(timeout),
        timeout);
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return this.pluginService.getCurrentConnection().isWrapperFor(iface);
  }

  @Override
  public String nativeSQL(final String sql) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.nativeSQL",
        () -> this.pluginService.getCurrentConnection().nativeSQL(sql),
        sql);
  }

  @Override
  public CallableStatement prepareCall(final String sql) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        CallableStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareCall",
        () -> this.pluginService.getCurrentConnection().prepareCall(sql),
        sql);
  }

  @Override
  public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        CallableStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareCall",
        () ->
            this.pluginService
                .getCurrentConnection()
                .prepareCall(sql, resultSetType, resultSetConcurrency),
        sql,
        resultSetType,
        resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(
      final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        CallableStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareCall",
        () ->
            this.pluginService
                .getCurrentConnection()
                .prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability),
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () -> this.pluginService.getCurrentConnection().prepareStatement(sql),
        sql);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () ->
            this.pluginService
                .getCurrentConnection()
                .prepareStatement(sql, resultSetType, resultSetConcurrency),
        sql,
        resultSetType,
        resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(
      final String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () ->
            this.pluginService
                .getCurrentConnection()
                .prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability),
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () -> this.pluginService.getCurrentConnection().prepareStatement(sql, autoGeneratedKeys),
        sql,
        autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () -> this.pluginService.getCurrentConnection().prepareStatement(sql, columnIndexes),
        sql,
        columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        PreparedStatement.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.prepareStatement",
        () -> this.pluginService.getCurrentConnection().prepareStatement(sql, columnNames),
        sql,
        columnNames);
  }

  @Override
  public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.releaseSavepoint",
        () -> {
          if (savepoint instanceof SavepointWrapper) {
            this.pluginService.getCurrentConnection().releaseSavepoint(((SavepointWrapper) savepoint).savepoint);
          } else {
            this.pluginService.getCurrentConnection().releaseSavepoint(savepoint);
          }
        },
        savepoint);
  }

  @Override
  public void rollback() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.rollback",
        () -> {
          this.pluginService.getCurrentConnection().rollback();
          this.pluginManagerService.setInTransaction(false);
        });
  }

  @Override
  public void rollback(final Savepoint savepoint) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.rollback",
        () -> {
          if (savepoint instanceof SavepointWrapper) {
            this.pluginService.getCurrentConnection().rollback(((SavepointWrapper) savepoint).savepoint);
          } else {
            this.pluginService.getCurrentConnection().rollback(savepoint);
          }
          this.pluginManagerService.setInTransaction(false);
        },
        savepoint);
  }

  @Override
  public void setAutoCommit(final boolean autoCommit) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setAutoCommit",
        () -> {
          this.pluginService.getCurrentConnection().setAutoCommit(autoCommit);
        },
        autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.getAutoCommit",
        () -> this.pluginService.getCurrentConnection().getAutoCommit());
  }

  @Override
  public void setCatalog(final String catalog) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setCatalog",
        () -> this.pluginService.getCurrentConnection().setCatalog(catalog),
        catalog);
  }

  @Override
  public void setClientInfo(final String name, final String value) throws SQLClientInfoException {
    WrapperUtils.runWithPlugins(
        SQLClientInfoException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setClientInfo",
        () -> this.pluginService.getCurrentConnection().setClientInfo(name, value),
        name,
        value);
  }

  @Override
  public void setClientInfo(final Properties properties) throws SQLClientInfoException {
    WrapperUtils.runWithPlugins(
        SQLClientInfoException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setClientInfo",
        () -> this.pluginService.getCurrentConnection().setClientInfo(properties),
        properties);
  }

  @Override
  public void setHoldability(final int holdability) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setHoldability",
        () -> this.pluginService.getCurrentConnection().setHoldability(holdability),
        holdability);
  }

  @Override
  public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setNetworkTimeout",
        () -> this.pluginService.getCurrentConnection().setNetworkTimeout(executor, milliseconds),
        executor,
        milliseconds);
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Savepoint.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setSavepoint",
        () -> this.pluginService.getCurrentConnection().setSavepoint());
  }

  @Override
  public Savepoint setSavepoint(final String name) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Savepoint.class,
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setSavepoint",
        () -> this.pluginService.getCurrentConnection().setSavepoint(name),
        name);
  }

  @Override
  public void setSchema(final String schema) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setSchema",
        () -> this.pluginService.getCurrentConnection().setSchema(schema),
        schema);
  }

  @Override
  public void setTransactionIsolation(final int level) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setTransactionIsolation",
        () -> this.pluginService.getCurrentConnection().setTransactionIsolation(level),
        level);
  }

  @Override
  public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.pluginService.getCurrentConnection(),
        "Connection.setTypeMap",
        () -> this.pluginService.getCurrentConnection().setTypeMap(map),
        map);
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return this.pluginService.getCurrentConnection().unwrap(iface);
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.pluginService.getCurrentConnection();
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  protected void finalize() throws Throwable {

    try {
      if (this.openConnectionStacktrace != null) {
        LOGGER.log(
            Level.WARNING,
            this.openConnectionStacktrace,
            () -> Messages.get(
                "ConnectionWrapper.finalizingUnclosedConnection"));
        this.openConnectionStacktrace = null;
      }

      this.releaseResources();

    } finally {
      super.finalize();
    }
  }
}
