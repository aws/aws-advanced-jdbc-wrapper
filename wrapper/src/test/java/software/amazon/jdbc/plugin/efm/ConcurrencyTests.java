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

package software.amazon.jdbc.plugin.efm;

import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_COUNT;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_INTERVAL;
import static software.amazon.jdbc.plugin.efm.HostMonitoringConnectionPlugin.FAILURE_DETECTION_TIME;
import static software.amazon.jdbc.plugin.efm.MonitorServiceImpl.MONITOR_DISPOSAL_TIME_MS;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.AllowedAndBlockedHosts;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.UnknownDialect;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.states.SessionStateService;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

@Disabled
@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
public class ConcurrencyTests {

  @Test
  public void testUsePluginConcurrently_SeparatePluginInstances() throws InterruptedException {

    final Level logLevel = Level.OFF;

    final Logger efmLogger = Logger.getLogger("software.amazon.jdbc.plugin.efm");
    efmLogger.setUseParentHandlers(false);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(logLevel);
    handler.setFormatter(new SimpleFormatter() {
      private static final String format = "[%1$tF %1$tT] [%4$-10s] [%2$-7s] %3$s %n";

      @Override
      public synchronized String format(LogRecord lr) {
        return String.format(format,
            new Date(lr.getMillis()),
            lr.getLevel().getLocalizedName(),
            lr.getMessage(),
            Thread.currentThread().getName()
        );
      }
    });
    efmLogger.addHandler(handler);

    final ClassLoader mainClassLoader = ClassLoader.getSystemClassLoader();
    final ExecutorService executor = Executors.newCachedThreadPool(
        r -> {
          final Thread monitoringThread = new Thread(r);
          monitoringThread.setDaemon(true);
          monitoringThread.setContextClassLoader(mainClassLoader);
          return monitoringThread;
        });

    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("test-host")
        .build();
    hostSpec.addAlias("test-host-alias-a");
    hostSpec.addAlias("test-host-alias-b");

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {

        final Properties properties = new Properties();
        MONITOR_DISPOSAL_TIME_MS.set(properties, "30000");
        FAILURE_DETECTION_TIME.set(properties, "10000");
        FAILURE_DETECTION_INTERVAL.set(properties, "1000");
        FAILURE_DETECTION_COUNT.set(properties, "1");

        final JdbcCallable<ResultSet, SQLException> sqlFunction = () -> {
          try {
            TimeUnit.SECONDS.sleep(5);
          } catch (InterruptedException e) {
            // do nothing
          }
          return null;
        };

        final Connection connection = new TestConnection();
        PluginService pluginService = new TestPluginService(hostSpec, connection);

        final HostMonitoringConnectionPlugin targetPlugin =
            new HostMonitoringConnectionPlugin(pluginService, properties);

        final Logger threadLogger = Logger.getLogger("software.amazon.jdbc.plugin.efm");
        threadLogger.setLevel(logLevel);

        while (!Thread.currentThread().isInterrupted()) {
          try {
            threadLogger.log(Level.FINEST, "Run target plugin execute()");
            targetPlugin.execute(
                ResultSet.class,
                SQLException.class,
                Connection.class,
                "Connection.executeQuery",
                sqlFunction,
                new Object[0]);
          } catch (SQLException e) {
            threadLogger.log(Level.FINEST, "Exception", e);
          }
        }
        threadLogger.log(Level.FINEST, "Stopped.");
      });
    }
    executor.shutdown();

    TimeUnit.SECONDS.sleep(60); // test time

    // cool down
    executor.shutdownNow();
  }

  @Test
  public void testUsePluginConcurrently_SamePluginInstance() throws InterruptedException {

    final Level logLevel = Level.OFF;

    final Logger efmLogger = Logger.getLogger("software.amazon.jdbc.plugin.efm");
    efmLogger.setUseParentHandlers(false);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(logLevel);
    handler.setFormatter(new SimpleFormatter() {
      private static final String format = "[%1$tF %1$tT] [%4$-10s] [%2$-7s] %3$s %n";

      @Override
      public synchronized String format(LogRecord lr) {
        return String.format(format,
            new Date(lr.getMillis()),
            lr.getLevel().getLocalizedName(),
            lr.getMessage(),
            Thread.currentThread().getName()
        );
      }
    });
    efmLogger.addHandler(handler);

    final ClassLoader mainClassLoader = ClassLoader.getSystemClassLoader();
    final ExecutorService executor = Executors.newCachedThreadPool(
        r -> {
          final Thread monitoringThread = new Thread(r);
          monitoringThread.setDaemon(true);
          monitoringThread.setContextClassLoader(mainClassLoader);
          return monitoringThread;
        });

    final HostSpec hostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("test-host")
        .build();
    hostSpec.addAlias("test-host-alias-a");
    hostSpec.addAlias("test-host-alias-b");

    final Properties properties = new Properties();
    MONITOR_DISPOSAL_TIME_MS.set(properties, "30000");
    FAILURE_DETECTION_TIME.set(properties, "10000");
    FAILURE_DETECTION_INTERVAL.set(properties, "1000");
    FAILURE_DETECTION_COUNT.set(properties, "1");

    final JdbcCallable<ResultSet, SQLException> sqlFunction = () -> {
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException e) {
        // do nothing
      }
      return null;
    };

    final Connection connection = new TestConnection();
    final PluginService pluginService = new TestPluginService(hostSpec, connection);

    final HostMonitoringConnectionPlugin targetPlugin =
        new HostMonitoringConnectionPlugin(pluginService, properties);

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {

        final Logger threadLogger = Logger.getLogger("software.amazon.jdbc.plugin.efm");
        threadLogger.setLevel(logLevel);

        while (!Thread.currentThread().isInterrupted()) {
          try {
            threadLogger.log(Level.FINEST, "Run target plugin execute()");
            targetPlugin.execute(
                ResultSet.class,
                SQLException.class,
                Connection.class,
                "Connection.executeQuery",
                sqlFunction,
                new Object[0]);
          } catch (SQLException e) {
            threadLogger.log(Level.FINEST, "Exception", e);
          }
        }
        threadLogger.log(Level.FINEST, "Stopped.");
      });
    }
    executor.shutdown();

    TimeUnit.SECONDS.sleep(60); // test time

    // cool down
    executor.shutdownNow();
  }

  public static class TestSessionStateService implements SessionStateService {

    @Override
    public Optional<Boolean> getAutoCommit() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public void setupPristineAutoCommit() throws SQLException {

    }

    @Override
    public void setupPristineAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public Optional<Boolean> getReadOnly() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public void setupPristineReadOnly() throws SQLException {

    }

    @Override
    public void setupPristineReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public Optional<String> getCatalog() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public void setupPristineCatalog() throws SQLException {

    }

    @Override
    public void setupPristineCatalog(String catalog) throws SQLException {

    }

    @Override
    public Optional<Integer> getHoldability() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public void setupPristineHoldability() throws SQLException {

    }

    @Override
    public void setupPristineHoldability(int holdability) throws SQLException {

    }

    @Override
    public Optional<Integer> getNetworkTimeout() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setNetworkTimeout(int milliseconds) throws SQLException {

    }

    @Override
    public void setupPristineNetworkTimeout() throws SQLException {

    }

    @Override
    public void setupPristineNetworkTimeout(int milliseconds) throws SQLException {

    }

    @Override
    public Optional<String> getSchema() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public void setupPristineSchema() throws SQLException {

    }

    @Override
    public void setupPristineSchema(String schema) throws SQLException {

    }

    @Override
    public Optional<Integer> getTransactionIsolation() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public void setupPristineTransactionIsolation() throws SQLException {

    }

    @Override
    public void setupPristineTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public Optional<Map<String, Class<?>>> getTypeMap() throws SQLException {
      return Optional.empty();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setupPristineTypeMap() throws SQLException {

    }

    @Override
    public void setupPristineTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void reset() {

    }

    @Override
    public void begin() throws SQLException {

    }

    @Override
    public void complete() {

    }

    @Override
    public void applyCurrentSessionState(Connection newConnection) throws SQLException {

    }

    @Override
    public void applyPristineSessionState(Connection connection) throws SQLException {

    }
  }

  public static class TestPluginService implements PluginService {

    private final HostSpec hostSpec;
    private final Connection connection;

    public TestPluginService(HostSpec hostSpec, Connection connection) {
      this.hostSpec = hostSpec;
      this.connection = connection;
    }

    @Override
    public Connection getCurrentConnection() {
      return this.connection;
    }

    @Override
    public HostSpec getCurrentHostSpec() {
      return this.hostSpec;
    }

    @Override
    public void setCurrentConnection(@NonNull Connection connection, @NonNull HostSpec hostSpec)
        throws SQLException {

    }

    @Override
    public EnumSet<NodeChangeOptions> setCurrentConnection(@NonNull Connection connection,
        @NonNull HostSpec hostSpec, @Nullable ConnectionPlugin skipNotificationForThisPlugin)
        throws SQLException {
      return null;
    }

    @Override
    public List<HostSpec> getAllHosts() {
      return null;
    }

    @Override
    public List<HostSpec> getHosts() {
      return null;
    }

    @Override
    public HostSpec getInitialConnectionHostSpec() {
      return null;
    }

    @Override
    public String getOriginalUrl() {
      return null;
    }

    @Override
    public void setAllowedAndBlockedHosts(AllowedAndBlockedHosts allowedAndBlockedHosts) {
    }

    @Override
    public boolean acceptsStrategy(HostRole role, String strategy) {
      return false;
    }

    @Override
    public HostSpec getHostSpecByStrategy(HostRole role, String strategy) {
      return null;
    }

    @Override
    public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy) {
      return null;
    }

    @Override
    public HostRole getHostRole(Connection conn) {
      return null;
    }

    @Override
    public void setAvailability(Set<String> hostAliases, HostAvailability availability) {
    }

    @Override
    public boolean isInTransaction() {
      return false;
    }

    @Override
    public HostListProvider getHostListProvider() {
      return null;
    }

    @Override
    public void refreshHostList() throws SQLException {
    }

    @Override
    public void refreshHostList(Connection connection) throws SQLException {
    }

    @Override
    public void forceRefreshHostList() throws SQLException {
    }

    @Override
    public void forceRefreshHostList(Connection connection) throws SQLException {
    }

    @Override
    public boolean forceRefreshHostList(final boolean shouldVerifyWriter, long timeoutMs)
        throws SQLException {
      return false;
    }

    @Override
    public Connection connect(HostSpec hostSpec, Properties props, @Nullable ConnectionPlugin pluginToSkip)
        throws SQLException {
      return new TestConnection();
    }

    @Override
    public Connection connect(HostSpec hostSpec, Properties props) throws SQLException {
      return this.connect(hostSpec, props, null);
    }

    @Override
    public Connection forceConnect(HostSpec hostSpec, Properties props) throws SQLException {
      return this.forceConnect(hostSpec, props, null);
    }

    @Override
    public Connection forceConnect(HostSpec hostSpec, Properties props, @Nullable ConnectionPlugin pluginToSkip)
        throws SQLException {
      return new TestConnection();
    }

    @Override
    public TelemetryFactory getTelemetryFactory() {
      return null;
    }

    @Override
    public String getTargetName() {
      return null;
    }

    @Override
    public @NonNull SessionStateService getSessionStateService() {
      return new TestSessionStateService();
    }

    @Override
    public <T> T getPlugin(Class<T> pluginClazz) {
      return null;
    }

    @Override
    public boolean isNetworkException(Throwable throwable) {
      return false;
    }

    @Override
    public boolean isNetworkException(Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
      return false;
    }

    @Override
    public boolean isNetworkException(String sqlState) {
      return false;
    }

    @Override
    public boolean isLoginException(String sqlState) {
      return false;
    }

    @Override
    public boolean isLoginException(Throwable throwable) {
      return false;
    }

    @Override
    public boolean isLoginException(Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect) {
      return false;
    }

    @Override
    public Dialect getDialect() {
      return new UnknownDialect();
    }

    @Override
    public TargetDriverDialect getTargetDriverDialect() {
      return new PgTargetDriverDialect();
    }

    public void updateDialect(final @NonNull Connection connection) throws SQLException { }

    @Override
    public HostSpec identifyConnection(Connection connection) throws SQLException {
      return null;
    }

    @Override
    public void fillAliases(Connection connection, HostSpec hostSpec) throws SQLException {

    }

    @Override
    public HostSpecBuilder getHostSpecBuilder() {
      return new HostSpecBuilder(new SimpleHostAvailabilityStrategy());
    }

    @Override
    public ConnectionProvider getConnectionProvider() {
      return null;
    }

    @Override
    public boolean isPooledConnectionProvider(HostSpec host, Properties props) {
      return false;
    }

    @Override
    public String getDriverProtocol() {
      return null;
    }

    @Override
    public Properties getProperties() {
      return null;
    }

    @Override
    public <T> void setStatus(final Class<T> clazz, final @NonNull T status, final boolean clusterBound) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void setStatus(final Class<T> clazz, final @Nullable T status, final String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getStatus(final @NonNull Class<T> clazz, final boolean clusterBound) {
      throw new UnsupportedOperationException();
    }

    public <T> T getStatus(final @NonNull Class<T> clazz, String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPluginInUse(Class<? extends ConnectionPlugin> pluginClazz) {
      return false;
    }
  }

  public static class TestConnection implements Connection {

    @Override
    public Statement createStatement() throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
      return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
      return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
      return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
      return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
      return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
      return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
      return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
      return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
      return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return null;
    }

    @Override
    public Clob createClob() throws SQLException {
      return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
      return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
      return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
      return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
      return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
      return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
    }
  }
}
