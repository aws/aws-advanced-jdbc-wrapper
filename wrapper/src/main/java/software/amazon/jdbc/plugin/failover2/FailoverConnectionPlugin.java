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

package software.amazon.jdbc.plugin.failover2;

import static software.amazon.jdbc.plugin.failover.FailoverMode.STRICT_READER;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverMode;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;
import software.amazon.jdbc.plugin.staledns.AuroraStaleDnsHelper;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * Failover Plugin v.2
 * This plugin provides cluster-aware failover features. The plugin switches connections upon
 * detecting communication related exceptions and/or cluster topology changes.
 */
public class FailoverConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(FailoverConnectionPlugin.class.getName());
  private static final String TELEMETRY_WRITER_FAILOVER = "failover to writer node";
  private static final String TELEMETRY_READER_FAILOVER = "failover to replica";

  public static final AwsWrapperProperty FAILOVER_TIMEOUT_MS =
      new AwsWrapperProperty(
          "failoverTimeoutMs",
          "300000",
          "Maximum allowed time for the failover process.");

  public static final AwsWrapperProperty FAILOVER_MODE =
      new AwsWrapperProperty(
          "failoverMode", null,
          "Set node role to follow during failover.");

  public static final AwsWrapperProperty TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE =
      new AwsWrapperProperty(
          "telemetryFailoverAdditionalTopTrace", "false",
          "Post an additional top-level trace for failover process.");

  public static final AwsWrapperProperty FAILOVER_READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "failoverReaderHostSelectorStrategy",
          "random",
          "The strategy that should be used to select a new reader host while opening a new connection.");

  public static final AwsWrapperProperty ENABLE_CONNECT_FAILOVER =
      new AwsWrapperProperty(
          "enableConnectFailover", "false",
          "Enable/disable cluster-aware failover if the initial connection to the database fails due to a "
              + "network exception. Note that this may result in a connection to a different instance in the cluster "
              + "than was specified by the URL.");

  public static final AwsWrapperProperty SKIP_FAILOVER_ON_INTERRUPTED_THREAD =
      new AwsWrapperProperty(
          "skipFailoverOnInterruptedThread", "false",
          "Enable to skip failover if the current thread is interrupted.");

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);

          /* Hikari need to clear warnings on a connection before returning it back to a pool. And a current
          connection might be closed since recent failover so failover plugin needs to handle it. */
          add("Connection.clearWarnings");

          add("connect");
          add("initHostProvider");
        }
      });

  protected static final String METHOD_ABORT = "Connection.abort";
  protected static final String METHOD_CLOSE = "Connection.close";
  protected static final String METHOD_IS_CLOSED = "Connection.isClosed";

  protected final PluginService pluginService;
  protected final Properties properties;
  protected int failoverTimeoutMsSetting;
  protected FailoverMode failoverMode;
  protected boolean telemetryFailoverAdditionalTopTraceSetting;
  protected String failoverReaderHostSelectorStrategySetting;
  protected boolean closedExplicitly = false;
  protected boolean isClosed = false;
  protected final RdsUtils rdsHelper;
  protected Throwable lastExceptionDealtWith = null;
  protected PluginManagerService pluginManagerService;
  protected boolean isInTransaction = false;
  protected RdsUrlType rdsUrlType;
  protected HostListProviderService hostListProviderService;
  protected final AuroraStaleDnsHelper staleDnsHelper;
  protected final TelemetryCounter failoverWriterTriggeredCounter;
  protected final TelemetryCounter failoverWriterSuccessCounter;
  protected final TelemetryCounter failoverWriterFailedCounter;
  protected final TelemetryCounter failoverReaderTriggeredCounter;
  protected final TelemetryCounter failoverReaderSuccessCounter;
  protected final TelemetryCounter failoverReaderFailedCounter;
  protected final boolean skipFailoverOnInterruptedThread;


  static {
    PropertyDefinition.registerPluginProperties(FailoverConnectionPlugin.class);
  }

  public FailoverConnectionPlugin(final PluginService pluginService, final Properties properties) {
    this(pluginService, properties, new RdsUtils());
  }

  FailoverConnectionPlugin(
      final PluginService pluginService,
      final Properties properties,
      final RdsUtils rdsHelper) {
    this.pluginService = pluginService;
    this.properties = properties;
    this.rdsHelper = rdsHelper;

    if (pluginService instanceof PluginManagerService) {
      this.pluginManagerService = (PluginManagerService) pluginService;
    }

    this.staleDnsHelper = new AuroraStaleDnsHelper(this.pluginService);

    this.failoverTimeoutMsSetting = FAILOVER_TIMEOUT_MS.getInteger(this.properties);
    this.telemetryFailoverAdditionalTopTraceSetting =
        TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.getBoolean(this.properties);
    this.failoverReaderHostSelectorStrategySetting =
        FAILOVER_READER_HOST_SELECTOR_STRATEGY.getString(this.properties);
    this.skipFailoverOnInterruptedThread = SKIP_FAILOVER_ON_INTERRUPTED_THREAD.getBoolean(this.properties);

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    this.failoverWriterTriggeredCounter = telemetryFactory.createCounter("writerFailover.triggered.count");
    this.failoverWriterSuccessCounter = telemetryFactory.createCounter("writerFailover.completed.success.count");
    this.failoverWriterFailedCounter = telemetryFactory.createCounter("writerFailover.completed.failed.count");
    this.failoverReaderTriggeredCounter = telemetryFactory.createCounter("readerFailover.triggered.count");
    this.failoverReaderSuccessCounter = telemetryFactory.createCounter("readerFailover.completed.success.count");
    this.failoverReaderFailedCounter = telemetryFactory.createCounter("readerFailover.completed.failed.count");
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    try {
      if (this.pluginService.getCurrentConnection() != null
          && !this.canDirectExecute(methodName)
          && !this.closedExplicitly
          && this.pluginService.getCurrentConnection().isClosed()) {
        this.pickNewConnection();
      }
    } catch (SQLException ex) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
    }

    if (canDirectExecute(methodName)) {
      return jdbcMethodFunc.call();
    }

    if (this.isClosed && !this.allowedOnClosedConnection(methodName)) {
      try {
        this.invalidInvocationOnClosedConnection();
      } catch (final SQLException ex) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    T result = null;

    try {
      result = jdbcMethodFunc.call();
    } catch (final IllegalStateException e) {
      dealWithIllegalStateException(e, exceptionClass);
    } catch (final Exception e) {
      this.dealWithOriginalException(e, null, exceptionClass);
    }

    return result;
  }

  @Override
  public void initHostProvider(
      final String driverProtocol,
      final String initialUrl,
      final Properties properties,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    initHostProvider(
        hostListProviderService,
        initHostProviderFunc);
  }

  void initHostProvider(
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    this.hostListProviderService = hostListProviderService;
    initHostProviderFunc.call();
  }

  protected boolean isFailoverEnabled() {
    return !RdsUrlType.RDS_PROXY.equals(this.rdsUrlType)
        && !Utils.isNullOrEmpty(this.pluginService.getAllHosts());
  }

  protected void invalidInvocationOnClosedConnection() throws SQLException {
    if (!this.closedExplicitly) {
      this.isClosed = false;
      this.pickNewConnection();

      // "The active SQL connection has changed. Please re-configure session state if required."
      LOGGER.info(Messages.get("Failover.connectionChangedError"));
      throw new FailoverSuccessSQLException();
    } else {
      String reason = Messages.get("Failover.noOperationsAfterConnectionClosed");
      throw new SQLException(reason, SqlState.CONNECTION_NOT_OPEN.getState());
    }
  }

  /**
   * Checks if the given method is allowed on closed connections.
   *
   * @param methodName A method name to check.
   * @return true if the given method is allowed on closed connections
   */
  protected boolean allowedOnClosedConnection(final String methodName) {
    TargetDriverDialect dialect = this.pluginService.getTargetDriverDialect();
    return dialect.getAllowedOnConnectionMethodNames().contains(methodName);
  }

  protected <E extends Exception> void dealWithOriginalException(
      final Throwable originalException,
      final Throwable wrapperException,
      final Class<E> exceptionClass) throws E {

    Throwable exceptionToThrow = wrapperException;
    if (originalException != null) {
      LOGGER.finer(() -> Messages.get("Failover.detectedException", new Object[]{originalException.getMessage()}));
      if (this.lastExceptionDealtWith != originalException
          && shouldExceptionTriggerConnectionSwitch(originalException)) {
        this.invalidateCurrentConnection();
        this.pluginService.setAvailability(
            this.pluginService.getCurrentHostSpec().getAliases(), HostAvailability.NOT_AVAILABLE);
        try {
          this.pickNewConnection();
        } catch (final SQLException e) {
          throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, e);
        }
        this.lastExceptionDealtWith = originalException;
      }

      if (originalException instanceof Error) {
        throw (Error) originalException;
      }
      exceptionToThrow = originalException;
    }

    if (exceptionToThrow instanceof Error) {
      throw (Error) exceptionToThrow;
    }

    throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, exceptionToThrow);
  }

  protected <E extends Exception> void dealWithIllegalStateException(
      final IllegalStateException e,
      final Class<E> exceptionClass) throws E {
    dealWithOriginalException(e.getCause(), e, exceptionClass);
  }

  /**
   * Initiates the failover procedure. This process tries to establish a new connection to an
   * instance in the topology.
   *
   * @throws SQLException if an error occurs
   */
  protected void failover() throws SQLException {
    if (this.failoverMode == FailoverMode.STRICT_WRITER) {
      failoverWriter();
    } else {
      failoverReader();
    }
  }

  protected void failoverReader() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_READER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverReaderTriggeredCounter.inc();

    final long failoverStartNano = System.nanoTime();
    final long failoverEndNano = failoverStartNano + TimeUnit.MILLISECONDS.toNanos(this.failoverTimeoutMsSetting);

    try {
      LOGGER.fine(() -> Messages.get("Failover.startReaderFailover"));
      // When we pass a timeout of 0, we inform the plugin service that it should update its topology without waiting
      // for it to get updated, since we do not need updated topology to establish a reader connection.
      if (!this.pluginService.forceRefreshHostList(false, 0)) {
        LOGGER.severe(Messages.get("Failover.failoverReaderUnableToRefreshHostList"));
        throw new FailoverFailedSQLException(Messages.get("Failover.failoverReaderUnableToRefreshHostList"));
      }

      try {
        ReaderFailoverResult result = getReaderFailoverConnection(failoverEndNano);
        this.pluginService.setCurrentConnection(result.getConnection(), result.getHostSpec());
      } catch (TimeoutException e) {
        LOGGER.severe(Messages.get("Failover.unableToConnectToReader"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToReader"));
      }

      LOGGER.info(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[] {this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();
    } catch (FailoverSuccessSQLException ex) {
      this.failoverReaderSuccessCounter.inc();
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverReaderFailedCounter.inc();
      throw ex;
    } finally {
      LOGGER.finest(() -> Messages.get(
              "Failover.readerFailoverElapsed",
              new Object[]{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - failoverStartNano)}));
      telemetryContext.closeContext();
      if (this.telemetryFailoverAdditionalTopTraceSetting) {
        telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
      }
    }
  }

  protected ReaderFailoverResult getReaderFailoverConnection(long failoverEndTimeNano) throws TimeoutException {

    // The roles in this list might not be accurate, depending on whether the new topology has become available yet.
    final List<HostSpec> hosts = this.pluginService.getHosts();
    final Set<HostSpec> readerCandidates = hosts.stream()
        .filter(hostSpec -> HostRole.READER.equals(hostSpec.getRole()))
        .collect(Collectors.toSet());
    final HostSpec originalWriter = hosts.stream()
        .filter(hostSpec -> HostRole.WRITER.equals(hostSpec.getRole()))
        .findFirst()
        .orElse(null);
    boolean isOriginalWriterStillWriter = false;

    do {
      // First, try all original readers
      final Set<HostSpec> remainingReaders = new HashSet<>(readerCandidates);
      while (!remainingReaders.isEmpty() && System.nanoTime() < failoverEndTimeNano) {
        HostSpec readerCandidate;
        try {
          readerCandidate =
              this.pluginService.getHostSpecByStrategy(
                  new ArrayList<>(remainingReaders),
                  HostRole.READER,
                  this.failoverReaderHostSelectorStrategySetting);
        } catch (UnsupportedOperationException | SQLException ex) {
          LOGGER.finest(
              Utils.logTopology(
                  new ArrayList<>(remainingReaders),
                  Messages.get("Failover.errorSelectingReaderHost", new Object[]{ex.getMessage()})));
          break;
        }

        if (readerCandidate == null) {
          LOGGER.finest(
              Utils.logTopology(new ArrayList<>(remainingReaders), Messages.get("Failover.readerCandidateNull")));
          break;
        }

        try {
          Connection candidateConn = this.pluginService.connect(readerCandidate, this.properties, this);
          // Since the roles in the host list might not be accurate, we execute a query to check the instance's role.
          HostRole role = this.pluginService.getHostRole(candidateConn);
          if (role == HostRole.READER || this.failoverMode != STRICT_READER) {
            HostSpec updatedHostSpec = new HostSpec(readerCandidate, role);
            return new ReaderFailoverResult(candidateConn, updatedHostSpec);
          }

          // The role is WRITER or UNKNOWN, and we are in STRICT_READER mode, so the connection is not valid.
          remainingReaders.remove(readerCandidate);
          candidateConn.close();

          if (role == HostRole.WRITER) {
            // The reader candidate is actually a writer, which is not valid when failoverMode is STRICT_READER.
            // We will remove it from the list of reader candidates to avoid retrying it in future iterations.
            readerCandidates.remove(readerCandidate);
          } else {
            LOGGER.fine(
                Messages.get("Failover.strictReaderUnknownHostRole", new Object[]{readerCandidate.getUrl()}));
          }
        } catch (SQLException ex) {
          remainingReaders.remove(readerCandidate);
        }
      }

      // We were not able to connect to any of the original readers. We will try connecting to the original writer,
      // which may have been demoted to a reader.

      if (originalWriter == null || System.nanoTime() > failoverEndTimeNano) {
        // No writer was found in the original topology, or we have timed out.
        continue;
      }

      if (this.failoverMode == STRICT_READER && isOriginalWriterStillWriter) {
        // The original writer has been verified, so it is not valid when in STRICT_READER mode.
        continue;
      }

      // Try the original writer, which may have been demoted to a reader.
      try {
        Connection candidateConn = this.pluginService.connect(originalWriter, this.properties, this);
        HostRole role = this.pluginService.getHostRole(candidateConn);
        if (role == HostRole.READER || this.failoverMode != STRICT_READER) {
          HostSpec updatedHostSpec = new HostSpec(originalWriter, role);
          return new ReaderFailoverResult(candidateConn, updatedHostSpec);
        }

        // The role is WRITER or UNKNOWN, and we are in STRICT_READER mode, so the connection is not valid.
        candidateConn.close();

        if (role == HostRole.WRITER) {
          isOriginalWriterStillWriter = true;
        } else {
          LOGGER.fine(Messages.get("Failover.strictReaderUnknownHostRole", new Object[]{originalWriter.getUrl()}));
        }
      } catch (SQLException ex) {
        LOGGER.fine(Messages.get("Failover.failedReaderConnection", new Object[]{originalWriter.getUrl()}));
      }
    } while (System.nanoTime() < failoverEndTimeNano); // All hosts failed. Keep trying until we hit the timeout.

    throw new TimeoutException(Messages.get("Failover.failoverReaderTimeout"));
  }

  protected void throwFailoverSuccessException() throws SQLException {
    if (isInTransaction || this.pluginService.isInTransaction()) {
      if (this.pluginManagerService != null) {
        this.pluginManagerService.setInTransaction(false);
      }
      // "Transaction resolution unknown. Please re-configure session state if required and try
      // restarting transaction."
      final String errorMessage = Messages.get("Failover.transactionResolutionUnknownError");
      LOGGER.info(errorMessage);
      throw new TransactionStateUnknownSQLException();
    } else {
      // "The active SQL connection has changed due to a connection failure. Please re-configure
      // session state if required. "
      LOGGER.severe(() -> Messages.get("Failover.connectionChangedError"));
      throw new FailoverSuccessSQLException();
    }
  }

  protected void failoverWriter() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_WRITER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverWriterTriggeredCounter.inc();

    long failoverStartTimeNano = System.nanoTime();

    try {
      LOGGER.info(() -> Messages.get("Failover.startWriterFailover"));

      // It's expected that this method synchronously returns when topology is stabilized,
      // i.e. when cluster control plane has already chosen a new writer.
      if (!this.pluginService.forceRefreshHostList(true, this.failoverTimeoutMsSetting)) {
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToRefreshHostList"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToRefreshHostList"));
      }

      final List<HostSpec> updatedHosts = this.pluginService.getAllHosts();

      Connection writerCandidateConn;
      final HostSpec writerCandidate = updatedHosts.stream()
          .filter(x -> x.getRole() == HostRole.WRITER)
          .findFirst()
          .orElse(null);

      if (writerCandidate == null) {
        this.failoverWriterFailedCounter.inc();
        String message = Utils.logTopology(updatedHosts, Messages.get("Failover.noWriterHost"));
        LOGGER.severe(message);
        throw new FailoverFailedSQLException(message);
      }

      final List<HostSpec> allowedHosts = this.pluginService.getHosts();
      if (!Utils.containsUrl(allowedHosts, writerCandidate.getUrl())) {
        this.failoverWriterFailedCounter.inc();
        String topologyString = Utils.logTopology(allowedHosts, "");
        LOGGER.severe(Messages.get("Failover.newWriterNotAllowed",
            new Object[] {writerCandidate.getUrl(), topologyString}));
        throw new FailoverFailedSQLException(
            Messages.get("Failover.newWriterNotAllowed",
                new Object[] {writerCandidate.getUrl(), topologyString}));
      }

      try {
        writerCandidateConn = this.pluginService.connect(writerCandidate, this.properties, this);
      } catch (SQLException ex) {
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(
            Messages.get("Failover.exceptionConnectingToWriter", new Object[]{writerCandidate.getHost()}));
        throw new FailoverFailedSQLException(
            Messages.get("Failover.exceptionConnectingToWriter", new Object[]{writerCandidate.getHost()}), ex);
      }

      HostRole role = this.pluginService.getHostRole(writerCandidateConn);
      if (role != HostRole.WRITER) {
        try {
          writerCandidateConn.close();
        } catch (SQLException ex) {
          // do nothing
        }
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(
            Messages.get("Failover.unexpectedReaderRole", new Object[]{writerCandidate.getHost(), role}));
        throw new FailoverFailedSQLException(
            Messages.get("Failover.unexpectedReaderRole", new Object[]{writerCandidate.getHost(), role}));
      }

      this.pluginService.setCurrentConnection(writerCandidateConn, writerCandidate);

      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();
    } catch (FailoverSuccessSQLException ex) {
      this.failoverWriterSuccessCounter.inc();
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverWriterFailedCounter.inc();
      throw ex;
    } finally {
      LOGGER.finest(() -> Messages.get(
          "Failover.writerFailoverElapsed",
          new Object[]{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - failoverStartTimeNano)}));
      telemetryContext.closeContext();
      if (this.telemetryFailoverAdditionalTopTraceSetting) {
        telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
      }
    }
  }

  protected void invalidateCurrentConnection() {
    final Connection conn = this.pluginService.getCurrentConnection();
    if (conn == null) {
      return;
    }

    if (this.pluginService.isInTransaction()) {
      isInTransaction = this.pluginService.isInTransaction();
      try {
        conn.rollback();
      } catch (final SQLException e) {
        // swallow this exception
      }
    }

    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (final SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  protected void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      LOGGER.fine(() -> Messages.get("Failover.connectionClosedExplicitly"));
      return;
    }

    this.failover();
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(final Throwable t) {

    if (!isFailoverEnabled()) {
      LOGGER.fine(() -> Messages.get("Failover.failoverDisabled"));
      return false;
    }

    if (this.skipFailoverOnInterruptedThread && Thread.currentThread().isInterrupted()) {
      LOGGER.fine(() -> Messages.get("Failover.skipFailoverOnInterruptedThread"));
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState == null) {
      return false;
    }

    return this.pluginService.isNetworkException(t, this.pluginService.getTargetDriverDialect());
  }

  /**
   * Check whether the method provided can be executed directly without the failover functionality.
   *
   * @param methodName The name of the method that is being called
   * @return true if the method can be executed directly; false otherwise.
   */
  protected boolean canDirectExecute(final String methodName) {
    return (methodName.equals(METHOD_CLOSE)
        || methodName.equals(METHOD_IS_CLOSED)
        || methodName.equals(METHOD_ABORT));
  }

  protected void initFailoverMode() {
    if (this.rdsUrlType == null) {
      this.failoverMode = FailoverMode.fromValue(FAILOVER_MODE.getString(this.properties));
      final HostSpec initialHostSpec = this.hostListProviderService.getInitialConnectionHostSpec();
      this.rdsUrlType = this.rdsHelper.identifyRdsType(initialHostSpec.getHost());

      if (this.failoverMode == null) {
        this.failoverMode = this.rdsUrlType == RdsUrlType.RDS_READER_CLUSTER
            ? FailoverMode.READER_OR_WRITER
            : FailoverMode.STRICT_WRITER;
      }

      LOGGER.finer(
          () -> Messages.get(
              "Failover.parameterValue",
              new Object[]{"failoverMode", this.failoverMode}));
    }
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    this.initFailoverMode();

    Connection conn = null;

    if (!ENABLE_CONNECT_FAILOVER.getBoolean(props)) {
      return this.staleDnsHelper.getVerifiedConnection(isInitialConnection, this.hostListProviderService,
            driverProtocol, hostSpec, props, connectFunc);
    }

    final HostSpec hostSpecWithAvailability = this.pluginService.getHosts().stream()
        .filter(x -> x.getHostAndPort().equals(hostSpec.getHostAndPort()))
        .findFirst()
        .orElse(null);

    if (hostSpecWithAvailability == null
        || hostSpecWithAvailability.getAvailability() != HostAvailability.NOT_AVAILABLE) {

      try {
        conn = this.staleDnsHelper.getVerifiedConnection(isInitialConnection, this.hostListProviderService,
            driverProtocol, hostSpec, props, connectFunc);
      } catch (final SQLException e) {
        if (!this.shouldExceptionTriggerConnectionSwitch(e)) {
          throw e;
        }

        this.pluginService.setAvailability(hostSpec.asAliases(), HostAvailability.NOT_AVAILABLE);

        try {
          this.failover();
        } catch (FailoverSuccessSQLException failoverSuccessException) {
          conn = this.pluginService.getCurrentConnection();
        }
      }
    } else {
      try {
        this.pluginService.refreshHostList();
        this.failover();
      } catch (FailoverSuccessSQLException failoverSuccessException) {
        conn = this.pluginService.getCurrentConnection();
      }
    }

    if (conn == null) {
      // This should be unreachable, the above logic will either get a connection successfully or throw an exception.
      throw new SQLException(Messages.get("Failover.unableToConnect"));
    }

    if (isInitialConnection) {
      this.pluginService.refreshHostList(conn);
    }

    return conn;
  }
}
