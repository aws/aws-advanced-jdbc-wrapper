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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
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
  protected long failoverStartTimeNano = 0;
  protected final AuroraStaleDnsHelper staleDnsHelper;
  protected final TelemetryCounter failoverWriterTriggeredCounter;
  protected final TelemetryCounter failoverWriterSuccessCounter;
  protected final TelemetryCounter failoverWriterFailedCounter;
  protected final TelemetryCounter failoverReaderTriggeredCounter;
  protected final TelemetryCounter failoverReaderSuccessCounter;
  protected final TelemetryCounter failoverReaderFailedCounter;

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
        initialUrl,
        hostListProviderService,
        initHostProviderFunc);
  }

  void initHostProvider(
      final String initialUrl,
      final HostListProviderService hostListProviderService,
      final JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException {
    this.hostListProviderService = hostListProviderService;

    initHostProviderFunc.call();

    this.failoverMode = FailoverMode.fromValue(FAILOVER_MODE.getString(this.properties));
    this.rdsUrlType = this.rdsHelper.identifyRdsType(initialUrl);

    if (this.failoverMode == null) {
      if (this.rdsUrlType.isRdsCluster()) {
        this.failoverMode = (this.rdsUrlType == RdsUrlType.RDS_READER_CLUSTER)
            ? FailoverMode.READER_OR_WRITER
            : FailoverMode.STRICT_WRITER;
      } else {
        this.failoverMode = FailoverMode.STRICT_WRITER;
      }
    }

    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"failoverMode", this.failoverMode}));
  }

  protected boolean isFailoverEnabled() {
    return !RdsUrlType.RDS_PROXY.equals(this.rdsUrlType)
        && !Utils.isNullOrEmpty(this.pluginService.getHosts());
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
   * @param failedHost The host with network errors.
   * @throws SQLException if an error occurs
   */
  protected void failover(final HostSpec failedHost) throws SQLException {
    this.pluginService.setAvailability(failedHost.asAliases(), HostAvailability.NOT_AVAILABLE);

    if (this.failoverMode == FailoverMode.STRICT_WRITER) {
      failoverWriter();
    } else {
      failoverReader();
    }

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

  protected void failoverReader() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_READER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverReaderTriggeredCounter.inc();

    this.failoverStartTimeNano = System.nanoTime();
    final long failoverEndTimeNano = this.failoverStartTimeNano
        + TimeUnit.MILLISECONDS.toNanos(this.failoverTimeoutMsSetting);

    try {
      LOGGER.fine(() -> Messages.get("Failover.startReaderFailover"));

      // It's expected that this method synchronously returns when topology is stabilized,
      // i.e. when cluster control plane has already chosen a new writer.
      if (!this.pluginService.forceRefreshHostList(false, 0)) {
        // "Unable to establish SQL connection to reader instance"
        this.failoverReaderFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToConnectToReader"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToReader"));
      }

      final List<HostSpec> hosts = this.pluginService.getHosts();
      Connection readerCandidateConn = null;
      HostSpec readerCandidate = null;
      final Set<HostSpec> remainingHosts = new HashSet<>(hosts);

      while (!remainingHosts.isEmpty()
          && readerCandidateConn == null
          && System.nanoTime() < failoverEndTimeNano) {
        try {
          readerCandidate =
              this.pluginService.getHostSpecByStrategy(
                  remainingHosts.stream().collect(Collectors.toList()),
                  HostRole.READER,
                  this.failoverReaderHostSelectorStrategySetting);
        } catch (UnsupportedOperationException | SQLException ex) {
          // can't use selected strategy to get a reader host
          LOGGER.finest("Error: " + ex.getMessage());
          break;
        }

        if (readerCandidate == null) {
          break;
        }

        try {
          readerCandidateConn = this.pluginService.connect(readerCandidate, this.properties);
          if (this.pluginService.getHostRole(readerCandidateConn) != HostRole.READER) {
            readerCandidateConn.close();
            readerCandidateConn = null;
            remainingHosts.remove(readerCandidate);
            readerCandidate = null;
          }
        } catch (SQLException ex) {
          remainingHosts.remove(readerCandidate);
          readerCandidateConn = null;
          readerCandidate = null;
        }
      }

      if (readerCandidate == null
          && readerCandidateConn == null
          && this.failoverMode == FailoverMode.READER_OR_WRITER) {
        // As a last resort, let's try to connect to a writer
        try {
          readerCandidate = this.pluginService.getHostSpecByStrategy(HostRole.WRITER,
                  this.failoverReaderHostSelectorStrategySetting);
          if (readerCandidate != null) {
            try {
              readerCandidateConn = this.pluginService.connect(readerCandidate, this.properties);
            } catch (SQLException ex) {
              readerCandidate = null;
            }
          }
        } catch (UnsupportedOperationException ex) {
          // can't use selected strategy to get a reader host
        }
      }

      if (readerCandidateConn == null) {
        // "Unable to establish SQL connection to reader instance"
        this.failoverReaderFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToConnectToReader"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToReader"));
      }

      this.pluginService.setCurrentConnection(readerCandidateConn, readerCandidate);

      LOGGER.info(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));

      this.failoverReaderSuccessCounter.inc();

    } catch (FailoverSuccessSQLException ex) {
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      this.failoverReaderSuccessCounter.inc();
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverReaderFailedCounter.inc();
      throw ex;
    } finally {
      LOGGER.finest(() -> Messages.get(
              "Failover.readerFailoverElapsed",
              new Object[]{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.failoverStartTimeNano)}));
      telemetryContext.closeContext();
      if (this.telemetryFailoverAdditionalTopTraceSetting) {
        telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
      }
    }
  }

  protected void failoverWriter() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_WRITER_FAILOVER, TelemetryTraceLevel.NESTED);
    this.failoverWriterTriggeredCounter.inc();

    this.failoverStartTimeNano = System.nanoTime();

    try {
      LOGGER.info(() -> Messages.get("Failover.startWriterFailover"));

      // It's expected that this method synchronously returns when topology is stabilized,
      // i.e. when cluster control plane has already chosen a new writer.
      if (!this.pluginService.forceRefreshHostList(true, this.failoverTimeoutMsSetting)) {
        // "Unable to establish SQL connection to writer node"
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToConnectToWriter"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToWriter"));
      }

      final List<HostSpec> updatedHosts = this.pluginService.getHosts();

      Connection writerCandidateConn = null;
      final HostSpec writerCandidate = updatedHosts.stream()
          .filter(x -> x.getRole() == HostRole.WRITER)
          .findFirst()
          .orElse(null);

      if (writerCandidate != null) {
        try {
          writerCandidateConn = this.pluginService.connect(writerCandidate, this.properties);
        } catch (SQLException ex) {
          // do nothing
        }
      }

      if (writerCandidateConn == null) {
        // "Unable to establish SQL connection to writer node"
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToConnectToWriter"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToWriter"));
      }

      if (this.pluginService.getHostRole(writerCandidateConn) != HostRole.WRITER) {
        try {
          writerCandidateConn.close();
        } catch (SQLException ex) {
          // do nothing
        }
        this.failoverWriterFailedCounter.inc();
        LOGGER.severe(Messages.get("Failover.unableToConnectToWriter"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToWriter"));
      }

      this.pluginService.setCurrentConnection(writerCandidateConn, writerCandidate);

      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));

      this.failoverWriterSuccessCounter.inc();

    } catch (FailoverSuccessSQLException ex) {
      telemetryContext.setSuccess(true);
      telemetryContext.setException(ex);
      this.failoverWriterSuccessCounter.inc();
      throw ex;
    } catch (Exception ex) {
      telemetryContext.setSuccess(false);
      telemetryContext.setException(ex);
      this.failoverWriterFailedCounter.inc();
      throw ex;
    } finally {
      LOGGER.finest(() -> Messages.get(
          "Failover.writerFailoverElapsed",
          new Object[]{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.failoverStartTimeNano)}));
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
      LOGGER.fine(() -> Messages.get("Failover.transactionResolutionUnknownError"));
      return;
    }

    this.failover(this.pluginService.getCurrentHostSpec());
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(final Throwable t) {

    if (!isFailoverEnabled()) {
      LOGGER.fine(() -> Messages.get("Failover.failoverDisabled"));
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState == null) {
      return false;
    }

    return this.pluginService.isNetworkException(t);
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
}
