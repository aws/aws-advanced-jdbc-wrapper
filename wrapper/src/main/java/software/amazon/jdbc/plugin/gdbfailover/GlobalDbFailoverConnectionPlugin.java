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

package software.amazon.jdbc.plugin.gdbfailover;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.plugin.failover2.ReaderFailoverResult;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.LogUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RetryUtil;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class GlobalDbFailoverConnectionPlugin extends FailoverConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(GlobalDbFailoverConnectionPlugin.class.getName());
  private static final String TELEMETRY_FAILOVER = "failover";

  public static final AwsWrapperProperty ACTIVE_HOME_FAILOVER_MODE =
      new AwsWrapperProperty(
          "activeHomeFailoverMode", null,
          "Set node role to follow during failover when GDB primary region is in home region.",
          false,
          new String[] {
              "strict-writer", "strict-home-reader", "strict-out-of-home-reader", "strict-any-reader",
              "home-reader-or-writer", "out-of-home-reader-or-writer", "any-reader-or-writer"
          });

  public static final AwsWrapperProperty INACTIVE_HOME_FAILOVER_MODE =
      new AwsWrapperProperty(
          "inactiveHomeFailoverMode", null,
          "Set node role to follow during failover when GDB primary region is not in home region.",
          false,
          new String[] {
              "strict-writer", "strict-home-reader", "strict-out-of-home-reader", "strict-any-reader",
              "home-reader-or-writer", "out-of-home-reader-or-writer", "any-reader-or-writer"
          });

  public static final AwsWrapperProperty FAILOVER_HOME_REGION =
      new AwsWrapperProperty(
          "failoverHomeRegion", null,
          "Set home region for failover.");

  static {
    PropertyDefinition.registerPluginProperties(GlobalDbFailoverConnectionPlugin.class);
  }

  protected final RetryUtil retryUtil = new RetryUtil();

  // Inherited failoverMode member should not be used in this class.
  // Use activeHomeFailoverMode and inactiveHomeFailoverMode instead.
  protected GlobalDbFailoverMode activeHomeFailoverMode;
  protected GlobalDbFailoverMode inactiveHomeFailoverMode;

  protected String homeRegion;

  public GlobalDbFailoverConnectionPlugin(final FullServicesContainer servicesContainer,
      Properties properties) {
    super(servicesContainer, properties);
  }

  @Override
  protected void initFailoverMode() throws SQLException {
    if (this.rdsUrlType != null) {
      return;
    }

    final HostSpec initialHostSpec = this.hostListProviderService.getInitialConnectionHostSpec();
    this.rdsUrlType = this.rdsHelper.identifyRdsType(initialHostSpec.getHost());

    this.homeRegion = FAILOVER_HOME_REGION.getString(properties);
    if (StringUtils.isNullOrEmpty(this.homeRegion)) {
      if (!this.rdsUrlType.hasRegion()) {
        throw new SQLException(Messages.get("GlobalDbFailoverConnectionPlugin.missingHomeRegion"));
      }
      this.homeRegion = this.rdsHelper.getRdsRegion(initialHostSpec.getHost());
      if (StringUtils.isNullOrEmpty(this.homeRegion)) {
        throw new SQLException(Messages.get("GlobalDbFailoverConnectionPlugin.missingHomeRegion"));
      }
    }

    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"failoverHomeRegion", this.homeRegion}));

    this.activeHomeFailoverMode = GlobalDbFailoverMode.fromValue(
        ACTIVE_HOME_FAILOVER_MODE.getString(this.properties));
    this.inactiveHomeFailoverMode = GlobalDbFailoverMode.fromValue(
        INACTIVE_HOME_FAILOVER_MODE.getString(this.properties));

    if (this.activeHomeFailoverMode == null) {
      switch (this.rdsUrlType) {
        case RDS_WRITER_CLUSTER:
        case RDS_GLOBAL_WRITER_CLUSTER:
          this.activeHomeFailoverMode = GlobalDbFailoverMode.STRICT_WRITER;
          break;
        default:
          this.activeHomeFailoverMode = GlobalDbFailoverMode.HOME_READER_OR_WRITER;
      }
    }

    if (this.inactiveHomeFailoverMode == null) {
      switch (this.rdsUrlType) {
        case RDS_WRITER_CLUSTER:
        case RDS_GLOBAL_WRITER_CLUSTER:
          this.inactiveHomeFailoverMode = GlobalDbFailoverMode.STRICT_WRITER;
          break;
        default:
          this.inactiveHomeFailoverMode = GlobalDbFailoverMode.HOME_READER_OR_WRITER;
      }
    }

    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"activeHomeFailoverMode", this.activeHomeFailoverMode}));
    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"inactiveHomeFailoverMode", this.inactiveHomeFailoverMode}));
  }

  @Override
  protected void failover() throws SQLException {
    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_FAILOVER, TelemetryTraceLevel.NESTED);

    final long failoverStartTimeNano = System.nanoTime();
    final long failoverEndNano = failoverStartTimeNano + TimeUnit.MILLISECONDS.toNanos(this.failoverTimeoutMsSetting);

    try {
      LOGGER.info(() -> Messages.get("GlobalDbFailoverConnectionPlugin.startFailover"));

      // It's expected that this method synchronously returns when topology is stabilized,
      // i.e. when cluster control plane has already chosen a new writer.
      if (!this.pluginService.forceRefreshHostList(true, this.failoverTimeoutMsSetting)) {
        // Let's assume it's a writer failover
        if (this.failoverWriterTriggeredCounter != null) {
          this.failoverWriterTriggeredCounter.inc();
        }
        if (this.failoverWriterFailedCounter != null) {
          this.failoverWriterFailedCounter.inc();
        }
        LOGGER.severe(Messages.get("Failover.unableToRefreshHostList"));
        throw new FailoverFailedSQLException(Messages.get("Failover.unableToRefreshHostList"));
      }

      final List<HostSpec> updatedHosts = this.pluginService.getAllHosts();
      final HostSpec writerCandidate = Utils.getWriter(updatedHosts);

      if (writerCandidate == null) {
        if (this.failoverWriterTriggeredCounter != null) {
          this.failoverWriterTriggeredCounter.inc();
        }
        if (this.failoverWriterFailedCounter != null) {
          this.failoverWriterFailedCounter.inc();
        }
        String message = LogUtils.logTopology(updatedHosts, Messages.get("Failover.noWriterHost"));
        LOGGER.severe(message);
        throw new FailoverFailedSQLException(message);
      }

      // Check writer region
      final String writerRegion = this.rdsHelper.getRdsRegion(writerCandidate.getHost());
      final boolean isHomeRegion = this.homeRegion.equalsIgnoreCase(writerRegion);
      LOGGER.finest(() ->
          Messages.get("GlobalDbFailoverConnectionPlugin.isHomeRegion", new Object[]{isHomeRegion}));
      GlobalDbFailoverMode currentFailoverMode = isHomeRegion
          ? this.activeHomeFailoverMode
          : this.inactiveHomeFailoverMode;
      LOGGER.finest(() ->
          Messages.get("GlobalDbFailoverConnectionPlugin.currentFailoverMode", new Object[]{currentFailoverMode}));

      switch (currentFailoverMode) {
        case STRICT_WRITER:
          this.failoverToWriter(writerCandidate, failoverEndNano);
          break;
        case STRICT_HOME_READER:
          this.failoverToAllowedHost(
              () -> this.pluginService.getHosts().stream()
                  .filter(x -> x.getRole() == HostRole.READER
                      && this.rdsHelper.getRdsRegion(x.getHost()).equalsIgnoreCase(this.homeRegion))
                  .collect(Collectors.toSet()),
              HostRole.READER,
              failoverEndNano);
          break;
        case STRICT_OUT_OF_HOME_READER:
          this.failoverToAllowedHost(
              () -> this.pluginService.getHosts().stream()
                  .filter(x -> x.getRole() == HostRole.READER
                      && !this.rdsHelper.getRdsRegion(x.getHost()).equalsIgnoreCase(this.homeRegion))
                  .collect(Collectors.toSet()),
              HostRole.READER,
              failoverEndNano);
          break;
        case STRICT_ANY_READER:
          this.failoverToAllowedHost(
              () -> this.pluginService.getHosts().stream()
                  .filter(x -> x.getRole() == HostRole.READER)
                  .collect(Collectors.toSet()),
              HostRole.READER,
              failoverEndNano);
          break;
        case HOME_READER_OR_WRITER:
          this.failoverToAllowedHost(
              () -> this.pluginService.getHosts().stream()
                  .filter(x -> x.getRole() == HostRole.WRITER
                      || (x.getRole() == HostRole.READER
                          && this.rdsHelper.getRdsRegion(x.getHost()).equalsIgnoreCase(this.homeRegion)))
                  .collect(Collectors.toSet()),
              null,
              failoverEndNano);
          break;
        case OUT_OF_HOME_READER_OR_WRITER:
          this.failoverToAllowedHost(
              () -> this.pluginService.getHosts().stream()
                  .filter(x -> x.getRole() == HostRole.WRITER
                      || (x.getRole() == HostRole.READER
                      && !this.rdsHelper.getRdsRegion(x.getHost()).equalsIgnoreCase(this.homeRegion)))
                  .collect(Collectors.toSet()),
              null,
              failoverEndNano);
          break;
        case ANY_READER_OR_WRITER:
          this.failoverToAllowedHost(
              () -> new HashSet<>(this.pluginService.getHosts()),
              null,
              failoverEndNano);
          break;
        default:
          throw new UnsupportedOperationException(currentFailoverMode.toString());
      }

      LOGGER.fine(
          () -> Messages.get(
              "Failover.establishedConnection",
              new Object[]{this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();

    } catch (FailoverSuccessSQLException ex) {
      if (telemetryContext != null) {
        telemetryContext.setSuccess(true);
        telemetryContext.setException(ex);
      }
      throw ex;
    } catch (Exception ex) {
      if (telemetryContext != null) {
        telemetryContext.setSuccess(false);
        telemetryContext.setException(ex);
      }
      throw ex;
    } finally {
      LOGGER.finest(() -> Messages.get(
          "GlobalDbFailoverConnectionPlugin.failoverElapsed",
          new Object[]{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - failoverStartTimeNano)}));

      if (telemetryContext != null) {
        telemetryContext.closeContext();
        if (this.telemetryFailoverAdditionalTopTraceSetting) {
          telemetryFactory.postCopy(telemetryContext, TelemetryTraceLevel.FORCE_TOP_LEVEL);
        }
      }
    }
  }

  protected void failoverToWriter(final HostSpec writerCandidate, final long failoverEndNano)
      throws SQLException {

    if (this.failoverWriterTriggeredCounter != null) {
      this.failoverWriterTriggeredCounter.inc();
    }

    RetryUtil.Results result = null;
    try {
      result = this.retryUtil.getWriterConnection(
          this.pluginService,
          this.properties,
          this,
          true,
          failoverEndNano);
      this.pluginService.setCurrentConnection(result.getConnection(), result.getHostSpec());
      result = null;
      LOGGER.info(() -> Messages.get("Failover.establishedConnection",
          new Object[] {this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();

    } catch (FailoverSuccessSQLException ex) {
      if (this.failoverWriterFailedCounter != null) {
        this.failoverWriterFailedCounter.inc();
      }
      throw ex;
    } catch (TimeoutException e) {
      if (this.failoverWriterFailedCounter != null) {
        this.failoverWriterFailedCounter.inc();
      }
      LOGGER.severe(Messages.get("Failover.exceptionConnectingToWriter", new Object[]{writerCandidate.getHost()}));
      throw new FailoverFailedSQLException(
          Messages.get("Failover.exceptionConnectingToWriter", new Object[]{writerCandidate.getHost()}));
    } finally {
      if (result != null && result.getConnection() != this.pluginService.getCurrentConnection()) {
        try {
          result.getConnection().close();
        } catch (SQLException ex) {
          // do nothing
        }
      }
    }
  }

  protected void failoverToAllowedHost(
      final @NonNull Supplier<Set<HostSpec>> allowedHosts,
      @Nullable HostRole verifyRole,
      final long failoverEndNano)
      throws SQLException {

    if (this.failoverReaderTriggeredCounter != null) {
      this.failoverReaderTriggeredCounter.inc();
    }

    RetryUtil.Results result = null;
    try {
      result = this.retryUtil.getAllowedConnection(
          this.pluginService,
          this.properties,
          this,
          allowedHosts,
          this.failoverReaderHostSelectorStrategySetting,
          verifyRole,
          failoverEndNano);
      this.pluginService.setCurrentConnection(result.getConnection(), result.getHostSpec());
      result = null;
      LOGGER.info(() -> Messages.get("Failover.establishedConnection",
              new Object[] {this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();

    } catch (FailoverSuccessSQLException ex) {
      if (this.failoverReaderSuccessCounter != null) {
        this.failoverReaderSuccessCounter.inc();
      }
      throw ex;
    } catch (TimeoutException e) {
      if (this.failoverReaderFailedCounter != null) {
        this.failoverReaderFailedCounter.inc();
      }
      LOGGER.severe(Messages.get("Failover.unableToConnectToReader"));
      throw new FailoverFailedSQLException(Messages.get("Failover.unableToConnectToReader"));
    } catch (Exception ex) {
      if (this.failoverReaderFailedCounter != null) {
        this.failoverReaderFailedCounter.inc();
      }
      throw ex;
    } finally {
      if (result != null && result.getConnection() != this.pluginService.getCurrentConnection()) {
        try {
          result.getConnection().close();
        } catch (SQLException ex) {
          // do nothing
        }
      }
    }
  }

  @Override
  protected void failoverReader() throws SQLException {
    // This method should not be used in this class. See failover() method for implementation details.
    throw new UnsupportedOperationException();
  }

  protected void failoverWriter() throws SQLException {
    // This method should not be used in this class. See failover() method for implementation details.
    throw new UnsupportedOperationException();
  }
}
