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

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.XaFailoverNotSupportedSQLException;
import software.amazon.jdbc.plugin.failover2.FailoverConnectionPlugin;
import software.amazon.jdbc.util.AccessibleRegions;
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
  protected @Nullable Set<String> accessibleRegions;

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

    String resolvedHomeRegion = FAILOVER_HOME_REGION.getString(properties);
    if (StringUtils.isNullOrEmpty(resolvedHomeRegion)) {
      if (!this.rdsUrlType.hasRegion()) {
        throw new SQLException(Messages.get("GlobalDbFailoverConnectionPlugin.missingHomeRegion"));
      }
      resolvedHomeRegion = this.rdsHelper.getRdsRegion(initialHostSpec.getHost());
      if (StringUtils.isNullOrEmpty(resolvedHomeRegion)) {
        throw new SQLException(Messages.get("GlobalDbFailoverConnectionPlugin.missingHomeRegion"));
      }
    }
    // resolvedHomeRegion is guaranteed non-null here (StringUtils.isNullOrEmpty carries @EnsuresNonNullIf).
    this.homeRegion = resolvedHomeRegion;

    LOGGER.finer(
        () -> Messages.get(
            "Failover.parameterValue",
            new Object[]{"failoverHomeRegion", this.homeRegion}));

    final Set<String> parsedRegions = AccessibleRegions.parse(this.properties);
    this.accessibleRegions = parsedRegions;
    if (parsedRegions != null) {
      LOGGER.finer(
          () -> Messages.get(
              "Failover.parameterValue",
              new Object[]{"gdbAccessibleRegions", parsedRegions}));
    }

    if (parsedRegions != null
        && !parsedRegions.contains(this.homeRegion.toLowerCase(Locale.ROOT))) {
      throw new SQLException(Messages.get(
          "GlobalDbFailoverConnectionPlugin.homeRegionNotInAccessibleRegions",
          new Object[]{this.homeRegion, this.accessibleRegions}));
    }

    GlobalDbFailoverMode resolvedActiveMode = GlobalDbFailoverMode.fromValue(
        ACTIVE_HOME_FAILOVER_MODE.getString(this.properties));
    if (resolvedActiveMode == null) {
      switch (this.rdsUrlType) {
        case RDS_WRITER_CLUSTER:
        case RDS_GLOBAL_WRITER_CLUSTER:
          resolvedActiveMode = GlobalDbFailoverMode.STRICT_WRITER;
          break;
        default:
          resolvedActiveMode = GlobalDbFailoverMode.HOME_READER_OR_WRITER;
      }
    }
    this.activeHomeFailoverMode = resolvedActiveMode;

    GlobalDbFailoverMode resolvedInactiveMode = GlobalDbFailoverMode.fromValue(
        INACTIVE_HOME_FAILOVER_MODE.getString(this.properties));
    if (resolvedInactiveMode == null) {
      switch (this.rdsUrlType) {
        case RDS_WRITER_CLUSTER:
        case RDS_GLOBAL_WRITER_CLUSTER:
          resolvedInactiveMode = GlobalDbFailoverMode.STRICT_WRITER;
          break;
        default:
          resolvedInactiveMode = GlobalDbFailoverMode.HOME_READER_OR_WRITER;
      }
    }
    this.inactiveHomeFailoverMode = resolvedInactiveMode;

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
  protected boolean isStrictWriterFailoverMode() {
    final String currentRegion = this.rdsHelper.getRdsRegion(this.pluginService.getCurrentHostSpec().getHost());
    final boolean isHomeRegion = this.homeRegion.equalsIgnoreCase(currentRegion);
    return isHomeRegion ? this.activeHomeFailoverMode == GlobalDbFailoverMode.STRICT_WRITER :
        this.inactiveHomeFailoverMode == GlobalDbFailoverMode.STRICT_WRITER;
  }

  @Override
  protected void failover() throws SQLException {
    // Failover cannot be skipped (the current connection is broken) and cannot preserve an XA
    // branch. Fail fast so the transaction manager rolls the branch back.
    if (this.pluginService.isXaTransactionActive()) {
      throw new XaFailoverNotSupportedSQLException();
    }

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

      // A Global Database switchover can move the writer into or out of the home region while this failover
      // is still reconnecting, for example while it waits for a home reader that is restarting. The failover
      // mode depends on the writer region, so an attempt that outlives such a change is abandoned and the mode
      // is chosen again from the current topology.
      boolean attemptAbandoned = false;
      while (true) {
        final List<HostSpec> updatedHosts = this.pluginService.getAllHosts();
        final HostSpec writerCandidate = Utils.getWriter(updatedHosts);

        if (writerCandidate == null) {
          if (attemptAbandoned && System.nanoTime() < failoverEndNano) {
            // The switchover that abandoned the previous attempt may still be in progress, and the topology can
            // briefly have no writer. Wait for one rather than failing the failover.
            this.waitForTopologyChange();
            continue;
          }
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

        // Abandons the attempt below as soon as the writer moves into or out of the home region. When both modes
        // are the same, a region change can't change the outcome, so the attempt is never abandoned.
        final BooleanSupplier primaryRegionChanged = this.activeHomeFailoverMode == this.inactiveHomeFailoverMode
            ? null
            : () -> {
              final Boolean writerInHomeRegion = this.isWriterInHomeRegion();
              return writerInHomeRegion != null && writerInHomeRegion != isHomeRegion;
            };

        switch (currentFailoverMode) {
          case STRICT_WRITER:
            if (!isInAccessibleRegion(writerCandidate)) {
              if (this.failoverWriterTriggeredCounter != null) {
                this.failoverWriterTriggeredCounter.inc();
              }
              if (this.failoverWriterFailedCounter != null) {
                this.failoverWriterFailedCounter.inc();
              }
              String message = Messages.get(
                  "GlobalDbFailoverConnectionPlugin.writerNotInAccessibleRegion",
                  new Object[]{writerRegion, this.accessibleRegions});
              LOGGER.severe(message);
              throw new FailoverFailedSQLException(message);
            }
            this.failoverToWriter(writerCandidate, failoverEndNano, primaryRegionChanged);
            break;
          case STRICT_HOME_READER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(x -> {
                      final String region = this.rdsHelper.getRdsRegion(x.getHost());
                      return x.getRole() == HostRole.READER
                          && region != null && region.equalsIgnoreCase(this.homeRegion);
                    })
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                HostRole.READER,
                failoverEndNano,
                primaryRegionChanged);
            break;
          case STRICT_OUT_OF_HOME_READER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(x -> {
                      final String region = this.rdsHelper.getRdsRegion(x.getHost());
                      return x.getRole() == HostRole.READER
                          && region != null && !region.equalsIgnoreCase(this.homeRegion);
                    })
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                HostRole.READER,
                failoverEndNano,
                primaryRegionChanged);
            break;
          case STRICT_ANY_READER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(x -> x.getRole() == HostRole.READER)
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                HostRole.READER,
                failoverEndNano,
                primaryRegionChanged);
            break;
          case HOME_READER_OR_WRITER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(x -> {
                      final String region = this.rdsHelper.getRdsRegion(x.getHost());
                      return x.getRole() == HostRole.WRITER
                          || (x.getRole() == HostRole.READER
                              && region != null && region.equalsIgnoreCase(this.homeRegion));
                    })
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                null,
                failoverEndNano,
                primaryRegionChanged);
            break;
          case OUT_OF_HOME_READER_OR_WRITER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(x -> {
                      final String region = this.rdsHelper.getRdsRegion(x.getHost());
                      return x.getRole() == HostRole.WRITER
                          || (x.getRole() == HostRole.READER
                              && region != null && !region.equalsIgnoreCase(this.homeRegion));
                    })
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                null,
                failoverEndNano,
                primaryRegionChanged);
            break;
          case ANY_READER_OR_WRITER:
            this.failoverToAllowedHost(
                () -> this.pluginService.getHosts().stream()
                    .filter(this::isInAccessibleRegion)
                    .collect(Collectors.toSet()),
                null,
                failoverEndNano,
                primaryRegionChanged);
            break;
          default:
            throw new UnsupportedOperationException(currentFailoverMode.toString());
        }

        // Every attempt above either throws or returns because the primary region changed.
        attemptAbandoned = true;
        LOGGER.info(() -> Messages.get(
            "GlobalDbFailoverConnectionPlugin.primaryRegionChangedDuringFailover",
            new Object[]{currentFailoverMode}));
        if (System.nanoTime() >= failoverEndNano) {
          LOGGER.severe(Messages.get("GlobalDbFailoverConnectionPlugin.failoverTimeout"));
          throw new FailoverFailedSQLException(Messages.get("GlobalDbFailoverConnectionPlugin.failoverTimeout"));
        }
      }

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

  /**
   * Checks if a host is in one of the accessible regions.
   * If accessibleRegions is null (no restriction), all hosts are considered accessible.
   *
   * @param host the host to check
   * @return true if the host is in an accessible region or if no region restriction is set
   */
  protected boolean isInAccessibleRegion(final HostSpec host) {
    final Set<String> regions = this.accessibleRegions;
    if (regions == null) {
      return true;
    }
    final String region = this.rdsHelper.getRdsRegion(host.getHost());
    return region != null && regions.contains(region.toLowerCase(Locale.ROOT));
  }

  /**
   * Whether the writer in the current topology is in the home region.
   *
   * @return true or false, or null when the topology has no writer or its region can't be determined
   */
  protected @Nullable Boolean isWriterInHomeRegion() {
    final HostSpec writer = Utils.getWriter(this.pluginService.getAllHosts());
    if (writer == null) {
      return null;
    }
    final String writerRegion = this.rdsHelper.getRdsRegion(writer.getHost());
    return writerRegion == null ? null : this.homeRegion.equalsIgnoreCase(writerRegion);
  }

  /**
   * Waits briefly and refreshes the topology. Used while a switchover is in progress and the topology has no
   * writer yet. A failed refresh keeps the current topology.
   */
  protected void waitForTopologyChange() {
    try {
      TimeUnit.MILLISECONDS.sleep(100);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
    try {
      this.pluginService.refreshHostList();
    } catch (SQLException ex) {
      // Keep the current topology.
    }
  }

  protected void failoverToWriter(final HostSpec writerCandidate, final long failoverEndNano)
      throws SQLException {
    this.failoverToWriter(writerCandidate, failoverEndNano, null);
  }

  /**
   * Connects to the writer and throws {@link FailoverSuccessSQLException}, or throws
   * {@link FailoverFailedSQLException}.
   *
   * @param writerCandidate the writer to connect to
   * @param failoverEndNano when the failover times out
   * @param abandonWhen checked while connecting; when it returns true the attempt is abandoned and this method
   *     returns normally. Null to never abandon.
   * @throws SQLException always, unless the attempt was abandoned
   */
  protected void failoverToWriter(
      final HostSpec writerCandidate,
      final long failoverEndNano,
      final @Nullable BooleanSupplier abandonWhen)
      throws SQLException {

    // An abandoned attempt records neither a success nor a failure, so it isn't counted as triggered either.
    boolean abandoned = false;
    RetryUtil.Results result = null;
    try {
      result = this.retryUtil.getWriterConnection(
          this.pluginService,
          this.properties,
          this,
          true,
          failoverEndNano,
          abandonWhen);
      if (result == null) {
        // Abandoned: the caller chooses the failover mode again.
        abandoned = true;
        return;
      }
      this.pluginService.setCurrentConnection(result.getConnection(), result.getHostSpec());
      result = null;
      LOGGER.info(() -> Messages.get("Failover.establishedConnection",
          new Object[] {this.pluginService.getCurrentHostSpec()}));
      throwFailoverSuccessException();

    } catch (FailoverSuccessSQLException ex) {
      if (this.failoverWriterSuccessCounter != null) {
        this.failoverWriterSuccessCounter.inc();
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
      if (!abandoned && this.failoverWriterTriggeredCounter != null) {
        this.failoverWriterTriggeredCounter.inc();
      }
      if (result != null
          && result.getConnection() != null
          && result.getConnection() != this.pluginService.getCurrentConnection()) {
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
    this.failoverToAllowedHost(allowedHosts, verifyRole, failoverEndNano, null);
  }

  /**
   * Connects to one of the allowed hosts and throws {@link FailoverSuccessSQLException}, or throws
   * {@link FailoverFailedSQLException}.
   *
   * @param allowedHosts the hosts to choose from, evaluated again on every retry
   * @param verifyRole the role the new connection must have, or null for any role
   * @param failoverEndNano when the failover times out
   * @param abandonWhen checked while connecting; when it returns true the attempt is abandoned and this method
   *     returns normally. Null to never abandon.
   * @throws SQLException always, unless the attempt was abandoned
   */
  protected void failoverToAllowedHost(
      final @NonNull Supplier<Set<HostSpec>> allowedHosts,
      @Nullable HostRole verifyRole,
      final long failoverEndNano,
      final @Nullable BooleanSupplier abandonWhen)
      throws SQLException {

    // An abandoned attempt records neither a success nor a failure, so it isn't counted as triggered either.
    boolean abandoned = false;
    RetryUtil.Results result = null;
    try {
      result = this.retryUtil.getAllowedConnection(
          this.pluginService,
          this.properties,
          this,
          allowedHosts,
          this.failoverReaderHostSelectorStrategySetting,
          verifyRole,
          failoverEndNano,
          abandonWhen);
      if (result == null) {
        // Abandoned: the caller chooses the failover mode again.
        abandoned = true;
        return;
      }
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
      if (!abandoned && this.failoverReaderTriggeredCounter != null) {
        this.failoverReaderTriggeredCounter.inc();
      }
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
