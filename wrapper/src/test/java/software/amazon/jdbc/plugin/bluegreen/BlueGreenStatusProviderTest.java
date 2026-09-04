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

package software.amazon.jdbc.plugin.bluegreen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.PgTargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.StorageService;

class BlueGreenStatusProviderTest {

  private static final String BGD_ID = "1";
  private static final String CLUSTER_ID = "cluster-id";

  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private PluginService mockPluginService;
  @Mock private StorageService mockStorageService;

  /**
   * A plain dialect that does NOT implement {@link software.amazon.jdbc.dialect.BlueGreenDialect}. The
   * provider constructor only starts its monitor threads for a Blue/Green capable dialect, so this
   * keeps the unit under test free of background threads and real connections.
   */
  @Mock private Dialect mockDialect;

  private Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    when(mockServicesContainer.getPluginService()).thenReturn(mockPluginService);
    when(mockServicesContainer.getStorageService()).thenReturn(mockStorageService);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private BlueGreenStatusProvider newProvider() {
    return new BlueGreenStatusProvider(mockServicesContainer, props, BGD_ID, CLUSTER_ID);
  }

  private BlueGreenInterimStatus interimStatus(final BlueGreenPhase phase) {
    return interimStatus(phase, false);
  }

  private BlueGreenInterimStatus interimStatus(
      final BlueGreenPhase phase, final boolean endpointUnreachable) {
    return new BlueGreenInterimStatus(
        phase,
        "1.0",
        5432,
        new ArrayList<>(),
        new ArrayList<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashSet<>(),
        false,
        false,
        false,
        endpointUnreachable);
  }

  /**
   * An interim status that looks like a monitor which has successfully collected everything: a
   * topology and a host name. Used for the readiness assertions, which require both roles to have
   * reported collected data.
   */
  private BlueGreenInterimStatus collectedInterimStatus(
      final BlueGreenPhase phase, final String host, final boolean endpointUnreachable) {
    return new BlueGreenInterimStatus(
        phase,
        "1.0",
        5432,
        Collections.singletonList(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host(host).port(5432).build()),
        new ArrayList<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashSet<>(Collections.singletonList(host)),
        false,
        false,
        false,
        endpointUnreachable);
  }

  /**
   * A provider with the monitor lifecycle and the clock replaced. {@code initMonitoring} is counted
   * instead of starting real monitor threads, which is what lets the deletion tests assert that the
   * context reset actually restarts monitoring.
   *
   * <p>Note that neither field uses an explicit initializer: {@code initMonitoring()} can be called
   * from the superclass constructor, and a subclass field initializer would run afterwards and wipe
   * the recorded count.
   */
  private static class TestableProvider extends BlueGreenStatusProvider {

    private long nanoTime;
    private int initMonitoringCount;

    TestableProvider(
        final FullServicesContainer servicesContainer,
        final Properties props,
        final String bgdId,
        final String clusterId) {
      super(servicesContainer, props, bgdId, clusterId);
    }

    @Override
    protected void initMonitoring() {
      this.initMonitoringCount++;
    }

    @Override
    protected long getNanoTime() {
      return this.nanoTime;
    }

    void advanceMs(final long ms) {
      this.nanoTime += TimeUnit.MILLISECONDS.toNanos(ms);
    }
  }

  private TestableProvider newTestableProvider() {
    return new TestableProvider(mockServicesContainer, props, BGD_ID, CLUSTER_ID);
  }

  /** Grace period used by the deletion tests, kept short so the intent is obvious. */
  private static final long GRACE_MS = 30_000;

  /**
   * The regression this guards (issue #2096): a target driver property restricting which node may be
   * connected to must not reach the monitoring connections. The green node is a replica until it is
   * promoted, so inheriting {@code targetServerType=primary} stopped the green monitor from ever
   * connecting, and the plugin never observed the switchover finishing.
   */
  @Test
  void monitoringPropertiesDropNodeSelectionRestrictions() {
    props.setProperty("targetServerType", "primary");
    props.setProperty("socketTimeout", "3000");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertNull(monitoringProps.getProperty("targetServerType"));
    assertEquals("3000", monitoringProps.getProperty("socketTimeout"),
        "Unrelated properties must still be inherited by monitoring connections.");
  }

  /**
   * The documented workaround for the above on released versions: the monitoring prefix overrides the
   * inherited value, so the restriction can be neutralized without touching application connections.
   */
  @Test
  void monitoringPrefixOverridesInheritedProperty() {
    props.setProperty("someProperty", "applicationValue");
    props.setProperty("blue-green-monitoring-someProperty", "monitoringValue");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertEquals("monitoringValue", monitoringProps.getProperty("someProperty"));
    assertNull(monitoringProps.getProperty("blue-green-monitoring-someProperty"),
        "The prefixed key itself must not be passed to the target driver.");
  }

  /**
   * Removing inherited restrictions must not defeat an explicit monitoring override. Someone who has
   * deliberately set a monitoring value for a node-selection property means it, so the removal has to
   * happen before the prefixed values are applied, not after.
   */
  @Test
  void explicitMonitoringOverrideSurvivesRemoval() {
    props.setProperty("targetServerType", "primary");
    props.setProperty("blue-green-monitoring-targetServerType", "preferSecondary");
    when(mockPluginService.getTargetDriverDialect()).thenReturn(new PgTargetDriverDialect());

    final Properties monitoringProps = newProvider().getMonitoringProperties();

    assertEquals("preferSecondary", monitoringProps.getProperty("targetServerType"),
        "An explicit monitoring override must win over both the inherited value and the removal.");
  }

  @Test
  void inProgressSuspendsTrafficWhileBlueDnsStillPointsAtOldTopology() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = false;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
    assertFalse(status.getExecuteRouting().isEmpty(),
        "Traffic should still be suspended while the blue endpoints resolve to the old topology.");
  }

  /**
   * The regression this guards: reading a status after SWITCHOVER_IN_PROGRESS is not guaranteed, so
   * once the blue endpoints resolve to new IP addresses the provider must stop holding traffic instead
   * of waiting for the switchover timer to expire.
   */
  @Test
  void inProgressReleasesTrafficOnceBlueDnsResolvesToNewTopology() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.POST, status.getCurrentPhase());
    assertTrue(status.getExecuteRouting().isEmpty(),
        "Held JDBC calls should be released once the blue endpoints point at the new topology.");
  }

  @Test
  void inProgressKeepsSuspendingTrafficWhenReleaseOnBlueDnsUpdateIsDisabled() {
    BlueGreenStatusProvider.BG_RELEASE_ON_BLUE_DNS_UPDATE.set(props, "false");
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
  }

  @Test
  void inProgressKeepsSuspendingTrafficDuringRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.blueDnsUpdateCompleted = true;
    provider.rollback = true;

    final BlueGreenStatus status = provider.getStatusOfInProgress();

    assertEquals(BlueGreenPhase.IN_PROGRESS, status.getCurrentPhase());
  }

  /**
   * The regression this guards: a monitor reports NOT_CREATED both when the deployment is genuinely
   * absent and when its status simply can't be read, which is expected once a switchover tears the
   * deployment down. Treating that as a rollback would reinstate the pre-switchover routing and send
   * traffic back to the old topology.
   */
  @Test
  void notCreatedAfterSwitchoverIsNotTreatedAsRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.NOT_CREATED));

    assertFalse(provider.rollback, "NOT_CREATED must not be mistaken for a rollback.");
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase,
        "An unreadable status must not move the phase backwards.");
  }

  @Test
  void deploymentReportedAvailableAgainIsTreatedAsRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(BlueGreenPhase.CREATED));

    assertTrue(provider.rollback, "A deployment reported as CREATED again is a genuine rollback.");
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  @Test
  void phaseDoesNotMoveBackwardsWithoutRollback() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        interimStatus(BlueGreenPhase.IN_PROGRESS);

    // Only TARGET drives rollback detection, so a SOURCE regression must be ignored outright.
    provider.updatePhase(BlueGreenRole.SOURCE, interimStatus(BlueGreenPhase.CREATED));

    assertFalse(provider.rollback);
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase);
  }

  @Test
  void nullPhaseLeavesTheCurrentPhaseUntouched() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;

    // A monitor reports a null phase when the status table returned no rows at all.
    provider.updatePhase(BlueGreenRole.TARGET, interimStatus(null));

    assertFalse(provider.rollback);
    assertEquals(BlueGreenPhase.IN_PROGRESS, provider.latestStatusPhase);
  }

  @Test
  void unsupportedDialectDoesNotStartMonitoring() {
    final BlueGreenStatusProvider provider = newProvider();

    // No Blue/Green capable dialect means no monitors, so the provider stays in its initial state.
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
    assertEquals(Collections.emptyMap(), provider.correspondingNodes);
  }

  // ---------------------------------------------------------------------------------------------
  // Blue/Green Deployment deleted (and possibly recreated) while the application keeps running.
  //
  // These cannot be covered by the integration tests: reproducing them requires deleting a
  // deployment and creating a second one against a still-running JVM, and the failure only shows up
  // in state that survives across deployments.
  // ---------------------------------------------------------------------------------------------

  /**
   * Puts the provider into the state it reaches once a deployment has been discovered and then
   * deleted: a known CREATED deployment, stale mappings collected from the green environment, and a
   * source monitor that no longer sees any status information.
   */
  private TestableProvider providerWithDeletedDeployment() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "green-node", false);
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
    provider.roleByHost.put("green-node", BlueGreenRole.TARGET);
    provider.correspondingNodes.put("blue-node", Pair.create(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue-node").build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("green-node").build()));
    return provider;
  }

  @Test
  void absentDeploymentIsNotActedOnBeforeTheGracePeriodExpires() {
    final TestableProvider provider = providerWithDeletedDeployment();

    // First observation only starts the grace period.
    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS - 1);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount,
        "An empty status result can be transient and must not immediately discard the context.");
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  /**
   * The bug this guards: a deployment that is deleted and recreated left the target monitor pinned to
   * the deleted green endpoint, and left the blue to green mapping pointing at nodes that no longer
   * exist, until the application was restarted. Resetting the context and restarting monitoring is
   * what allows the new green endpoint to be discovered.
   */
  @Test
  void deletedDeploymentResetsContextAndRestartsMonitoring() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount, "Monitoring must be restarted from scratch.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
    assertTrue(provider.correspondingNodes.isEmpty(),
        "The blue to green mapping refers to nodes that no longer exist and must be discarded.");
    assertTrue(provider.roleByHost.isEmpty());
    assertNull(provider.interimStatuses[BlueGreenRole.SOURCE.getValue()]);
    assertNull(provider.interimStatuses[BlueGreenRole.TARGET.getValue()]);
  }

  /**
   * The detection signal has to come from the source monitor. When a deployment is deleted the green
   * environment disappears, so the target monitor cannot connect at all and keeps reporting the last
   * phase it read; only the source monitor observes the status information going away.
   */
  @Test
  void absentStatusReportedByTargetAloneDoesNotResetContext() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(BlueGreenPhase.CREATED);
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(null);

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS * 10);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.CREATED, provider.latestStatusPhase);
  }

  /**
   * A status result that comes back before the grace period expires must restart the clock, otherwise
   * a brief blip would eventually be enough to discard a healthy deployment.
   */
  @Test
  void deploymentReappearingBeforeTheGracePeriodExpiresRestartsTheGracePeriod() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS - 1);

    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(BlueGreenPhase.CREATED);
    provider.checkDeploymentRemoved();

    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
    provider.advanceMs(GRACE_MS - 1);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount,
        "The grace period must be measured from the most recent uninterrupted absence.");
  }

  /**
   * Some engines report a deleted deployment as an error rather than as an empty result, which the
   * monitor maps to NOT_CREATED. Both have to be treated as the deployment being gone; handling only
   * the empty result would leave the fix inert on those engines.
   */
  @Test
  void notCreatedReportedBySourceIsTreatedAsDeletedDeployment() {
    final TestableProvider provider = providerWithDeletedDeployment();
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        interimStatus(BlueGreenPhase.NOT_CREATED);

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  @Test
  void deploymentDeletedDuringSwitchoverAlsoResetsContext() {
    final TestableProvider provider = providerWithDeletedDeployment();
    provider.latestStatusPhase = BlueGreenPhase.IN_PROGRESS;

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();

    assertEquals(1, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  /**
   * A completed switchover legitimately removes the status information, and that path is owned by
   * resetContextWhenCompleted(). Treating it as a deletion here would replace the switchover summary
   * with a deletion reset.
   */
  @Test
  void completedSwitchoverIsNotTreatedAsDeletedDeployment() {
    final TestableProvider provider = providerWithDeletedDeployment();
    provider.latestStatusPhase = BlueGreenPhase.COMPLETED;

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS * 10);
    provider.checkDeploymentRemoved();

    assertEquals(0, provider.initMonitoringCount);
    assertEquals(BlueGreenPhase.COMPLETED, provider.latestStatusPhase);
  }

  /**
   * After the reset the monitors report no deployment for as long as none exists. That steady state
   * must not be mistaken for a fresh deletion, or the provider would reset itself in a loop and never
   * keep a monitoring connection long enough to discover a new deployment.
   */
  @Test
  void steadyStateWithNoDeploymentDoesNotResetContextRepeatedly() {
    final TestableProvider provider = providerWithDeletedDeployment();

    provider.checkDeploymentRemoved();
    provider.advanceMs(GRACE_MS);
    provider.checkDeploymentRemoved();
    assertEquals(1, provider.initMonitoringCount);

    // Replacement monitors come up and keep reporting that there is no deployment.
    for (int i = 0; i < 5; i++) {
      provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] = interimStatus(null);
      provider.advanceMs(GRACE_MS * 2);
      provider.checkDeploymentRemoved();
    }

    assertEquals(1, provider.initMonitoringCount,
        "Absence of a deployment is only actionable once, when a known deployment disappears.");
  }

  /**
   * The regression this guards: prepareStatus() skips all processing when the interim status hash is
   * unchanged, and a deleted deployment reports an identical status on every tick. The deletion check
   * therefore has to run on that short-circuit path too. A counter of consecutive observations, or a
   * check placed only on the change path, would never fire.
   */
  @Test
  void deletedDeploymentIsDetectedEvenAfterTheInterimStatusStopsChanging() {
    final TestableProvider provider = newTestableProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;

    final BlueGreenInterimStatus absentStatus = interimStatus(null);

    // First call goes through the change path and starts the grace period.
    provider.prepareStatus(BlueGreenRole.SOURCE, absentStatus);
    assertEquals(0, provider.initMonitoringCount);

    // Every later call reports exactly the same status, so prepareStatus short-circuits.
    provider.advanceMs(GRACE_MS);
    provider.prepareStatus(BlueGreenRole.SOURCE, absentStatus);

    assertEquals(1, provider.initMonitoringCount,
        "The deletion check must keep running once the interim status stops changing.");
    assertEquals(BlueGreenPhase.NOT_CREATED, provider.latestStatusPhase);
  }

  // ---------------------------------------------------------------------------------------------
  // Green environment unreachable. A green environment the driver cannot reach blocks a switchover,
  // because the blue to green mapping needed to route traffic is never built.
  // ---------------------------------------------------------------------------------------------

  @Test
  void unreachableGreenReArmsTheReadinessEvent() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);
    // The deployment had previously been reported as ready for switchover.
    provider.greenTopologyRecognizedLogged.set(true);

    provider.checkGreenReachability();

    assertTrue(provider.greenUnreachableLogged.get());
    assertFalse(provider.greenTopologyRecognizedLogged.get(),
        "Regaining reachability has to be reported, not just the outage.");
  }

  @Test
  void unreachableGreenIsReportedOncePerOutage() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);

    provider.checkGreenReachability();
    // If the warning fired again it would re-arm the readiness event a second time.
    provider.greenTopologyRecognizedLogged.set(true);
    provider.checkGreenReachability();

    assertTrue(provider.greenTopologyRecognizedLogged.get(),
        "A sustained outage must not be reported on every status update.");
  }

  @Test
  void reachableGreenClearsTheUnreachableState() {
    final BlueGreenStatusProvider provider = newProvider();
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, true);
    provider.checkGreenReachability();
    assertTrue(provider.greenUnreachableLogged.get());

    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] = interimStatus(
        BlueGreenPhase.CREATED, false);
    provider.checkGreenReachability();

    assertFalse(provider.greenUnreachableLogged.get(),
        "A recovered green environment must allow the next outage to be reported again.");
  }

  /**
   * Readiness has to account for reachability. The target monitor keeps reporting the last topology
   * it managed to collect, so the collected data still looks complete after the green environment has
   * gone away; without the reachability check the deployment would be reported as ready for
   * switchover when it is not.
   */
  @Test
  void unreachableGreenIsNotReportedAsReadyForSwitchover() {
    final BlueGreenStatusProvider provider = providerReadyForSwitchover(true);

    provider.logGreenTopologyRecognized();

    assertFalse(provider.greenTopologyRecognizedLogged.get(),
        "An unreachable green environment is not ready for switchover.");
  }

  @Test
  void reachableGreenWithCollectedTopologyIsReportedAsReadyForSwitchover() {
    final BlueGreenStatusProvider provider = providerReadyForSwitchover(false);

    provider.logGreenTopologyRecognized();

    assertTrue(provider.greenTopologyRecognizedLogged.get(),
        "The readiness event must still be emitted when the green environment is reachable.");
  }

  /**
   * A provider whose collected data satisfies every readiness condition, so that reachability is the
   * only variable left.
   */
  private BlueGreenStatusProvider providerReadyForSwitchover(final boolean greenUnreachable) {
    final BlueGreenStatusProvider provider = newProvider();
    provider.latestStatusPhase = BlueGreenPhase.CREATED;
    provider.interimStatuses[BlueGreenRole.SOURCE.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "blue-node", false);
    provider.interimStatuses[BlueGreenRole.TARGET.getValue()] =
        collectedInterimStatus(BlueGreenPhase.CREATED, "green-node", greenUnreachable);
    provider.correspondingNodes.put("blue-node", Pair.create(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("blue-node").build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("green-node").build()));
    return provider;
  }
}
