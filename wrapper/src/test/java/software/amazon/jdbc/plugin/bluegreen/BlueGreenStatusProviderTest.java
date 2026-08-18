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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.util.FullServicesContainer;
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
        false);
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
}
