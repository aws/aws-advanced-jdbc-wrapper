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

package software.amazon.jdbc.plugin.readwritesplitting.gate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.PluginCallContext;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.parser.RoutingHint;
import software.amazon.jdbc.parser.SqlContextKeys;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.signal.TargetRole;
import software.amazon.jdbc.states.SessionStateService;

/** Unit tests for {@link TransactionAwareGate}. */
public class TransactionAwareGateTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private SessionStateService sessionStateService;

  private PluginCallContext callContext;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    callContext = new PluginCallContext();
    when(ctx.pluginService()).thenReturn(pluginService);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void noDecisionAndKeep_neverSwitch() throws SQLException {
    final TransactionAwareGate gate = new TransactionAwareGate();
    assertFalse(gate.canSwitch(ctx, TargetRole.KEEP));
    assertFalse(gate.canSwitch(ctx, TargetRole.NO_DECISION));
  }

  @Test
  void classicGate_allowsSwitchWhenNotInTransaction() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    final TransactionAwareGate gate = new TransactionAwareGate();
    assertTrue(gate.canSwitch(ctx, TargetRole.READER));
    assertTrue(gate.canSwitch(ctx, TargetRole.WRITER));
  }

  @Test
  void classicGate_pinsWhenInTransaction() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(true);
    final TransactionAwareGate gate = new TransactionAwareGate();
    assertFalse(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void keepHint_pinsWhenHonored() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.KEEP);

    final TransactionAwareGate gate = new TransactionAwareGate(true, false);
    assertFalse(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void keepHint_ignoredWhenNotHonored() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    callContext.setAttribute(SqlContextKeys.ROUTING_HINT, RoutingHint.KEEP);

    // Classic gate does not honor the KEEP hint.
    final TransactionAwareGate gate = new TransactionAwareGate();
    assertTrue(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void autoCommitOff_pinsWhenConfigured() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(false));

    final TransactionAwareGate gate = new TransactionAwareGate(true, true);
    assertFalse(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void autoCommitOn_allowsSwitch() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.of(true));

    final TransactionAwareGate gate = new TransactionAwareGate(true, true);
    assertTrue(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void autoCommitIndeterminate_fallsBackToAllowingSwitch() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenThrow(new SQLException("indeterminate"));

    final TransactionAwareGate gate = new TransactionAwareGate(true, true);
    assertTrue(gate.canSwitch(ctx, TargetRole.READER));
  }

  @Test
  void autoCommitEmpty_allowsSwitch() throws SQLException {
    when(pluginService.isInTransaction()).thenReturn(false);
    when(pluginService.getCallContext()).thenReturn(callContext);
    when(pluginService.getSessionStateService()).thenReturn(sessionStateService);
    when(sessionStateService.getAutoCommit()).thenReturn(Optional.empty());

    final TransactionAwareGate gate = new TransactionAwareGate(true, true);
    assertTrue(gate.canSwitch(ctx, TargetRole.READER));
  }
}
