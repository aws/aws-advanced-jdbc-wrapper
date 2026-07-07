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

package software.amazon.jdbc.plugin.readwritesplitting.updater;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.EndpointRoleClassifier;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.TopologyRoleClassifier;

/** Unit tests for the connection-update policies. */
public class UpdatePoliciesTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private Connection currentConn;
  @Mock private Connection otherConn;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
  private final HostSpec readerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader.endpoint").port(5432).role(HostRole.READER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // ---- RoleBasedUpdatePolicy ----

  @Test
  void roleBased_updatesByHostRole() throws SQLException {
    final RoleBasedUpdatePolicy policy = new RoleBasedUpdatePolicy(new TopologyRoleClassifier());
    assertTrue(policy.shouldUpdateWriter(ctx, currentConn, writerHost));
    assertFalse(policy.shouldUpdateReader(ctx, currentConn, writerHost));
    assertTrue(policy.shouldUpdateReader(ctx, currentConn, readerHost));
    assertFalse(policy.shouldUpdateWriter(ctx, currentConn, readerHost));
  }

  // ---- VerifiedEndpointUpdatePolicy ----

  private VerifiedEndpointUpdatePolicy policy(final boolean verify) {
    return new VerifiedEndpointUpdatePolicy(
        new EndpointRoleClassifier("writer.endpoint", "reader.endpoint"), verify);
  }

  @Test
  void verifiedEndpoint_verifyOff_newWriterConnection_updates() throws SQLException {
    when(ctx.writerConnection()).thenReturn(null);
    assertTrue(policy(false).shouldUpdateWriter(ctx, currentConn, writerHost));
  }

  @Test
  void verifiedEndpoint_sameConnection_doesNotUpdate() throws SQLException {
    when(ctx.writerConnection()).thenReturn(currentConn);
    assertFalse(policy(false).shouldUpdateWriter(ctx, currentConn, writerHost));
  }

  @Test
  void verifiedEndpoint_verifyOn_roleMatches_updates() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.writerConnection()).thenReturn(otherConn);
    when(pluginService.getHostRole(currentConn)).thenReturn(HostRole.WRITER);
    assertTrue(policy(true).shouldUpdateWriter(ctx, currentConn, writerHost));
  }

  @Test
  void verifiedEndpoint_verifyOn_roleMismatch_doesNotUpdate() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.writerConnection()).thenReturn(otherConn);
    when(pluginService.getHostRole(currentConn)).thenReturn(HostRole.READER);
    assertFalse(policy(true).shouldUpdateWriter(ctx, currentConn, writerHost));
  }

  @Test
  void verifiedEndpoint_readerUpdate_verifyOn() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.readerConnection()).thenReturn(otherConn);
    when(pluginService.getHostRole(currentConn)).thenReturn(HostRole.READER);
    assertTrue(policy(true).shouldUpdateReader(ctx, currentConn, readerHost));
  }

  @Test
  void verifiedEndpoint_wrongHostRole_doesNotUpdate() throws SQLException {
    // A reader host is never adopted as the writer.
    assertFalse(policy(false).shouldUpdateWriter(ctx, currentConn, readerHost));
  }
}
