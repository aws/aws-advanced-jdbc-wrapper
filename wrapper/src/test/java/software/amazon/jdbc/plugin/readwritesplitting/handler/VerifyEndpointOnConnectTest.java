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

package software.amazon.jdbc.plugin.readwritesplitting.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/** Unit tests for {@link VerifyEndpointOnConnect}. */
public class VerifyEndpointOnConnectTest {

  private static final String PROTOCOL = "jdbc:postgresql:";

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private HostListProviderService hostListProviderService;
  @Mock private RdsUtils rdsUtils;
  @Mock private EndpointConnectionVerifier verifier;
  @Mock private JdbcCallable<Connection, SQLException> connectFunc;
  @Mock private Connection directConn;
  @Mock private Connection verifiedConn;

  private final Properties props = new Properties();
  private final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("cluster.endpoint").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(connectFunc.call()).thenReturn(directConn);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void nonInitialConnection_passesThrough() throws SQLException {
    final VerifyEndpointOnConnect handler =
        new VerifyEndpointOnConnect(true, null, rdsUtils, verifier);

    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, false, connectFunc);

    assertEquals(directConn, result);
    verify(verifier, never()).getVerifiedConnection(any(), any(), any(), any(), any());
  }

  @Test
  void verifyDisabled_passesThrough() throws SQLException {
    final VerifyEndpointOnConnect handler =
        new VerifyEndpointOnConnect(false, null, rdsUtils, verifier);

    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(directConn, result);
    verify(verifier, never()).getVerifiedConnection(any(), any(), any(), any(), any());
  }

  @Test
  void writerClusterUrl_verifiesAsWriter() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.properties()).thenReturn(props);
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(rdsUtils.identifyRdsType(host.getHost())).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    when(verifier.getVerifiedConnection(ctx, props, host, HostRole.WRITER, connectFunc))
        .thenReturn(verifiedConn);

    final VerifyEndpointOnConnect handler =
        new VerifyEndpointOnConnect(true, null, rdsUtils, verifier);
    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(verifiedConn, result);
    verify(hostListProviderService).setInitialConnectionHostSpec(eq(host));
  }

  @Test
  void readerClusterUrl_verifiesAsReader() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.properties()).thenReturn(props);
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(rdsUtils.identifyRdsType(host.getHost())).thenReturn(RdsUrlType.RDS_READER_CLUSTER);
    when(verifier.getVerifiedConnection(ctx, props, host, HostRole.READER, connectFunc))
        .thenReturn(verifiedConn);

    final VerifyEndpointOnConnect handler =
        new VerifyEndpointOnConnect(true, null, rdsUtils, verifier);
    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(verifiedConn, result);
  }

  @Test
  void verificationReturnsNull_fallsBackToDirectConnect() throws SQLException {
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.properties()).thenReturn(props);
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(rdsUtils.identifyRdsType(host.getHost())).thenReturn(RdsUrlType.RDS_WRITER_CLUSTER);
    when(verifier.getVerifiedConnection(ctx, props, host, HostRole.WRITER, connectFunc))
        .thenReturn(null);

    final VerifyEndpointOnConnect handler =
        new VerifyEndpointOnConnect(true, null, rdsUtils, verifier);
    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(directConn, result);
  }
}
