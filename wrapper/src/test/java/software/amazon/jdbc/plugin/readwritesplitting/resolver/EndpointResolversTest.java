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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
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
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.cache.NanoTimeSource;
import software.amazon.jdbc.plugin.readwritesplitting.handler.EndpointConnectionVerifier;

/** Unit tests for the endpoint-based writer/reader resolvers. */
public class EndpointResolversTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private Connection conn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();
  private final HostSpec readerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader.endpoint").port(5432).role(HostRole.READER).build();

  private EndpointConnectionVerifier verifier;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
    when(ctx.properties()).thenReturn(props);
    verifier = new EndpointConnectionVerifier(1_000L, 0, () -> 0L);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // ---- Fail-fast construction ----

  @Test
  void writerResolver_missingEndpoint_failsFast() {
    assertThrows(RuntimeException.class, () -> new EndpointWriterResolver(null, false, verifier));
    assertThrows(RuntimeException.class, () -> new EndpointWriterResolver("   ", false, verifier));
  }

  @Test
  void readerResolver_missingEndpoint_failsFast() {
    assertThrows(RuntimeException.class, () -> new EndpointReaderResolver(null, false, verifier));
    assertThrows(RuntimeException.class, () -> new EndpointReaderResolver("   ", false, verifier));
  }

  // ---- EndpointWriterResolver ----

  @Test
  void writerResolver_happyPath_connectsToWriteEndpoint() throws SQLException {
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    when(ctx.connect(writerHost, props)).thenReturn(conn);

    final WriterResolution resolution =
        new EndpointWriterResolver("writer.endpoint", false, verifier).resolveWriter(ctx);

    assertTrue(resolution.isConnected());
    assertEquals(conn, resolution.getConnection());
    assertEquals(writerHost, resolution.getHostSpec());
  }

  @Test
  void writerResolver_nullConnection_throws() throws SQLException {
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    when(ctx.connect(writerHost, props)).thenReturn(null);
    doThrow(new SQLException("failed to connect to writer")).when(ctx).logAndThrow(anyString(), any());

    assertThrows(SQLException.class,
        () -> new EndpointWriterResolver("writer.endpoint", false, verifier).resolveWriter(ctx));
  }

  // ---- EndpointReaderResolver ----

  @Test
  void readerResolver_happyPath_connectsAndBindsReadEndpoint() throws SQLException {
    when(ctx.readerHostSpec()).thenReturn(readerHost);
    when(ctx.connect(readerHost, props)).thenReturn(conn);

    new EndpointReaderResolver("reader.endpoint", false, verifier).switchToReader(ctx);

    verify(ctx).bindReader(conn, readerHost);
    verify(ctx).switchCurrentConnectionTo(conn, readerHost);
    verify(ctx).markReaderFromPool(false);
  }

  @Test
  void readerResolver_nullConnection_throws() throws SQLException {
    when(ctx.readerHostSpec()).thenReturn(readerHost);
    when(ctx.connect(readerHost, props)).thenReturn(null);
    doThrow(new SQLException("failed to connect to reader")).when(ctx).logAndThrow(anyString(), any());

    assertThrows(SQLException.class,
        () -> new EndpointReaderResolver("reader.endpoint", false, verifier).switchToReader(ctx));
    verify(ctx, org.mockito.Mockito.never()).bindReader(any(), any());
  }
}
