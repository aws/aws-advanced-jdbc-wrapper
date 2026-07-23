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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
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

/** Unit tests for {@link EndpointConnectionVerifier} using an injected, deterministic clock. */
public class EndpointConnectionVerifierTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;
  @Mock private Connection conn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer.endpoint").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  /** A clock returning a fixed sequence of values (last value repeats for any extra calls). */
  private NanoTimeSource sequenceClock(final long... values) {
    final AtomicInteger idx = new AtomicInteger(0);
    return () -> {
      final int i = idx.getAndIncrement();
      return values[Math.min(i, values.length - 1)];
    };
  }

  @Test
  void happyPath_returnsConnectionWithMatchingRole() throws SQLException {
    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(1_000L, 0, sequenceClock(0L));
    when(ctx.connect(writerHost, props)).thenReturn(conn);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.WRITER);

    final Connection result = verifier.getVerifiedConnection(ctx, props, writerHost, HostRole.WRITER, null);

    assertEquals(conn, result);
  }

  @Test
  void roleMismatch_retriesUntilTimeoutThenReturnsNull() throws SQLException {
    // endTime = 0 + 1ms; loop check 0 (enter), then 2ms (exit) → exactly one attempt.
    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(1L, 0, sequenceClock(0L, 0L, 2_000_000L));
    when(ctx.connect(writerHost, props)).thenReturn(conn);
    when(pluginService.getHostRole(conn)).thenReturn(HostRole.READER);

    final Connection result = verifier.getVerifiedConnection(ctx, props, writerHost, HostRole.WRITER, null);

    assertNull(result);
    // The mismatched connection is closed before retrying.
    verify(conn, times(1)).close();
  }

  @Test
  void loginException_rethrownImmediately() throws SQLException {
    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(1_000L, 0, sequenceClock(0L));
    when(ctx.connect(writerHost, props)).thenThrow(new SQLException("bad password"));
    when(pluginService.isLoginException(any(Throwable.class), any())).thenReturn(true);

    assertThrows(SQLException.class,
        () -> verifier.getVerifiedConnection(ctx, props, writerHost, HostRole.WRITER, null));
  }

  @Test
  void nullHostSpecAndNoConnectFunc_returnsNull() throws SQLException {
    final EndpointConnectionVerifier verifier =
        new EndpointConnectionVerifier(1_000L, 0, sequenceClock(0L));

    final Connection result = verifier.getVerifiedConnection(ctx, props, null, HostRole.WRITER, null);

    assertNull(result);
  }
}
