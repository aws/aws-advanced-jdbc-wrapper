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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
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
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for {@link GdbInitAndVerify} and {@link NoOpInitialConnectionHandler}. */
public class GdbInitAndVerifyTest {

  private static final String PROTOCOL = "jdbc:postgresql:";

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private GdbSettings settings;
  @Mock private InitialConnectionHandler delegate;
  @Mock private JdbcCallable<Connection, SQLException> connectFunc;
  @Mock private Connection conn;

  private final Properties props = new Properties();
  private final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer.us-east-1").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void happyPath_initsThenDelegates() throws SQLException {
    when(delegate.onConnect(ctx, PROTOCOL, host, props, true, connectFunc)).thenReturn(conn);
    final GdbInitAndVerify handler = new GdbInitAndVerify(settings, delegate);

    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(conn, result);
    verify(settings).init(host, props);
    verify(delegate).onConnect(ctx, PROTOCOL, host, props, true, connectFunc);
  }

  @Test
  void initFailsFast_delegateNeverCalled() throws SQLException {
    doThrow(new SQLException("missing home region")).when(settings).init(host, props);
    final GdbInitAndVerify handler = new GdbInitAndVerify(settings, delegate);

    assertThrows(SQLException.class,
        () -> handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc));
    verify(delegate, never()).onConnect(any(), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  void noOpHandler_passesThrough() throws SQLException {
    when(connectFunc.call()).thenReturn(conn);
    final NoOpInitialConnectionHandler handler = new NoOpInitialConnectionHandler();

    final Connection result = handler.onConnect(ctx, PROTOCOL, host, props, true, connectFunc);

    assertEquals(conn, result);
  }
}
