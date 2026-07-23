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
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for {@link TopologyWriterResolver}. */
public class TopologyWriterResolverTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private Connection writerConn;

  private final Properties props = new Properties();
  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.properties()).thenReturn(props);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void happyPath_connectsToTopologyWriter() throws SQLException {
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    when(ctx.connect(writerHost, props)).thenReturn(writerConn);

    final WriterResolution resolution = new TopologyWriterResolver().resolveWriter(ctx);

    assertTrue(resolution.isConnected());
    assertEquals(writerConn, resolution.getConnection());
    assertEquals(writerHost, resolution.getHostSpec());
  }

  @Test
  void noWriterHost_throwsAndDoesNotConnect() throws SQLException {
    when(ctx.writerHostSpec()).thenReturn(null);
    doThrow(new SQLException("no writer")).when(ctx).logAndThrow(anyString());

    assertThrows(SQLException.class, () -> new TopologyWriterResolver().resolveWriter(ctx));
    verify(ctx, never()).connect(any(HostSpec.class), any(Properties.class));
  }

  @Test
  void connectFailure_propagates() throws SQLException {
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    when(ctx.connect(writerHost, props)).thenThrow(new SQLException("connect failed"));

    assertThrows(SQLException.class, () -> new TopologyWriterResolver().resolveWriter(ctx));
  }
}
