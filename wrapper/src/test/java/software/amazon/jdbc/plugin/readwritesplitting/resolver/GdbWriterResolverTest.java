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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.readwritesplitting.GdbSettings;
import software.amazon.jdbc.plugin.readwritesplitting.ReadWriteSplittingSQLException;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.RdsUtils;

/** Unit tests for {@link GdbWriterResolver}. */
public class GdbWriterResolverTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private GdbSettings settings;
  @Mock private WriterResolver delegate;
  @Mock private RdsUtils rdsUtils;
  @Mock private Connection writerConn;

  private final HostSpec writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("writer.us-east-1").port(5432).role(HostRole.WRITER).build();

  private GdbWriterResolver resolver;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.writerHostSpec()).thenReturn(writerHost);
    lenient().when(settings.rdsHelper()).thenReturn(rdsUtils);
    resolver = new GdbWriterResolver(settings, delegate);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void happyPath_inHomeRegion_delegates() throws SQLException {
    when(settings.isInAccessibleRegion(writerHost)).thenReturn(true);
    when(settings.restrictWriterToHomeRegion()).thenReturn(true);
    when(settings.isInHomeRegion(writerHost)).thenReturn(true);
    when(delegate.resolveWriter(ctx)).thenReturn(WriterResolution.connected(writerConn, writerHost));

    final WriterResolution resolution = resolver.resolveWriter(ctx);

    assertTrue(resolution.isConnected());
    assertEquals(writerConn, resolution.getConnection());
    verify(delegate).resolveWriter(ctx);
  }

  @Test
  void writerNotInAccessibleRegion_throws() {
    when(settings.isInAccessibleRegion(writerHost)).thenReturn(false);
    when(rdsUtils.getRdsRegion(writerHost.getHost())).thenReturn("us-east-1");
    when(settings.accessibleRegions()).thenReturn(Collections.singleton("us-west-2"));

    assertThrows(ReadWriteSplittingSQLException.class, () -> resolver.resolveWriter(ctx));
  }

  @Test
  void outOfHomeRegion_gwfEnabled_returnsStay() throws SQLException {
    when(settings.isInAccessibleRegion(writerHost)).thenReturn(true);
    when(settings.restrictWriterToHomeRegion()).thenReturn(true);
    when(settings.isInHomeRegion(writerHost)).thenReturn(false);
    when(settings.enableGwf()).thenReturn(true);
    when(rdsUtils.getRdsRegion(writerHost.getHost())).thenReturn("us-east-1");

    final WriterResolution resolution = resolver.resolveWriter(ctx);

    assertEquals(WriterResolution.Kind.STAY, resolution.getKind());
    verify(delegate, org.mockito.Mockito.never()).resolveWriter(ctx);
  }

  @Test
  void outOfHomeRegion_gwfDisabled_throws() {
    when(settings.isInAccessibleRegion(writerHost)).thenReturn(true);
    when(settings.restrictWriterToHomeRegion()).thenReturn(true);
    when(settings.isInHomeRegion(writerHost)).thenReturn(false);
    when(settings.enableGwf()).thenReturn(false);
    when(settings.homeRegion()).thenReturn("us-east-1");

    assertThrows(ReadWriteSplittingSQLException.class, () -> resolver.resolveWriter(ctx));
  }
}
