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

package software.amazon.jdbc.plugin.readwritesplitting.balancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
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

/** Unit tests for {@link PerQueryBalancedReaderPolicy} and the sticky policy's defaults. */
public class PerQueryBalancedReaderPolicyTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private PluginService pluginService;

  private final HostSpec reader1 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-1").port(5432).role(HostRole.READER).build();
  private final HostSpec reader2 = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("reader-2").port(5432).role(HostRole.READER).build();
  private final List<HostSpec> candidates = Arrays.asList(reader1, reader2);

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(ctx.pluginService()).thenReturn(pluginService);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void isPerQuery_true() {
    assertTrue(new PerQueryBalancedReaderPolicy(false, "random").isPerQuery());
  }

  @Test
  void stickyReader_isPerQuery_false() {
    assertFalse(new StickyReaderPolicy("random").isPerQuery());
  }

  @Test
  void includesWriter_reflectsConfiguration() {
    assertTrue(new PerQueryBalancedReaderPolicy(true, "random").includesWriter());
    assertFalse(new PerQueryBalancedReaderPolicy(false, "random").includesWriter());
  }

  @Test
  void pickReader_excludeWriter_selectsReaderRole() throws SQLException {
    when(pluginService.getHostSpecByStrategy(anyList(), eq(HostRole.READER), eq("random")))
        .thenReturn(reader1);

    final HostSpec picked = new PerQueryBalancedReaderPolicy(false, "random").pickReader(ctx, candidates);

    assertEquals(reader1, picked);
    verify(pluginService).getHostSpecByStrategy(candidates, HostRole.READER, "random");
  }

  @Test
  void pickReader_includeWriter_usesNullRole() throws SQLException {
    when(pluginService.getHostSpecByStrategy(anyList(), isNull(), eq("random"))).thenReturn(reader2);

    final HostSpec picked = new PerQueryBalancedReaderPolicy(true, "random").pickReader(ctx, candidates);

    assertEquals(reader2, picked);
    // A null role lets the strategy consider the writer as an eligible target too.
    verify(pluginService).getHostSpecByStrategy(candidates, null, "random");
  }
}
