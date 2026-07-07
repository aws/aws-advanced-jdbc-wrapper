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

package software.amazon.jdbc.plugin.readwritesplitting.refresher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/** Unit tests for {@link EndpointWriterHostRefresher}. */
public class EndpointWriterHostRefresherTest {

  private AutoCloseable closeable;

  @Mock private RwSplitContext ctx;
  @Mock private HostListProviderService hostListProviderService;

  private final HostSpec initialHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("initial").port(5432).role(HostRole.WRITER).build();

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void seedsWriterHostFromEndpoint_whenUnset() {
    when(ctx.writerHostSpec()).thenReturn(null);
    when(ctx.hostListProviderService()).thenReturn(hostListProviderService);
    when(hostListProviderService.getCurrentHostSpec()).thenReturn(initialHost);
    when(hostListProviderService.getHostSpecBuilder())
        .thenReturn(new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));

    new EndpointWriterHostRefresher("writer.endpoint").refresh(ctx);

    final ArgumentCaptor<HostSpec> captor = ArgumentCaptor.forClass(HostSpec.class);
    verify(ctx).setWriterHostSpec(captor.capture());
    assertEquals("writer.endpoint", captor.getValue().getHost());
    assertEquals(HostRole.WRITER, captor.getValue().getRole());
  }

  @Test
  void doesNotOverwrite_whenWriterHostAlreadySet() {
    when(ctx.writerHostSpec()).thenReturn(initialHost);

    new EndpointWriterHostRefresher("writer.endpoint").refresh(ctx);

    verify(ctx, never()).setWriterHostSpec(any(HostSpec.class));
  }

  @Test
  void noOp_whenHostListProviderServiceUnavailable() {
    when(ctx.writerHostSpec()).thenReturn(null);
    when(ctx.hostListProviderService()).thenReturn(null);

    new EndpointWriterHostRefresher("writer.endpoint").refresh(ctx);

    verify(ctx, never()).setWriterHostSpec(any(HostSpec.class));
  }
}
