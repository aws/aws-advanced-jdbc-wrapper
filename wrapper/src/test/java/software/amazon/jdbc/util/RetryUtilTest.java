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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

class RetryUtilTest {

  @Mock private PluginService mockPluginService;
  @Mock private Connection mockConnection;

  private AutoCloseable closeable;
  private final RetryUtil retryUtil = new RetryUtil();
  private final Properties props = new Properties();

  private HostSpec writerHost;
  private HostSpec readerHost;

  @BeforeEach
  void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    this.writerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("writer").role(HostRole.WRITER).build();
    this.readerHost = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("reader").role(HostRole.READER).build();

    // Return a fresh builder on each call, matching how the copy/availability step is used.
    when(mockPluginService.getHostSpecBuilder())
        .thenAnswer(invocation -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(mockPluginService.connect(any(HostSpec.class), any(Properties.class), any()))
        .thenReturn(mockConnection);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private long soonTimeout() {
    // Small but non-trivial window; the happy paths return well before this elapses.
    return System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
  }

  private Set<HostSpec> allowedSet() {
    final Set<HostSpec> hosts = new HashSet<>();
    hosts.add(writerHost);
    hosts.add(readerHost);
    return hosts;
  }

  @Test
  void getAllowedConnection_nullVerifyRole_selectsHostWithoutStrategy() throws Exception {
    final RetryUtil.Results results = retryUtil.getAllowedConnection(
        mockPluginService,
        props,
        null,
        this::allowedSet,
        "random",
        null, // any role is acceptable (reader-or-writer failover modes)
        soonTimeout());

    assertNotNull(results);
    assertSame(mockConnection, results.getConnection());

    // The role-agnostic path must not depend on the strategy selector, which requires a role.
    verify(mockPluginService, never()).getHostSpecByStrategy(any(), any(), any());
    // With a null verify role, the connected host's role is accepted as-is (no role query).
    verify(mockPluginService, never()).getHostRole(any());
  }

  @Test
  void getAllowedConnection_nullVerifyRole_succeedsEvenWhenStrategyUnsupported() throws Exception {
    // Simulate the DefaultConnectionPlugin behavior that rejects a null role by making any
    // strategy call fail. The null-role path must not reach this and must still connect.
    when(mockPluginService.getHostSpecByStrategy(any(), any(), any()))
        .thenThrow(new UnsupportedOperationException("null role not supported"));

    final RetryUtil.Results results = retryUtil.getAllowedConnection(
        mockPluginService,
        props,
        null,
        this::allowedSet,
        "random",
        null,
        soonTimeout());

    assertNotNull(results);
    assertSame(mockConnection, results.getConnection());
  }

  @Test
  void getAllowedConnection_nonNullVerifyRole_usesStrategyAndVerifiesRole() throws Exception {
    when(mockPluginService.getHostSpecByStrategy(any(), eq(HostRole.READER), eq("random")))
        .thenReturn(readerHost);
    when(mockPluginService.getHostRole(mockConnection)).thenReturn(HostRole.READER);

    final RetryUtil.Results results = retryUtil.getAllowedConnection(
        mockPluginService,
        props,
        null,
        this::allowedSet,
        "random",
        HostRole.READER,
        soonTimeout());

    assertNotNull(results);
    assertSame(mockConnection, results.getConnection());
    assertEquals(HostRole.READER, results.getHostSpec().getRole());
    verify(mockPluginService).getHostSpecByStrategy(any(), eq(HostRole.READER), eq("random"));
  }
}
