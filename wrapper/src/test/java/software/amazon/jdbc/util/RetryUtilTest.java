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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class RetryUtilTest {

  private static final HostSpec READER = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-2.xyz.us-east-2.rds.amazonaws.com").port(3306).role(HostRole.READER).build();

  private PluginService pluginService;
  private Connection connection;
  private final Supplier<Set<HostSpec>> allowedHosts = () -> Collections.singleton(READER);

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws SQLException {
    this.pluginService = mock(PluginService.class);
    this.connection = mock(Connection.class);
    when(this.pluginService.getHostSpecBuilder())
        .thenAnswer(i -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
    when(this.pluginService.getHostSpecByStrategy(anyList(), any(), anyString()))
        .thenAnswer(i -> ((List<HostSpec>) i.getArgument(0)).get(0));
    when(this.pluginService.connect(any(HostSpec.class), any(Properties.class), any(ConnectionPlugin.class)))
        .thenReturn(this.connection);
    when(this.pluginService.getHostRole(this.connection)).thenReturn(HostRole.READER);
  }

  private static long inOneSecond() {
    return System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
  }

  @Test
  void testWithoutAbandonConditionReturnsConnection() throws Exception {
    final RetryUtil.Results results = new RetryUtil().getAllowedConnection(
        this.pluginService, new Properties(), mock(ConnectionPlugin.class), this.allowedHosts, null,
        HostRole.READER, inOneSecond());

    assertNotNull(results);
    assertEquals(this.connection, results.getConnection());
  }

  @Test
  void testAbandonedBeforeConnecting() throws Exception {
    final RetryUtil.Results results = new RetryUtil().getAllowedConnection(
        this.pluginService, new Properties(), mock(ConnectionPlugin.class), this.allowedHosts, null,
        HostRole.READER, inOneSecond(), () -> true);

    assertNull(results);
    verify(this.pluginService, never()).connect(any(HostSpec.class), any(Properties.class), any());
  }

  @Test
  void testAbandonedAfterConnectingClosesTheConnection() throws Exception {
    // The condition turns true while the connection is being opened.
    final AtomicBoolean changed = new AtomicBoolean(false);
    when(this.pluginService.getHostRole(this.connection)).thenAnswer(i -> {
      changed.set(true);
      return HostRole.READER;
    });

    final RetryUtil.Results results = new RetryUtil().getAllowedConnection(
        this.pluginService, new Properties(), mock(ConnectionPlugin.class), this.allowedHosts, null,
        HostRole.READER, inOneSecond(), changed::get);

    assertNull(results);
    verify(this.connection).close();
  }

  @Test
  void testNotAbandonedReturnsConnection() throws Exception {
    final RetryUtil.Results results = new RetryUtil().getAllowedConnection(
        this.pluginService, new Properties(), mock(ConnectionPlugin.class), this.allowedHosts, null,
        HostRole.READER, inOneSecond(), () -> false);

    assertNotNull(results);
    verify(this.connection, never()).close();
  }
}
