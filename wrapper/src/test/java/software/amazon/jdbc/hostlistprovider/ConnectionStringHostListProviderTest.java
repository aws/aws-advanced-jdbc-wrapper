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

package software.amazon.jdbc.hostlistprovider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
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

/** Unit tests for {@link ConnectionStringHostListProvider}. */
public class ConnectionStringHostListProviderTest {

  private static final String URL =
      "jdbc:mysql://primary.xyz.us-east-1.rds.amazonaws.com:3306"
          + ",replica1.xyz.us-east-1.rds.amazonaws.com:3306"
          + ",replica2.xyz.us-east-1.rds.amazonaws.com:3306/db";
  private static final String PRIMARY = "primary.xyz.us-east-1.rds.amazonaws.com:3306";
  private static final String REPLICA1 = "replica1.xyz.us-east-1.rds.amazonaws.com:3306";

  private AutoCloseable closeable;

  @Mock private HostListProviderService hostListProviderService;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(hostListProviderService.getHostSpecBuilder())
        .thenAnswer(invocation -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy()));
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private ConnectionStringHostListProvider provider(final boolean singleWriterConnectionString) {
    final Properties props = new Properties();
    props.setProperty(
        ConnectionStringHostListProvider.SINGLE_WRITER_CONNECTION_STRING.name,
        String.valueOf(singleWriterConnectionString));
    return new ConnectionStringHostListProvider(props, URL, hostListProviderService);
  }

  private static HostSpec byHostAndPort(final List<HostSpec> hosts, final String hostAndPort) {
    return hosts.stream()
        .filter(host -> hostAndPort.equals(host.getHostAndPort()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("host not in list: " + hostAndPort));
  }

  @Test
  void singleWriterConnectionString_assignsRolesPositionally() throws SQLException {
    final List<HostSpec> hosts = provider(true).refresh();

    assertEquals(3, hosts.size());
    assertEquals(HostRole.WRITER, byHostAndPort(hosts, PRIMARY).getRole());
    assertEquals(HostRole.READER, byHostAndPort(hosts, REPLICA1).getRole());
  }

  @Test
  void updateHostRole_replacesRoleOfMatchingHost() throws SQLException {
    final ConnectionStringHostListProvider provider = provider(true);

    assertTrue(provider.updateHostRole(PRIMARY, HostRole.READER));

    assertEquals(HostRole.READER, byHostAndPort(provider.refresh(), PRIMARY).getRole());
  }

  /** The lists already handed out are unmodifiable views of the backing list, so they update too. */
  @Test
  void updateHostRole_isVisibleThroughPreviouslyReturnedList() throws SQLException {
    final ConnectionStringHostListProvider provider = provider(true);
    final List<HostSpec> hostsBefore = provider.refresh();

    assertTrue(provider.updateHostRole(REPLICA1, HostRole.WRITER));

    assertEquals(HostRole.WRITER, byHostAndPort(hostsBefore, REPLICA1).getRole());
  }

  @Test
  void updateHostRole_sameRoleIsNotAChange() throws SQLException {
    final ConnectionStringHostListProvider provider = provider(true);

    assertFalse(provider.updateHostRole(PRIMARY, HostRole.WRITER));
  }

  @Test
  void updateHostRole_unknownHostIsIgnored() throws SQLException {
    final ConnectionStringHostListProvider provider = provider(true);

    assertFalse(provider.updateHostRole("not-in-the-list.example.com:3306", HostRole.READER));
    assertEquals(HostRole.WRITER, byHostAndPort(provider.refresh(), PRIMARY).getRole());
  }

  @Test
  void updateHostRole_matchesHostNameCaseInsensitively() throws SQLException {
    final ConnectionStringHostListProvider provider = provider(true);

    assertTrue(provider.updateHostRole(PRIMARY.toUpperCase(), HostRole.READER));
    assertEquals(HostRole.READER, byHostAndPort(provider.refresh(), PRIMARY).getRole());
  }

  /**
   * Without {@code singleWriterConnectionString} every plain RDS instance endpoint is assumed to be
   * a writer, which leaves no reader for read/write splitting to select. Pinned here because it is
   * the most common cause of reads silently staying on the writer.
   */
  @Test
  void withoutSingleWriterConnectionString_everyHostIsAWriter() throws SQLException {
    final List<HostSpec> hosts = provider(false).refresh();

    assertEquals(3, hosts.size());
    assertTrue(hosts.stream().allMatch(host -> HostRole.WRITER.equals(host.getRole())));
  }
}
