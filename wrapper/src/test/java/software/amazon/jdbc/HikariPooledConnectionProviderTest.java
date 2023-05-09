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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.Dialect;

class HikariPooledConnectionProviderTest {
  @Mock Connection mockConnection;
  @Mock HikariDataSource mockDataSource;
  @Mock HostSpec mockHostSpec;
  @Mock HikariConfig mockConfig;
  @Mock Dialect mockDialect;

  private AutoCloseable closeable;
  private static final Properties emptyProperties = new Properties();

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.isValid(any(Integer.class))).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void testConnectWithDefaultMapping() throws SQLException {
    when(mockHostSpec.getUrl()).thenReturn("url");
    final Set<String> expected = new HashSet<>(Collections.singletonList("urlusername"));

    final HikariPooledConnectionProvider provider =
        spy(new HikariPooledConnectionProvider((hostSpec, properties) -> mockConfig));

    doReturn(mockDataSource).when(provider).createHikariDataSource(any(), any(), any());

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, "username");
    props.setProperty(PropertyDefinition.PASSWORD.name, "password");
    try (Connection conn = provider.connect(
        "protocol", mockDialect, mockHostSpec, props)) {
      assertEquals(mockConnection, conn);
      assertEquals(1, provider.getHostCount());
      final Set<String> hosts = provider.getHosts();
      assertEquals(expected, hosts);
    }

    provider.releaseResources();
  }

  @Test
  void testConnectWithCustomMapping() throws SQLException {
    when(mockHostSpec.getUrl()).thenReturn("url");
    final Set<String> expected = new HashSet<>(Collections.singletonList("url+someUniqueKeyusername"));

    final HikariPooledConnectionProvider provider = spy(new HikariPooledConnectionProvider(
        (hostSpec, properties) -> mockConfig,
        (hostSpec, properties) -> hostSpec.getUrl() + "+someUniqueKey"));

    doReturn(mockDataSource).when(provider).createHikariDataSource(any(), any(), any());

    Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, "username");
    props.setProperty(PropertyDefinition.PASSWORD.name, "password");
    try (Connection conn = provider.connect(
        "protocol", mockDialect, mockHostSpec, props)) {
      assertEquals(mockConnection, conn);
      assertEquals(1, provider.getHostCount());
      final Set<String> hosts = provider.getHosts();
      assertEquals(expected, hosts);
    }

    provider.releaseResources();
  }
}
