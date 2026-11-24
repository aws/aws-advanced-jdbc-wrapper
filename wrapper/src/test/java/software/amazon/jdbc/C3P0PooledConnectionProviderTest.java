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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Pair;

class C3P0PooledConnectionProviderTest {

  private final HostSpec instance = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("instance-1.xyz.us-east-1.rds.amazonaws.com").port(5432).build();

  private AutoCloseable closeable;
  private final List<ComboPooledDataSource> createdPools = new ArrayList<>();

  @Mock Dialect mockDialect;
  @Mock TargetDriverDialect mockTargetDriverDialect;

  // Creates a mock pool per call instead of a real c3p0 pool, which would try to reach the host.
  private final C3P0PooledConnectionProvider provider = new C3P0PooledConnectionProvider() {
    @Override
    protected ComboPooledDataSource createDataSource(
        final String protocol,
        final HostSpec hostSpec,
        final Properties props,
        final TargetDriverDialect driverDialect) {
      final ComboPooledDataSource ds = mock(ComboPooledDataSource.class);
      try {
        when(ds.getConnection()).thenReturn(mock(Connection.class));
      } catch (final SQLException e) {
        throw new IllegalStateException(e);
      }
      createdPools.add(ds);
      return ds;
    }
  };

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    C3P0PooledConnectionProvider.databasePools.clear();
  }

  @AfterEach
  void tearDown() throws Exception {
    C3P0PooledConnectionProvider.databasePools.clear();
    closeable.close();
  }

  private static Properties userProps(final String user, final String password) {
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, user);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);
    return props;
  }

  @Test
  void test_differentUsersGetDifferentPools() throws SQLException {
    provider.connect("jdbc:postgresql://", mockDialect, mockTargetDriverDialect, instance, userProps("alice", "a"));
    provider.connect("jdbc:postgresql://", mockDialect, mockTargetDriverDialect, instance, userProps("bob", "b"));

    // A pool shared between users would hand bob physical connections authenticated as alice.
    assertEquals(2, createdPools.size());
    assertEquals(2, C3P0PooledConnectionProvider.databasePools.size());
  }

  @Test
  void test_sameUserReusesPool() throws SQLException {
    provider.connect("jdbc:postgresql://", mockDialect, mockTargetDriverDialect, instance, userProps("alice", "a"));
    provider.connect("jdbc:postgresql://", mockDialect, mockTargetDriverDialect, instance, userProps("alice", "a"));

    assertEquals(1, createdPools.size());
    assertSame(createdPools.get(0),
        C3P0PooledConnectionProvider.databasePools.get(
            Pair.create(instance.getUrl(), "alice"), 0));
  }

  @Test
  void test_poolKeyWithoutUserIsEmpty() {
    assertEquals("", provider.getPoolKey(new Properties()));
  }
}
