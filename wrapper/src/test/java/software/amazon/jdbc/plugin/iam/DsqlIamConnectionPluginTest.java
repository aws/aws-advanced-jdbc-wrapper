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

package software.amazon.jdbc.plugin.iam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static software.amazon.jdbc.plugin.iam.DsqlTokenUtilityTest.ADMIN_USER;
import static software.amazon.jdbc.plugin.iam.DsqlTokenUtilityTest.REGULAR_USER;
import static software.amazon.jdbc.plugin.iam.DsqlTokenUtilityTest.assertTokenContainsProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginChainBuilder;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginManagerService;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

class DsqlIamConnectionPluginTest {

  private static final Region TEST_REGION = Region.US_EAST_1;
  private static final String TEST_HOSTNAME = String.format("foo0bar1baz2quux3quuux4.dsql.%s.on.aws", TEST_REGION);
  private static final int TEST_PORT = 5432;

  private static final String DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final HostSpec HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host(TEST_HOSTNAME).port(TEST_PORT).build();

  private static final String DEFAULT_USERNAME = "admin";

  private final Properties props = new Properties();

  private AutoCloseable cleanMocksCallback;
  @Mock private Connection mockConnection;
  @Mock private PluginService mockPluginService;
  @Mock private FullServicesContainer mockServicesContainer;
  @Mock private Dialect mockDialect;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryContext mockTelemetryContext;
  @Mock private JdbcCallable<Connection, SQLException> mockLambda;
  @Mock private ConnectionProvider mockConnectionProvider;
  @Mock private PluginManagerService mockPluginManagerService;

  @BeforeEach
  public void init() {
    cleanMocksCallback = MockitoAnnotations.openMocks(this);

    IamAuthConnectionPlugin.clearCache();

    props.setProperty(PropertyDefinition.USER.name, DEFAULT_USERNAME);
    props.setProperty("iamRegion", Region.US_EAST_1.toString());
    props.setProperty(PropertyDefinition.PLUGINS.name, "iamDsql");

    doReturn(mockPluginService).when(mockServicesContainer).getPluginService();
    doReturn(mockDialect).when(mockPluginService).getDialect();
    doReturn(TEST_PORT).when(mockDialect).getDefaultPort();
    doReturn(mockTelemetryFactory).when(mockPluginService).getTelemetryFactory();
    doReturn(mockTelemetryContext).when(mockTelemetryFactory)
        .openTelemetryContext(anyString(), eq(TelemetryTraceLevel.NESTED));
  }

  @AfterEach
  public void cleanup() throws Exception {
    cleanMocksCallback.close();
  }

  @SuppressWarnings("resource") // Prevent Mockito warning when mocking closeable return type.
  private void assertPluginProvidesDsqlTokens(final ConnectionPlugin plugin, final String username) throws SQLException {
    Mockito.doReturn(mockConnection).when(mockLambda).call();

    plugin
        .connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda)
        .close();

    final String cacheKey = IamAuthUtils.getCacheKey(
        username,
        TEST_HOSTNAME,
        TEST_PORT,
        TEST_REGION);

    final TokenInfo info = IamAuthCacheHolder.tokenCache.get(cacheKey);
    final String token = info.getToken();

    assertTokenContainsProperties(token, TEST_HOSTNAME, username);
  }

  @Test
  public void testDsqlPluginRegistration() throws SQLException {
    ConnectionPluginChainBuilder builder = new ConnectionPluginChainBuilder();

    final List<ConnectionPlugin> result = builder.getPlugins(
        mockServicesContainer,
        mockConnectionProvider,
        null,
        mockPluginManagerService,
        props,
        null);

    // 2 because default plugin is always included.
    assertEquals(2, result.size());
    final ConnectionPlugin plugin = result.get(0);

    assertInstanceOf(IamAuthConnectionPlugin.class, plugin);
    assertPluginProvidesDsqlTokens(plugin, DEFAULT_USERNAME);
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testDsqlTokenGeneratedBasedOnUser(final String username) throws SQLException {
    props.setProperty(PropertyDefinition.USER.name, username);

    final DsqlIamConnectionPluginFactory factory = new DsqlIamConnectionPluginFactory();
    final ConnectionPlugin plugin = factory.getInstance(mockPluginService, props);
    assertPluginProvidesDsqlTokens(plugin, username);
  }
}
