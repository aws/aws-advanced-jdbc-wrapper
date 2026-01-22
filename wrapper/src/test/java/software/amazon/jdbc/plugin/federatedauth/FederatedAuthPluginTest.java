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

package software.amazon.jdbc.plugin.federatedauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class FederatedAuthPluginTest {

  private static final int DEFAULT_PORT = 1234;
  private static final String DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final String HOST = "pg.testdb.us-east-2.rds.amazonaws.com";
  private static final String IAM_HOST = "pg-123.testdb.us-east-2.rds.amazonaws.com";
  private static final HostSpec HOST_SPEC =
      new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(HOST).build();
  private static final String DB_USER = "iamUser";
  private static final String TEST_TOKEN = "someTestToken";
  private static final TokenInfo TEST_TOKEN_INFO = new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000));
  @Mock private PluginService mockPluginService;
  @Mock private Dialect mockDialect;
  @Mock JdbcCallable<Connection, SQLException> mockLambda;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryContext mockTelemetryContext;
  @Mock private TelemetryCounter mockTelemetryCounter;
  @Mock private CredentialsProviderFactory mockCredentialsProviderFactory;
  @Mock private AwsCredentialsProvider mockAwsCredentialsProvider;
  @Mock private RdsUtils mockRdsUtils;
  @Mock private IamTokenUtility mockIamTokenUtils;
  @Mock private CompletableFuture completableFuture;
  @Mock private AwsCredentialsIdentity mockAwsCredentialsIdentity;
  private Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws ExecutionException, InterruptedException, SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "federatedAuth");
    props.setProperty(FederatedAuthPlugin.DB_USER.name, DB_USER);
    FederatedAuthPlugin.clearCache();

    when(mockRdsUtils.getRdsRegion(anyString())).thenReturn("us-east-2");
    when(mockIamTokenUtils.generateAuthenticationToken(
        any(AwsCredentialsProvider.class),
        any(Region.class),
        anyString(),
        anyInt(),
        anyString())).thenReturn(TEST_TOKEN);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PORT);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(any())).thenReturn(mockTelemetryCounter);
    when(mockTelemetryFactory.openTelemetryContext(any(), any())).thenReturn(mockTelemetryContext);
    when(mockCredentialsProviderFactory.getAwsCredentialsProvider(any(), any(), any()))
        .thenReturn(mockAwsCredentialsProvider);
    when(mockAwsCredentialsProvider.resolveIdentity()).thenReturn(completableFuture);
    when(completableFuture.get()).thenReturn(mockAwsCredentialsIdentity);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testCachedToken() throws SQLException {
    FederatedAuthPlugin plugin =
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory);

    String key = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:" + DEFAULT_PORT + ":iamUser";
    FederatedAuthCacheHolder.tokenCache.put(key, TEST_TOKEN_INFO);

    plugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);

    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
  }

  @Test
  void testExpiredCachedToken() throws SQLException {
    FederatedAuthPlugin spyPlugin = Mockito.spy(
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory, mockRdsUtils, mockIamTokenUtils));

    String key = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:" + DEFAULT_PORT + ":iamUser";
    String someExpiredToken = "someExpiredToken";
    TokenInfo expiredTokenInfo = new TokenInfo(
        someExpiredToken, Instant.now().minusMillis(300000));
    FederatedAuthCacheHolder.tokenCache.put(key, expiredTokenInfo);

    spyPlugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);
    verify(mockIamTokenUtils).generateAuthenticationToken(mockAwsCredentialsProvider,
        Region.US_EAST_2,
        HOST_SPEC.getHost(),
        DEFAULT_PORT,
        DB_USER);
    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
  }

  @Test
  void testNoCachedToken() throws SQLException {
    FederatedAuthPlugin spyPlugin = Mockito.spy(
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory, mockRdsUtils, mockIamTokenUtils));

    spyPlugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);
    verify(mockIamTokenUtils).generateAuthenticationToken(
        mockAwsCredentialsProvider,
        Region.US_EAST_2,
        HOST_SPEC.getHost(),
        DEFAULT_PORT,
        DB_USER);
    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
  }

  @Test
  void testSpecifiedIamHostPortRegion() throws SQLException {
    final String expectedHost = "pg.testdb.us-west-2.rds.amazonaws.com";
    final int expectedPort = 9876;
    final Region expectedRegion = Region.US_WEST_2;

    props.setProperty(FederatedAuthPlugin.IAM_HOST.name, expectedHost);
    props.setProperty(FederatedAuthPlugin.IAM_DEFAULT_PORT.name, String.valueOf(expectedPort));
    props.setProperty(FederatedAuthPlugin.IAM_REGION.name, expectedRegion.toString());

    final String key = "us-west-2:pg.testdb.us-west-2.rds.amazonaws.com:" + expectedPort + ":iamUser";
    FederatedAuthCacheHolder.tokenCache.put(key, TEST_TOKEN_INFO);

    FederatedAuthPlugin plugin =
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory, mockRdsUtils, mockIamTokenUtils);

    plugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);

    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
  }

  @Test
  void testIdpCredentialsFallback() throws SQLException {
    String expectedUser = "expectedUser";
    String expectedPassword = "expectedPassword";
    PropertyDefinition.USER.set(props, expectedUser);
    PropertyDefinition.PASSWORD.set(props, expectedPassword);

    FederatedAuthPlugin plugin =
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory, mockRdsUtils, mockIamTokenUtils);

    String key = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:" + DEFAULT_PORT + ":iamUser";
    FederatedAuthCacheHolder.tokenCache.put(key, TEST_TOKEN_INFO);

    plugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);

    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
    assertEquals(expectedUser, FederatedAuthPlugin.IDP_USERNAME.getString(props));
    assertEquals(expectedPassword, FederatedAuthPlugin.IDP_PASSWORD.getString(props));
  }

  @Test
  public void testUsingIamHost() throws SQLException {
    IamAuthConnectionPlugin.IAM_HOST.set(props, IAM_HOST);
    FederatedAuthPlugin spyPlugin = Mockito.spy(
        new FederatedAuthPlugin(mockPluginService, mockCredentialsProviderFactory, mockRdsUtils, mockIamTokenUtils));

    spyPlugin.connect(DRIVER_PROTOCOL, HOST_SPEC, props, true, mockLambda);

    assertEquals(DB_USER, PropertyDefinition.USER.getString(props));
    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
    verify(mockIamTokenUtils, times(1)).generateAuthenticationToken(
        mockAwsCredentialsProvider,
        Region.US_EAST_2,
        IAM_HOST,
        DEFAULT_PORT,
        DB_USER);
  }
}
