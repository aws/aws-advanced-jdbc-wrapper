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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.TestDefaultRdsUtilities;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

class IamAuthConnectionPluginTest {

  private static final String GENERATED_TOKEN = "generatedToken";
  private static final String TEST_TOKEN = "testToken";
  private static final int DEFAULT_PG_PORT = 5432;
  private static final int DEFAULT_MYSQL_PORT = 3306;
  private static final String PG_CACHE_KEY = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:"
      + DEFAULT_PG_PORT + ":postgresqlUser";
  private static final String MYSQL_CACHE_KEY = "us-east-2:mysql.testdb.us-east-2.rds.amazonaws.com:"
      + DEFAULT_MYSQL_PORT + ":mysqlUser";
  private static final String PG_DRIVER_PROTOCOL = "jdbc:postgresql:";
  private static final String MYSQL_DRIVER_PROTOCOL = "jdbc:mysql:";
  private static final HostSpec PG_HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-east-2.rds.amazonaws.com").build();
  private static final HostSpec PG_HOST_SPEC_WITH_PORT = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-east-2.rds.amazonaws.com").port(1234).build();
  private static final HostSpec PG_HOST_SPEC_WITH_REGION = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("pg.testdb.us-west-1.rds.amazonaws.com").build();
  private static final HostSpec MYSQL_HOST_SPEC = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
      .host("mysql.testdb.us-east-2.rds.amazonaws.com").build();
  private Properties props;

  @Mock PluginService mockPluginService;
  @Mock TelemetryFactory mockTelemetryFactory;
  @Mock TelemetryCounter mockTelemetryCounter;
  @Mock TelemetryContext mockTelemetryContext;
  @Mock JdbcCallable<Connection, SQLException> mockLambda;
  @Mock Dialect mockDialect;
  @Mock private RdsUtils mockRdsUtils;
  @Mock private IamTokenUtility mockIamTokenUtils;
  private AutoCloseable closable;

  @BeforeEach
  public void init() {
    closable = MockitoAnnotations.openMocks(this);
    props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, "postgresqlUser");
    props.setProperty(PropertyDefinition.PASSWORD.name, "postgresqlPassword");
    props.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    IamAuthConnectionPlugin.clearCache();

    when(mockRdsUtils.getRdsRegion(anyString())).thenReturn("us-east-2");
    when(mockIamTokenUtils.generateAuthenticationToken(
        any(AwsCredentialsProvider.class),
        any(Region.class),
        anyString(),
        anyInt(),
        anyString())).thenReturn(GENERATED_TOKEN);
    when(mockPluginService.getDialect()).thenReturn(mockDialect);
    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.createCounter(anyString())).thenReturn(mockTelemetryCounter);
    when(mockTelemetryFactory.openTelemetryContext(anyString(), eq(TelemetryTraceLevel.NESTED))).thenReturn(
        mockTelemetryContext);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    closable.close();
  }

  @BeforeAll
  public static void registerDrivers() throws SQLException {
    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  @Test
  public void testPostgresConnectValidTokenInCache() throws SQLException {
    IamAuthCacheHolder.tokenCache.put(PG_CACHE_KEY,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC);
  }

  @Test
  public void testMySqlConnectValidTokenInCache() throws SQLException {
    props.setProperty(PropertyDefinition.USER.name, "mysqlUser");
    props.setProperty(PropertyDefinition.PASSWORD.name, "mysqlPassword");
    IamAuthCacheHolder.tokenCache.put(MYSQL_CACHE_KEY,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_MYSQL_PORT);

    testTokenSetInProps(MYSQL_DRIVER_PROTOCOL, MYSQL_HOST_SPEC);
  }

  @Test
  public void testPostgresConnectWithInvalidPortFallbacksToHostPort() throws SQLException {
    final String invalidIamDefaultPort = "0";
    props.setProperty(IamAuthConnectionPlugin.IAM_DEFAULT_PORT.name, invalidIamDefaultPort);

    final String cacheKeyWithNewPort = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:"
        + PG_HOST_SPEC_WITH_PORT.getPort() + ":postgresqlUser";
    IamAuthCacheHolder.tokenCache.put(cacheKeyWithNewPort,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC_WITH_PORT);
  }

  @Test
  public void testPostgresConnectWithInvalidPortAndNoHostPortFallbacksToHostPort() throws SQLException {
    final String invalidIamDefaultPort = "0";
    props.setProperty(IamAuthConnectionPlugin.IAM_DEFAULT_PORT.name, invalidIamDefaultPort);

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    final String cacheKeyWithNewPort = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:"
        + DEFAULT_PG_PORT + ":postgresqlUser";
    IamAuthCacheHolder.tokenCache.put(cacheKeyWithNewPort,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC);
  }

  @Test
  public void testConnectExpiredTokenInCache() throws SQLException {
    IamAuthCacheHolder.tokenCache.put(PG_CACHE_KEY,
        new TokenInfo(TEST_TOKEN, Instant.now().minusMillis(300000)));

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    testGenerateToken(PG_DRIVER_PROTOCOL, PG_HOST_SPEC);
  }

  @Test
  public void testConnectEmptyCache() throws SQLException {
    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    testGenerateToken(PG_DRIVER_PROTOCOL, PG_HOST_SPEC);
  }

  @Test
  public void testConnectWithSpecifiedPort() throws SQLException {
    final String cacheKeyWithNewPort = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:" + "postgresqlUser";
    IamAuthCacheHolder.tokenCache.put(cacheKeyWithNewPort,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC_WITH_PORT);
  }

  @Test
  public void testConnectWithSpecifiedIamDefaultPort() throws SQLException {
    final String iamDefaultPort = "9999";
    props.setProperty(IamAuthConnectionPlugin.IAM_DEFAULT_PORT.name, iamDefaultPort);
    final String cacheKeyWithNewPort = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:"
        + iamDefaultPort + ":postgresqlUser";
    IamAuthCacheHolder.tokenCache.put(cacheKeyWithNewPort,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC_WITH_PORT);
  }

  @Test
  public void testConnectWithSpecifiedRegion() throws SQLException {
    final String cacheKeyWithNewRegion =
        "us-west-1:pg.testdb.us-west-1.rds.amazonaws.com:" + DEFAULT_PG_PORT + ":" + "postgresqlUser";
    props.setProperty(IamAuthConnectionPlugin.IAM_REGION.name, "us-west-1");
    IamAuthCacheHolder.tokenCache.put(cacheKeyWithNewRegion,
        new TokenInfo(TEST_TOKEN, Instant.now().plusMillis(300000)));

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    testTokenSetInProps(PG_DRIVER_PROTOCOL, PG_HOST_SPEC_WITH_REGION);
  }

  @Test
  public void testConnectWithSpecifiedHost() throws SQLException {
    props.setProperty(IamAuthConnectionPlugin.IAM_REGION.name, "us-east-2");
    props.setProperty(IamAuthConnectionPlugin.IAM_HOST.name, "pg.testdb.us-east-2.rds.amazonaws.com");

    when(mockDialect.getDefaultPort()).thenReturn(DEFAULT_PG_PORT);

    testGenerateToken(
        PG_DRIVER_PROTOCOL,
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("8.8.8.8").build(),
        "pg.testdb.us-east-2.rds.amazonaws.com");
  }

  @Test
  public void testAwsSupportedRegionsUrlExists() throws IOException {
    final URL url =
        new URL("https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html");
    final HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
    final int responseCode = urlConnection.getResponseCode();

    assertEquals(HttpURLConnection.HTTP_OK, responseCode);
  }

  public void testTokenSetInProps(final String protocol, final HostSpec hostSpec) throws SQLException {

    IamAuthConnectionPlugin targetPlugin = new IamAuthConnectionPlugin(mockPluginService, mockIamTokenUtils);
    doThrow(new SQLException()).when(mockLambda).call();

    assertThrows(SQLException.class, () -> targetPlugin.connect(protocol, hostSpec, props, true, mockLambda));
    verify(mockLambda, times(1)).call();

    assertEquals(TEST_TOKEN, PropertyDefinition.PASSWORD.getString(props));
  }

  private void testGenerateToken(final String protocol, final HostSpec hostSpec) throws SQLException {
    testGenerateToken(protocol, hostSpec, hostSpec.getHost());
  }

  private void testGenerateToken(
      final String protocol,
      final HostSpec hostSpec,
      final String expectedHost) throws SQLException {
    final IamAuthConnectionPlugin targetPlugin = new IamAuthConnectionPlugin(mockPluginService, mockIamTokenUtils);
    final IamAuthConnectionPlugin spyPlugin = Mockito.spy(targetPlugin);

    doThrow(new SQLException()).when(mockLambda).call();

    assertThrows(SQLException.class,
        () -> spyPlugin.connect(protocol, hostSpec, props, true, mockLambda));

    verify(mockIamTokenUtils).generateAuthenticationToken(
        any(DefaultCredentialsProvider.class),
        eq(Region.US_EAST_2),
        eq(expectedHost),
        eq(DEFAULT_PG_PORT),
        eq("postgresqlUser"));
    verify(mockLambda, times(1)).call();

    assertEquals(GENERATED_TOKEN, PropertyDefinition.PASSWORD.getString(props));
    assertEquals(GENERATED_TOKEN, IamAuthCacheHolder.tokenCache.get(PG_CACHE_KEY).getToken());
  }
}
