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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class OktaAuthPlugin extends AbstractConnectionPlugin {

  private final CredentialsProviderFactory credentialsProviderFactory;
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30;
  private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 60000;

  public static final AwsWrapperProperty IDP_ENDPOINT = new AwsWrapperProperty("idpEndpoint", null,
      "The hosting URL of the Identity Provider");
  public static final AwsWrapperProperty APP_ID = new AwsWrapperProperty("appId", null,
      "The ID of the AWS application configured on Okta");
  public static final AwsWrapperProperty IAM_ROLE_ARN =
      new AwsWrapperProperty("iamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");
  public static final AwsWrapperProperty IAM_IDP_ARN =
      new AwsWrapperProperty("iamIdpArn", null, "The ARN of the Identity Provider");
  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty("iamRegion", null,
      "Overrides AWS region that is used to generate the IAM token");
  public static final AwsWrapperProperty IAM_TOKEN_EXPIRATION = new AwsWrapperProperty("iamTokenExpiration",
      String.valueOf(DEFAULT_TOKEN_EXPIRATION_SEC), "IAM token cache expiration in seconds");
  public static final AwsWrapperProperty IDP_USERNAME =
      new AwsWrapperProperty("idpUsername", null, "The federated user name");
  public static final AwsWrapperProperty IDP_PASSWORD = new AwsWrapperProperty("idpPassword", null,
      "The federated user password");
  public static final AwsWrapperProperty IAM_HOST = new AwsWrapperProperty(
      "iamHost", null,
      "Overrides the host that is used to generate the IAM token");
  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty("iamDefaultPort", "-1",
      "Overrides default port that is used to generate the authentication token");
  public static final AwsWrapperProperty HTTP_CLIENT_SOCKET_TIMEOUT = new AwsWrapperProperty(
      "httpClientSocketTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The socket timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin");
  public static final AwsWrapperProperty HTTP_CLIENT_CONNECT_TIMEOUT = new AwsWrapperProperty(
      "httpClientConnectTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The connect timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin");
  public static final AwsWrapperProperty SSL_INSECURE = new AwsWrapperProperty("sslInsecure", "false",
      "Whether or not the SSL session is to be secure and the sever's certificates will be verified");
  public static final AwsWrapperProperty DB_USER =
      new AwsWrapperProperty("dbUser", null, "The database user used to access the database");

  private static final Logger LOGGER = Logger.getLogger(OktaAuthPlugin.class.getName());
  protected static final RegionUtils regionUtils = new RegionUtils();

  protected final PluginService pluginService;

  protected final RdsUtils rdsUtils;
  protected final SamlUtils samlUtils;
  private final IamTokenUtility iamTokenUtility;
  private final TelemetryFactory telemetryFactory;
  private final TelemetryGauge cacheSizeGauge;
  private final TelemetryCounter fetchTokenCounter;

  public OktaAuthPlugin(PluginService pluginService, CredentialsProviderFactory credentialsProviderFactory) {
    this(pluginService, credentialsProviderFactory, new RdsUtils(), IamAuthUtils.getTokenUtility());
  }

  OktaAuthPlugin(
      final PluginService pluginService,
      final CredentialsProviderFactory credentialsProviderFactory,
      final RdsUtils rdsUtils,
      final IamTokenUtility tokenUtils) {
    try {
      Class.forName("software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest");
      Class.forName("org.jsoup.nodes.Document");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("OktaAuthPlugin.requiredDependenciesMissing"));
    }

    this.pluginService = pluginService;
    this.credentialsProviderFactory = credentialsProviderFactory;
    this.rdsUtils = rdsUtils;
    this.samlUtils = new SamlUtils(this.rdsUtils);
    this.iamTokenUtility = tokenUtils;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.cacheSizeGauge = telemetryFactory.createGauge("oktaAuth.tokenCache.size",
        () -> (long) OktaAuthCacheHolder.tokenCache.size());
    this.fetchTokenCounter = telemetryFactory.createCounter("oktaAuth.fetchToken.count");
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return Collections.unmodifiableSet(new HashSet<String>() {
      {
        add("connect");
        add("forceConnect");
      }
    });
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    return connectInternal(hostSpec, props, connectFunc);
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(hostSpec, props, forceConnectFunc);
  }

  private Connection connectInternal(final HostSpec hostSpec, final Properties props,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    this.samlUtils.checkIdpCredentialsWithFallback(IDP_USERNAME, IDP_PASSWORD, props);

    final String host = IamAuthUtils.getIamHost(IAM_HOST.getString(props), hostSpec);

    final int port = IamAuthUtils.getIamPort(
        IAM_DEFAULT_PORT.getInteger(props),
        hostSpec,
        this.pluginService.getDialect().getDefaultPort());

    final Region region = regionUtils.getRegion(host, props, IAM_REGION.name);
    if (region == null) {
      throw new SQLException(
          Messages.get("OktaAuthPlugin.unableToDetermineRegion", new Object[]{ IAM_REGION.name }));
    }

    final String cacheKey = IamAuthUtils.getCacheKey(
        DB_USER.getString(props),
        host,
        port,
        region);

    final TokenInfo tokenInfo = OktaAuthCacheHolder.tokenCache.get(cacheKey);

    final boolean isCachedToken = tokenInfo != null && !tokenInfo.isExpired();

    if (isCachedToken) {
      LOGGER.finest(
          () -> Messages.get(
              "AuthenticationToken.useCachedToken",
              new Object[] {tokenInfo.getToken()}));
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      updateAuthenticationToken(hostSpec, props, region, cacheKey, host);
    }

    PropertyDefinition.USER.set(props, DB_USER.getString(props));

    try {
      return connectFunc.call();
    } catch (final SQLException exception) {
      updateAuthenticationToken(hostSpec, props, region, cacheKey, host);
      return connectFunc.call();
    } catch (final Exception exception) {
      LOGGER.warning(
          () -> Messages.get(
              "SamlAuthPlugin.unhandledException",
              new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  private void updateAuthenticationToken(
      final HostSpec hostSpec,
      final Properties props,
      final Region region,
      final String cacheKey,
      final String host)
      throws SQLException {
    final int tokenExpirationSec = IAM_TOKEN_EXPIRATION.getInteger(props);
    final Instant tokenExpiry = Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS);
    final int port = IamAuthUtils.getIamPort(
        StringUtils.isNullOrEmpty(IAM_DEFAULT_PORT.getString(props)) ? 0 : IAM_DEFAULT_PORT.getInteger(props),
        hostSpec,
        this.pluginService.getDialect().getDefaultPort());
    final AwsCredentialsProvider credentialsProvider =
        this.credentialsProviderFactory.getAwsCredentialsProvider(hostSpec.getHost(), region, props);
    this.fetchTokenCounter.inc();
    final String token = IamAuthUtils.generateAuthenticationToken(
        this.iamTokenUtility,
        this.pluginService,
        DB_USER.getString(props),
        host,
        port,
        region,
        credentialsProvider);
    LOGGER.finest(
        () -> Messages.get(
            "AuthenticationToken.useCachedToken",
            new Object[] {token}));
    PropertyDefinition.PASSWORD.set(props, token);
    OktaAuthCacheHolder.tokenCache.put(
        cacheKey,
        new TokenInfo(token, tokenExpiry));
  }

  public static void clearCache() {
    OktaAuthCacheHolder.clearCache();
  }
}
