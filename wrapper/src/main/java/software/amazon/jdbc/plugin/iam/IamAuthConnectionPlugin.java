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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class IamAuthConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(IamAuthConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30;

  public static final AwsWrapperProperty IAM_HOST = new AwsWrapperProperty(
      "iamHost", null,
      "Overrides the host that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty(
      "iamDefaultPort", "-1",
      "Overrides default port that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty(
      "iamRegion", null,
      "Overrides AWS region that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_EXPIRATION = new AwsWrapperProperty(
      "iamExpiration", String.valueOf(DEFAULT_TOKEN_EXPIRATION_SEC),
      "IAM token cache expiration in seconds");

  protected static final RegionUtils regionUtils = new RegionUtils();
  protected final PluginService pluginService;
  protected final RdsUtils rdsUtils = new RdsUtils();

  static {
    PropertyDefinition.registerPluginProperties(IamAuthConnectionPlugin.class);
  }

  private final TelemetryFactory telemetryFactory;
  private final TelemetryGauge cacheSizeGauge;
  private final TelemetryCounter fetchTokenCounter;

  private final IamTokenUtility iamTokenUtility;

  public IamAuthConnectionPlugin(final @NonNull PluginService pluginService) {
    this(pluginService, IamAuthUtils.getTokenUtility());
  }

  IamAuthConnectionPlugin(final @NonNull PluginService pluginService, IamTokenUtility utility) {
    this.iamTokenUtility = utility;
    this.pluginService = pluginService;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.cacheSizeGauge = telemetryFactory.createGauge("iam.tokenCache.size",
        () -> (long) IamAuthCacheHolder.tokenCache.size());
    this.fetchTokenCounter = telemetryFactory.createCounter("iam.fetchToken.count");
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, connectFunc);
  }

  private Connection connectInternal(String driverProtocol, HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    if (StringUtils.isNullOrEmpty(PropertyDefinition.USER.getString(props))) {
      throw new SQLException(PropertyDefinition.USER.name + " is null or empty.");
    }

    String host = IamAuthUtils.getIamHost(IAM_HOST.getString(props), hostSpec);

    int port = IamAuthUtils.getIamPort(
        IAM_DEFAULT_PORT.getInteger(props),
        hostSpec,
        this.pluginService.getDialect().getDefaultPort());

    final Region region = regionUtils.getRegion(host, props, IAM_REGION.name);
    if (region == null) {
      throw new SQLException(
          Messages.get("IamAuthConnectionPlugin.unableToDetermineRegion", new Object[]{ IAM_REGION.name }));
    }

    final int tokenExpirationSec = IAM_EXPIRATION.getInteger(props);

    final String cacheKey = IamAuthUtils.getCacheKey(
        PropertyDefinition.USER.getString(props),
        host,
        port,
        region);
    final TokenInfo tokenInfo = IamAuthCacheHolder.tokenCache.get(cacheKey);
    final boolean isCachedToken = tokenInfo != null && !tokenInfo.isExpired();

    if (isCachedToken && tokenExpirationSec > 0) {
      LOGGER.finest(
          () -> Messages.get(
              "AuthenticationToken.useCachedToken",
              new Object[] {tokenInfo.getToken()}));
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      final Instant tokenExpiry = Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS);
      this.fetchTokenCounter.inc();
      final String token = IamAuthUtils.generateAuthenticationToken(
          iamTokenUtility,
          pluginService,
          PropertyDefinition.USER.getString(props),
          host,
          port,
          region,
          AwsCredentialsManager.getProvider(hostSpec, props));
      LOGGER.finest(
          () -> Messages.get(
              "AuthenticationToken.generatedNewToken",
              new Object[] {token}));
      PropertyDefinition.PASSWORD.set(props, token);
      IamAuthCacheHolder.tokenCache.put(
          cacheKey,
          new TokenInfo(token, tokenExpiry));
    }

    try {
      return connectFunc.call();
    } catch (final SQLException exception) {

      LOGGER.finest(
          () -> Messages.get(
              "IamAuthConnectionPlugin.connectException",
              new Object[] {exception}));

      if (!this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())
          || !isCachedToken) {
        throw exception;
      }

      // Login unsuccessful with cached token
      // Try to generate a new token and try to connect again

      final Instant tokenExpiry = Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS);
      this.fetchTokenCounter.inc();
      final String token = IamAuthUtils.generateAuthenticationToken(
          iamTokenUtility,
          pluginService,
          PropertyDefinition.USER.getString(props),
          host,
          port,
          region,
          AwsCredentialsManager.getProvider(hostSpec, props));
      LOGGER.finest(
          () -> Messages.get(
              "AuthenticationToken.generatedNewToken",
              new Object[] {token}));
      PropertyDefinition.PASSWORD.set(props, token);
      IamAuthCacheHolder.tokenCache.put(
          cacheKey,
          new TokenInfo(token, tokenExpiry));

      return connectFunc.call();

    } catch (final Exception exception) {
      LOGGER.warning(
          () -> Messages.get(
              "IamAuthConnectionPlugin.unhandledException",
              new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  @Override
  public Connection forceConnect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, forceConnectFunc);
  }

  public static void clearCache() {
    IamAuthCacheHolder.clearCache();
  }
}
