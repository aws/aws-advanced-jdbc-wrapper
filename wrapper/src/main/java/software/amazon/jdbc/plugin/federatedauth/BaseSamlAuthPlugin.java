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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.GDBRegionUtils;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public abstract class BaseSamlAuthPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(BaseSamlAuthPlugin.class.getName());
  private static final long DISPOSAL_TIME_NANO = TimeUnit.MINUTES.toNanos(30);

  protected static final String IAM_REGION_NAME = "iamRegion";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });

  private final IamTokenUtility iamTokenUtility;
  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final CredentialsProviderFactory credentialsProviderFactory;
  protected final RdsUtils rdsUtils;
  protected final SamlUtils samlUtils;
  protected final TelemetryFactory telemetryFactory;

  protected RegionUtils regionUtils;
  protected TelemetryGauge cacheSizeGauge;
  protected TelemetryCounter fetchTokenCounter;

  BaseSamlAuthPlugin(
      final FullServicesContainer servicesContainer,
      final CredentialsProviderFactory credentialsProviderFactory,
      final RdsUtils rdsUtils,
      final IamTokenUtility tokenUtils) {
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.credentialsProviderFactory = credentialsProviderFactory;
    this.rdsUtils = rdsUtils;
    this.samlUtils = new SamlUtils(this.rdsUtils);
    this.iamTokenUtility = tokenUtils;
    this.telemetryFactory = pluginService.getTelemetryFactory();

    this.servicesContainer.getStorageService().registerItemClassIfAbsent(
        TokenInfo.class,
        true,
        DISPOSAL_TIME_NANO,
        null,
        null
    );
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return BaseSamlAuthPlugin.subscribedMethods;
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

  protected Connection connectInternal(final HostSpec hostSpec, final Properties props,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    this.checkIdpCredentialsWithFallback(props);

    final HostSpec host = IamAuthUtils.getIamHost(this.getIamHostProperty(props), hostSpec);

    final int port = IamAuthUtils.getIamPort(
        this.getIamPortProperty(props),
        hostSpec,
        this.pluginService.getDialect().getDefaultPort());

    final RdsUrlType type = rdsUtils.identifyRdsType(host.getHost());

    AwsCredentialsProvider credentialsProvider = null;
    if (RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER == type) {
      credentialsProvider =
          this.credentialsProviderFactory.getAwsCredentialsProvider(hostSpec.getHost(), null, props);
      this.regionUtils = new GDBRegionUtils(credentialsProvider);
    } else {
      this.regionUtils = new RegionUtils();
    }

    final Region region = this.regionUtils.getRegion(host, props, BaseSamlAuthPlugin.IAM_REGION_NAME);
    if (region == null) {
      throw new SQLException(
          Messages.get("OktaAuthPlugin.unableToDetermineRegion", new Object[] {BaseSamlAuthPlugin.IAM_REGION_NAME}));
    }

    final String cacheKey = IamAuthUtils.getCacheKey(
        this.getDbUserProperty(props),
        host.getHost(),
        port,
        region);

    final TokenInfo tokenInfo = this.servicesContainer.getStorageService().get(TokenInfo.class, cacheKey);

    final boolean isCachedToken = tokenInfo != null && !tokenInfo.isExpired();

    if (isCachedToken) {
      LOGGER.finest(
          () -> Messages.get(
              "AuthenticationToken.useCachedToken",
              new Object[] {tokenInfo.getToken()}));
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      updateAuthenticationToken(hostSpec, props, region, cacheKey, host.getHost(), credentialsProvider);
    }

    PropertyDefinition.USER.set(props, this.getDbUserProperty(props));

    try {
      return connectFunc.call();
    } catch (final SQLException exception) {
      if (!isCachedToken
          || !this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())) {
        throw exception;
      }
      updateAuthenticationToken(hostSpec, props, region, cacheKey, host.getHost(), credentialsProvider);
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
      final String host,
      AwsCredentialsProvider credentialsProvider)
      throws SQLException {
    final int tokenExpirationSec = this.getIamTokenExpiration(props);
    final Instant tokenExpiry = Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS);
    final int port = IamAuthUtils.getIamPort(
        this.getIamPortProperty(props),
        hostSpec,
        this.pluginService.getDialect().getDefaultPort());
    if (credentialsProvider == null) {
      // Assume a role early so we can use this credential to fetch region information.
      credentialsProvider = this.credentialsProviderFactory.getAwsCredentialsProvider(hostSpec.getHost(), region,
          props);
    }

    if (this.fetchTokenCounter != null) {
      this.fetchTokenCounter.inc();
    }
    final String token = IamAuthUtils.generateAuthenticationToken(
        this.iamTokenUtility,
        this.pluginService,
        this.getDbUserProperty(props),
        host,
        port,
        region,
        credentialsProvider);
    LOGGER.finest(
        () -> Messages.get(
            "AuthenticationToken.generatedNewToken",
            new Object[] {token}));
    PropertyDefinition.PASSWORD.set(props, token);
    this.servicesContainer.getStorageService().set(
        cacheKey,
        new TokenInfo(token, tokenExpiry));
  }

  public static void clearCache() {
    CoreServicesContainer.getInstance().getStorageService().clear(TokenInfo.class);
  }

  // Abstract methods to retrieve configuration parameters instead of moving them to this class.
  // This is to preserve backwards compatibility.

  abstract String getDbUserProperty(Properties props);

  abstract String getIamHostProperty(Properties props);

  abstract int getIamPortProperty(Properties props);

  abstract int getIamTokenExpiration(Properties props);

  abstract void checkIdpCredentialsWithFallback(Properties props);
}
