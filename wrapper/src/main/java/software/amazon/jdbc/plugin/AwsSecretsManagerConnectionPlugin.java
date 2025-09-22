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

package software.amazon.jdbc.plugin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class AwsSecretsManagerConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER =
      Logger.getLogger(AwsSecretsManagerConnectionPlugin.class.getName());
  private static final String TELEMETRY_UPDATE_SECRETS = "fetch credentials";
  private static final String TELEMETRY_FETCH_CREDENTIALS_COUNTER =
      "secretsManager.fetchCredentials.count";

  private static final int DEFAULT_CREDENTIALS_EXPIRATION_SEC = 15 * 60 - 30;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("connect");
              add("forceConnect");
            }
          });

  public static final AwsWrapperProperty SECRET_ID_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerSecretId", null, "The name or the ARN of the secret to retrieve.");
  public static final AwsWrapperProperty REGION_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerRegion", "us-east-1", "The region of the secret to retrieve.");
  public static final AwsWrapperProperty ENDPOINT_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerEndpoint", null, "The endpoint of the secret to retrieve.");

  public static final AwsWrapperProperty SECRETS_MANAGER_SECRET_USERNAME_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerSecretUsernameProperty",
          "username",
          "Set this value to be the key in the JSON secret that contains the username for database connection.");

  public static final AwsWrapperProperty SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerSecretPasswordProperty",
          "password",
          "Set this value to be the key in the JSON secret that contains the password for database connection.");

  public static final AwsWrapperProperty SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerExpirationTimeSec",
          String.valueOf(DEFAULT_CREDENTIALS_EXPIRATION_SEC),
          "Secrets Manager credentials' expiration time in seconds.");

  protected static final RegionUtils regionUtils = new RegionUtils();
  private static final Pattern SECRETS_ARN_PATTERN =
      Pattern.compile("^arn:aws:secretsmanager:(?<region>[^:\\n]*):[^:\\n]*:([^:/\\n]*[:/])?(.*)$");

  final Pair<String /* secretId */, String /* region */> secretKey;
  private final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc;
  private final Function<String, GetSecretValueRequest> getSecretValueRequestFunc;
  private Secret secret;
  private final String secretUsername;
  private final String secretPassword;
  private final long secretExpirationTime;
  protected PluginService pluginService;

  private final TelemetryCounter fetchCredentialsCounter;

  static {
    PropertyDefinition.registerPluginProperties(AwsSecretsManagerConnectionPlugin.class);
  }

  public AwsSecretsManagerConnectionPlugin(
      final PluginService pluginService, final Properties props) {
    this(
        pluginService,
        props,
        (hostSpec, region) -> {
          final String endpoint = ENDPOINT_PROPERTY.getString(props);
          if (endpoint != null && !endpoint.isEmpty()) {
            try {
              final URI endpointURI = new URI(endpoint);
              return SecretsManagerClient.builder()
                  .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props))
                  .endpointOverride(endpointURI)
                  .region(region)
                  .build();
            } catch (URISyntaxException e) {
              throw new RuntimeException(
                  Messages.get(
                      "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                      new Object[] {e.getMessage()}));
            }
          } else {
            return SecretsManagerClient.builder()
                .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props))
                .region(region)
                .build();
          }
        },
        (secretId) -> GetSecretValueRequest.builder().secretId(secretId).build());
  }

  AwsSecretsManagerConnectionPlugin(
      final PluginService pluginService,
      final Properties props,
      final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc,
      final Function<String, GetSecretValueRequest> getSecretValueRequestFunc) {
    this.pluginService = pluginService;

    try {
      Class.forName("software.amazon.awssdk.services.secretsmanager.SecretsManagerClient");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(
          Messages.get("AwsSecretsManagerConnectionPlugin.javaSdkNotInClasspath"));
    }

    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(
          Messages.get("AwsSecretsManagerConnectionPlugin.jacksonDatabindNotInClasspath"));
    }

    final String secretId = SECRET_ID_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(secretId)) {
      throw new RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {SECRET_ID_PROPERTY.name}));
    }

    Region region = regionUtils.getRegion(props, REGION_PROPERTY.name);
    if (region == null) {
      final Matcher matcher = SECRETS_ARN_PATTERN.matcher(secretId);
      if (matcher.matches()) {
        region = regionUtils.getRegionFromRegionString(matcher.group("region"));
      }
    }

    if (region == null) {
      throw new RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {REGION_PROPERTY.name}));
    }

    this.secretUsername =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_USERNAME_PROPERTY.getString(props);
    this.secretPassword =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY.getString(props);
    this.secretExpirationTime =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY.getInteger(props);
    this.secretKey = Pair.create(secretId, region.id());

    this.secretsManagerClientFunc = secretsManagerClientFunc;
    this.getSecretValueRequestFunc = getSecretValueRequestFunc;
    this.fetchCredentialsCounter =
        this.pluginService.getTelemetryFactory().createCounter(TELEMETRY_FETCH_CREDENTIALS_COUNTER);
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
    return connectInternal(hostSpec, props, connectFunc);
  }

  private Connection connectInternal(
      HostSpec hostSpec, Properties props, JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    if (StringUtils.isNullOrEmpty(this.secretUsername)) {
      throw new SQLException("secretsManagerSecretUsernameProperty shouldn't be an empty string.");
    }

    if (StringUtils.isNullOrEmpty(this.secretPassword)) {
      throw new SQLException("secretsManagerSecretPasswordProperty shouldn't be an empty string.");
    }

    boolean secretWasFetched = updateSecret(hostSpec, false);

    try {
      applySecretToProperties(props);
      return connectFunc.call();

    } catch (final SQLException exception) {
      if (this.pluginService.isLoginException(
              exception, this.pluginService.getTargetDriverDialect())
          && !secretWasFetched) {
        // Login unsuccessful with cached credentials
        // Try to re-fetch credentials and try again

        secretWasFetched = updateSecret(hostSpec, true);
        if (secretWasFetched) {
          applySecretToProperties(props);
          return connectFunc.call();
        }
      }

      throw exception;
    } catch (final Exception exception) {
      LOGGER.warning(
          () ->
              Messages.get(
                  "AwsSecretsManagerConnectionPlugin.unhandledException",
                  new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  @Override
  public Connection forceConnect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(hostSpec, props, forceConnectFunc);
  }

  /**
   * Called to update credentials from the cache, or from the AWS Secrets Manager service.
   *
   * @param forceReFetch Allows ignoring cached credentials and force fetches the latest credentials
   *     from the service.
   * @return true, if credentials were fetched from the service.
   */
  private boolean updateSecret(final HostSpec hostSpec, final boolean forceReFetch)
      throws SQLException {

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext =
        telemetryFactory.openTelemetryContext(TELEMETRY_UPDATE_SECRETS, TelemetryTraceLevel.NESTED);
    if (this.fetchCredentialsCounter != null) {
      this.fetchCredentialsCounter.inc();
    }

    this.secret = AwsSecretsManagerCacheHolder.secretsCache.get(this.secretKey);

    try {
      boolean fetched = false;
      if (secret == null || forceReFetch || secret.isExpired()) {
        try {
          this.secret = fetchLatestCredentials(hostSpec);
          if (this.secret != null) {
            fetched = true;
            AwsSecretsManagerCacheHolder.secretsCache.put(this.secretKey, this.secret);
          }
        } catch (final SecretsManagerException | JsonProcessingException exception) {
          LOGGER.log(
              Level.WARNING,
              exception,
              () -> Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
          throw new SQLException(
              Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"),
              exception);
        } catch (SdkClientException exception) {
          LOGGER.log(
              Level.WARNING,
              exception,
              () ->
                  Messages.get(
                      "AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
                      new Object[] {exception.getMessage()}));
          throw new SQLException(
              Messages.get(
                  "AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
                  new Object[] {exception.getMessage()}),
              exception);
        } catch (Exception exception) {
          if (exception.getCause() != null && exception.getCause() instanceof URISyntaxException) {
            LOGGER.log(
                Level.WARNING,
                exception,
                () ->
                    Messages.get(
                        "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                        new Object[] {exception.getCause().getMessage()}));
            throw new RuntimeException(
                Messages.get(
                    "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                    new Object[] {exception.getCause().getMessage()}));
          }
          LOGGER.log(
              Level.WARNING,
              exception,
              () ->
                  Messages.get(
                      "AwsSecretsManagerConnectionPlugin.unhandledException",
                      new Object[] {exception.getMessage()}));
          throw new SQLException(exception);
        }
      }
      return fetched;
    } catch (Exception ex) {
      if (telemetryContext != null) {
        telemetryContext.setSuccess(false);
        telemetryContext.setException(ex);
      }
      throw ex;
    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
  }

  /**
   * Fetches the current credentials from AWS Secrets Manager service.
   *
   * @param hostSpec A {@link HostSpec} instance containing host information for the current
   *     connection.
   * @return a Secret object containing the credentials fetched from the AWS Secrets Manager
   *     service.
   * @throws SecretsManagerException if credentials can't be fetched from the AWS Secrets Manager
   *     service.
   * @throws JsonProcessingException if credentials can't be read from the JSON object returned by
   *     the SDK.
   */
  Secret fetchLatestCredentials(final HostSpec hostSpec)
      throws SecretsManagerException, JsonProcessingException, SQLException {
    final SecretsManagerClient client =
        secretsManagerClientFunc.apply(hostSpec, Region.of(this.secretKey.getValue2()));
    final GetSecretValueRequest request =
        getSecretValueRequestFunc.apply(this.secretKey.getValue1());

    final GetSecretValueResponse valueResponse;
    try {
      valueResponse = client.getSecretValue(request);
    } finally {
      client.close();
    }

    final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueResponse.secretString());

    if (!jsonNode.has(this.secretUsername) || !jsonNode.has(this.secretPassword)) {
      throw new SQLException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.invalidSecretFormat",
              new Object[] {this.secretUsername, this.secretPassword}));
    }

    final Instant secretExpiry = Instant.now().plus(this.secretExpirationTime, ChronoUnit.SECONDS);
    return new Secret(
        jsonNode.get(this.secretUsername).asText(),
        jsonNode.get(this.secretPassword).asText(),
        secretExpiry);
  }

  /**
   * Updates credentials in provided properties. Other plugins in the plugin chain may change them
   * if needed. Eventually, credentials will be used to open a new connection in {@link
   * DefaultConnectionPlugin#connect}.
   *
   * @param properties Properties to store credentials.
   */
  private void applySecretToProperties(final Properties properties) {
    if (this.secret != null) {
      PropertyDefinition.USER.set(properties, secret.getUsername());
      PropertyDefinition.PASSWORD.set(properties, secret.getPassword());
    }
  }

  public static void clearCache() {
    AwsSecretsManagerCacheHolder.clearCache();
  }

  static class Secret {
    private final String username;
    private final String password;
    private final Instant expirationTime;

    Secret(final String username, final String password, Instant expirationTimeSec) {
      this.username = username;
      this.password = password;
      this.expirationTime = expirationTimeSec;
    }

    String getUsername() {
      return this.username;
    }

    String getPassword() {
      return this.password;
    }

    boolean isExpired() {
      return this.expirationTime != null && this.expirationTime.isBefore(Instant.now());
    }
  }
}
