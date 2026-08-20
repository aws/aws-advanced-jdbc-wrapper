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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class AwsSecretsManagerConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(AwsSecretsManagerConnectionPlugin.class.getName());
  protected static final String TELEMETRY_UPDATE_SECRETS = "fetch credentials";
  private static final String TELEMETRY_FETCH_CREDENTIALS_COUNTER = "secretsManager.fetchCredentials.count";
  private static final long CACHE_DISPOSAL_TIME_NANO = TimeUnit.MINUTES.toNanos(30);

  private static final int DEFAULT_CREDENTIALS_EXPIRATION_SEC = 15 * 60 - 30;

  /**
   * Default upper bound for the exponentially growing delay between connect retries. A configured
   * {@code secretsManagerConnectRetryIntervalMs} larger than this value raises the cap instead of
   * being clamped, so an explicitly requested interval is always honored.
   */
  private static final long MAX_CONNECT_RETRY_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.FORCECONNECT.methodName);
        }
      });

  public static final AwsWrapperProperty SECRET_ID_PROPERTY = new AwsWrapperProperty(
      "secretsManagerSecretId", null,
      "The name or the ARN of the secret to retrieve.");
  public static final AwsWrapperProperty REGION_PROPERTY = new AwsWrapperProperty(
      "secretsManagerRegion", "us-east-1",
      "The region of the secret to retrieve.");
  public static final AwsWrapperProperty ENDPOINT_PROPERTY = new AwsWrapperProperty(
      "secretsManagerEndpoint", null,
      "The endpoint of the secret to retrieve.");

  public static final AwsWrapperProperty SECRETS_MANAGER_SECRET_USERNAME_PROPERTY = new AwsWrapperProperty(
      "secretsManagerSecretUsernameProperty", "username",
      "Set this value to be the key in the JSON secret that contains the username for database connection."
  );

  public static final AwsWrapperProperty SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY = new AwsWrapperProperty(
      "secretsManagerSecretPasswordProperty", "password",
      "Set this value to be the key in the JSON secret that contains the password for database connection."
  );

  public static final AwsWrapperProperty SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY = new AwsWrapperProperty(
      "secretsManagerExpirationTimeSec", String.valueOf(DEFAULT_CREDENTIALS_EXPIRATION_SEC),
      "Secrets Manager credentials' expiration time in seconds."
  ).nonNegative();

  public static final AwsWrapperProperty SECRETS_MANAGER_CONNECT_RETRY_TIMEOUT_MS_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerConnectRetryTimeoutMs", "0",
          "Total time budget, in milliseconds, for retrying a connection that failed to log in. Before each retry "
              + "the credentials are re-fetched from AWS Secrets Manager, which allows the plugin to bridge a "
              + "secret rotation window during which AWSCURRENT has not been promoted to the new version yet. "
              + "Set to 0 (the default) to disable retrying and keep the behavior of at most one re-fetch."
      ).nonNegative();

  public static final AwsWrapperProperty SECRETS_MANAGER_CONNECT_RETRY_INTERVAL_MS_PROPERTY =
      new AwsWrapperProperty(
          "secretsManagerConnectRetryIntervalMs", "1000",
          "Initial delay, in milliseconds, before a connection that failed to log in is retried. The delay doubles "
              + "after every failed attempt, capped at 30000ms and at the remaining time budget. Only used when "
              + "secretsManagerConnectRetryTimeoutMs is greater than 0."
      ).nonNegative();

  protected static final RegionUtils regionUtils = new RegionUtils();
  private static final Pattern SECRETS_ARN_PATTERN =
      Pattern.compile("^arn:aws:secretsmanager:(?<region>[^:\\n]*):[^:\\n]*:([^:/\\n]*[:/])?(.*)$");

  final Pair<String /* secretId */, String /* region */> secretKey;
  private final BiFunction<HostSpec, Region, SecretsManagerClient>
      secretsManagerClientFunc;
  private final Function<String, GetSecretValueRequest> getSecretValueRequestFunc;
  protected @Nullable Secret secret;
  private final String secretUsername;
  private final String secretPassword;
  protected final long secretExpirationTime;
  protected final long connectRetryTimeoutMs;
  protected final long connectRetryIntervalMs;
  protected final PluginService pluginService;
  protected final FullServicesContainer servicesContainer;

  protected final @Nullable TelemetryCounter fetchCredentialsCounter;

  static {
    PropertyDefinition.registerPluginProperties(AwsSecretsManagerConnectionPlugin.class);
  }

  public AwsSecretsManagerConnectionPlugin(final FullServicesContainer servicesContainer, final Properties props) {
    this(
        servicesContainer,
        props,
        (hostSpec, region) -> {
          final String endpoint = ENDPOINT_PROPERTY.getString(props);
          if (endpoint != null && !endpoint.isEmpty()) {
            try {
              final URI endpointURI = new URI(endpoint);
              return SecretsManagerClient.builder()
                  .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props, region))
                  .endpointOverride(endpointURI)
                  .region(region)
                  .build();
            } catch (URISyntaxException e) {
              throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                  new Object[] {e.getMessage()}));
            }
          } else {
            return SecretsManagerClient.builder()
                .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props, region))
                .region(region)
                .build();
          }
        },
        (secretId) -> GetSecretValueRequest.builder()
            .secretId(secretId)
            .build()
    );
  }

  // resolveSecretExpirationTime is an overridable instance method invoked here during construction;
  // it only reads its argument and does not touch not-yet-initialized state, so the call is safe.
  @SuppressWarnings("method.invocation")
  AwsSecretsManagerConnectionPlugin(
      final FullServicesContainer servicesContainer,
      final Properties props,
      final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc,
      final Function<String, GetSecretValueRequest> getSecretValueRequestFunc) {
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();

    try {
      Class.forName("software.amazon.awssdk.services.secretsmanager.SecretsManagerClient");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.javaSdkNotInClasspath"));
    }

    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.jacksonDatabindNotInClasspath"));
    }

    this.servicesContainer.getStorageService().registerItemClassIfAbsent(
        Secret.class,
        false,
        CACHE_DISPOSAL_TIME_NANO,
        null,
        null
    );

    final String secretId = SECRET_ID_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(secretId)) {
      throw new
          RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {SECRET_ID_PROPERTY.name}));
    }

    Region region = regionUtils.getRegion(props, REGION_PROPERTY.name);
    if (region == null) {
      final Matcher matcher = SECRETS_ARN_PATTERN.matcher(secretId);
      if (matcher.matches()) {
        final String regionFromArn = matcher.group("region");
        if (regionFromArn != null) {
          region = regionUtils.getRegionFromRegionString(regionFromArn);
        }
      }
    }

    if (region == null) {
      throw new RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {REGION_PROPERTY.name}));
    }

    // Both properties are declared with non-null default values ("username"/"password"), so getString
    // never returns null; the fallback keeps the fields non-null to satisfy the checker.
    final String configuredSecretUsername =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_USERNAME_PROPERTY.getString(props);
    this.secretUsername = configuredSecretUsername != null ? configuredSecretUsername : "username";
    final String configuredSecretPassword =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY.getString(props);
    this.secretPassword = configuredSecretPassword != null ? configuredSecretPassword : "password";
    this.secretExpirationTime = resolveSecretExpirationTime(
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY.getInteger(props));
    this.connectRetryTimeoutMs =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_CONNECT_RETRY_TIMEOUT_MS_PROPERTY.getLong(props);
    this.connectRetryIntervalMs =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_CONNECT_RETRY_INTERVAL_MS_PROPERTY.getLong(props);
    this.secretKey = Pair.create(secretId, region.id());

    this.secretsManagerClientFunc = secretsManagerClientFunc;
    this.getSecretValueRequestFunc = getSecretValueRequestFunc;
    this.fetchCredentialsCounter = this.pluginService.getTelemetryFactory()
        .createCounter(TELEMETRY_FETCH_CREDENTIALS_COUNTER);
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

  private Connection connectInternal(HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    if (StringUtils.isNullOrEmpty(this.secretUsername)) {
      throw new SQLException(Messages.get("AwsSecretsManagerConnectionPlugin.emptySecretUsernameProperty"));
    }

    if (StringUtils.isNullOrEmpty(this.secretPassword)) {
      throw new SQLException(Messages.get("AwsSecretsManagerConnectionPlugin.emptySecretPasswordProperty"));
    }

    if (this.connectRetryTimeoutMs > 0) {
      return connectWithRetryBudget(hostSpec, props, connectFunc);
    }

    return connectWithSingleRetry(hostSpec, props, connectFunc);
  }

  /**
   * Opens a connection, re-fetching the credentials once and retrying if the attempt with cached
   * credentials failed to log in. This is the default behavior, used when
   * {@code secretsManagerConnectRetryTimeoutMs} is 0.
   */
  private Connection connectWithSingleRetry(HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    boolean secretWasFetched = updateSecret(hostSpec, false);

    try {
      applySecretToProperties(props);
      return connectFunc.call();

    } catch (final SQLException exception) {
      if (this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())
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
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.unhandledException",
              new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  /**
   * Opens a connection, re-fetching the credentials from AWS Secrets Manager and retrying with a
   * capped exponential backoff for as long as {@code secretsManagerConnectRetryTimeoutMs} allows,
   * whenever an attempt fails to log in.
   *
   * <p>This bridges a Secrets Manager rotation window: between {@code setSecret} (the database
   * password has already been changed) and {@code finishSecret} ({@code AWSCURRENT} is promoted to
   * the new version) neither the cached nor a freshly fetched secret can log in, so a single
   * re-fetch cannot recover. Unlike {@link #connectWithSingleRetry}, retries are also performed
   * when the very first attempt already fetched the credentials from the service, which is the case
   * for the first connection opened with an empty cache.
   *
   * <p>Each retry issues one {@code GetSecretValue} call, so the interval should be chosen with the
   * Secrets Manager request quota in mind.
   */
  private Connection connectWithRetryBudget(
      final HostSpec hostSpec,
      final Properties props,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    final ConnectRetryBackoff backoff =
        new ConnectRetryBackoff(this.connectRetryTimeoutMs, this.connectRetryIntervalMs);
    @Nullable SQLException lastLoginException = null;
    int attempt = 0;

    while (true) {
      attempt++;
      final int currentAttempt = attempt;

      try {
        // The first attempt may use cached credentials. Every later attempt forces a re-fetch, since
        // the point of retrying is to pick up a secret version promoted in the meantime.
        updateSecret(hostSpec, attempt > 1);
      } catch (final SQLException fetchException) {
        if (lastLoginException == null) {
          // Nothing has failed to log in yet, so this is a plain fetch failure. Report it exactly as
          // the non-retrying path would.
          throw fetchException;
        }
        // A transient Secrets Manager failure must not consume the whole budget. Keep the login
        // failure as the error to report and try again.
        LOGGER.log(Level.FINE, fetchException,
            () -> Messages.get("AwsSecretsManagerConnectionPlugin.connectRetryFetchFailed"));
        if (!backoff.awaitNextAttempt()) {
          logConnectRetryBudgetExhausted(currentAttempt);
          throw lastLoginException;
        }
        continue;
      }

      try {
        applySecretToProperties(props);
        final Connection connection = connectFunc.call();
        if (currentAttempt > 1) {
          LOGGER.fine(
              () -> Messages.get(
                  "AwsSecretsManagerConnectionPlugin.connectRetrySucceeded",
                  new Object[] {currentAttempt}));
        }
        return connection;

      } catch (final SQLException exception) {
        if (!this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())) {
          // Not a credentials problem, so re-fetching and retrying would not help.
          throw exception;
        }
        lastLoginException = exception;
        if (!backoff.awaitNextAttempt()) {
          logConnectRetryBudgetExhausted(currentAttempt);
          throw exception;
        }
      } catch (final Exception exception) {
        LOGGER.warning(
            () -> Messages.get(
                "AwsSecretsManagerConnectionPlugin.unhandledException",
                new Object[] {exception}));
        throw new SQLException(exception);
      }
    }
  }

  private void logConnectRetryBudgetExhausted(final int attempts) {
    LOGGER.fine(
        () -> Messages.get(
            "AwsSecretsManagerConnectionPlugin.connectRetryBudgetExhausted",
            new Object[] {attempts, this.connectRetryTimeoutMs}));
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
   * Resolves the effective credentials' expiration time (in seconds) from the configured value.
   * Subclasses may override to enforce constraints (e.g. a minimum value).
   *
   * @param configuredExpirationTime the value configured via {@code secretsManagerExpirationTimeSec}.
   * @return the expiration time to use.
   */
  protected long resolveSecretExpirationTime(final long configuredExpirationTime) {
    return configuredExpirationTime;
  }

  /**
   * Called to update credentials from the cache, or from the AWS Secrets Manager service.
   *
   * @param hostSpec     the host the credentials are being resolved for.
   * @param forceReFetch Allows ignoring cached credentials and force fetches the latest credentials from the service.
   * @return true, if credentials were fetched from the service.
   * @throws SQLException if the secret could not be fetched or parsed.
   */
  protected boolean updateSecret(final HostSpec hostSpec, final boolean forceReFetch) throws SQLException {

    TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_UPDATE_SECRETS, TelemetryTraceLevel.NESTED);
    if (this.fetchCredentialsCounter != null) {
      this.fetchCredentialsCounter.inc();
    }

    this.secret = this.servicesContainer.getStorageService().get(Secret.class, this.secretKey);

    try {
      boolean fetched = false;
      if (secret == null || forceReFetch || secret.isExpired()) {
        this.secret = fetchAndStoreSecret(hostSpec);
        fetched = this.secret != null;
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
   * Fetches the latest credentials from the AWS Secrets Manager service and stores them in the shared cache.
   * Service-specific exceptions are translated into a {@link SQLException} (or {@link RuntimeException} for a
   * misconfigured endpoint), preserving the original error handling.
   *
   * @param hostSpec A {@link HostSpec} instance containing host information for the current connection.
   * @return the freshly fetched {@link Secret}, or {@code null} if none was returned.
   * @throws SQLException if the credentials could not be fetched.
   */
  protected Secret fetchAndStoreSecret(final HostSpec hostSpec) throws SQLException {
    try {
      final Secret fetched = fetchLatestCredentials(hostSpec);
      if (fetched != null) {
        this.servicesContainer.getStorageService().set(this.secretKey, fetched);
      }
      return fetched;
    } catch (final SecretsManagerException | JsonProcessingException exception) {
      LOGGER.log(
          Level.WARNING,
          exception,
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
      throw new SQLException(
          Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"), exception);
    } catch (SdkClientException exception) {
      LOGGER.log(
          Level.WARNING,
          exception,
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
              new Object[] {exception.getMessage()}));
      throw new SQLException(
          Messages.get("AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
              new Object[] {exception.getMessage()}), exception);
    } catch (Exception exception) {
      final Throwable cause = exception.getCause();
      if (cause != null && cause instanceof URISyntaxException) {
        LOGGER.log(
            Level.WARNING,
            exception,
            () -> Messages.get(
                "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                new Object[] {cause.getMessage()}));
        throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
            new Object[] {cause.getMessage()}));
      }
      LOGGER.log(
          Level.WARNING,
          exception,
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.unhandledException",
              new Object[] {exception.getMessage()}));
      throw new SQLException(exception);
    }
  }

  /**
   * Fetches the current credentials from AWS Secrets Manager service.
   *
   * @param hostSpec A {@link HostSpec} instance containing host information for the current connection.
   * @return a Secret object containing the credentials fetched from the AWS Secrets Manager service.
   * @throws SecretsManagerException if credentials can't be fetched from the AWS Secrets Manager service.
   * @throws JsonProcessingException if credentials can't be read from the JSON object returned by the SDK.
   */
  Secret fetchLatestCredentials(final HostSpec hostSpec)
      throws SecretsManagerException, JsonProcessingException, SQLException {
    final SecretsManagerClient client = secretsManagerClientFunc.apply(
        hostSpec,
        Region.of(this.secretKey.getValue2()));
    final GetSecretValueRequest request = getSecretValueRequestFunc.apply(this.secretKey.getValue1());

    final GetSecretValueResponse valueResponse;
    try {
      valueResponse = client.getSecretValue(request);
    } finally {
      client.close();
    }

    final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueResponse.secretString());

    if (!jsonNode.has(this.secretUsername) || !jsonNode.has(this.secretPassword)) {
      throw new SQLException(Messages.get(
          "AwsSecretsManagerConnectionPlugin.invalidSecretFormat",
          new Object[] { this.secretUsername, this.secretPassword }));
    }

    final Instant secretExpiry = Instant.now().plus(this.secretExpirationTime, ChronoUnit.SECONDS);
    return new Secret(
        jsonNode.get(this.secretUsername).asText(),
        jsonNode.get(this.secretPassword).asText(),
        secretExpiry);
  }

  /**
   * Updates credentials in provided properties. Other plugins in the plugin chain may change them if needed.
   * Eventually, credentials will be used to open a new connection in {@link DefaultConnectionPlugin#connect}.
   *
   * @param properties Properties to store credentials.
   */
  private void applySecretToProperties(final Properties properties) {
    final Secret currentSecret = this.secret;
    if (currentSecret != null) {
      PropertyDefinition.USER.set(properties, currentSecret.getUsername());
      PropertyDefinition.PASSWORD.set(properties, currentSecret.getPassword());
    }
  }

  public static void clearCache() {
    CoreServicesContainer.getInstance().getStorageService().clear(Secret.class);
  }

  /**
   * Capped exponential backoff bounded by a total time budget, used by
   * {@link #connectWithRetryBudget}.
   */
  static final class ConnectRetryBackoff {
    private final long deadlineNano;
    private final long maxDelayMs;
    private long nextDelayMs;

    ConnectRetryBackoff(final long timeoutMs, final long intervalMs) {
      this.deadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
      // Never sleep longer than the whole budget, and never sleep for a non-positive duration.
      this.nextDelayMs = Math.max(1L, Math.min(intervalMs, timeoutMs));
      // An explicitly configured interval larger than the default cap raises the cap rather than
      // being clamped, so the backoff never shrinks below the requested starting interval.
      this.maxDelayMs = Math.max(this.nextDelayMs, MAX_CONNECT_RETRY_INTERVAL_MS);
    }

    /**
     * Waits until the next attempt is due.
     *
     * @return true if another attempt should be made, false if the time budget is exhausted.
     * @throws SQLException if the calling thread is interrupted while waiting. The thread's interrupt
     *     status is restored before the exception is thrown.
     */
    boolean awaitNextAttempt() throws SQLException {
      final long remainingMs = TimeUnit.NANOSECONDS.toMillis(this.deadlineNano - System.nanoTime());
      if (remainingMs <= 0) {
        return false;
      }

      try {
        TimeUnit.MILLISECONDS.sleep(Math.min(this.nextDelayMs, remainingMs));
      } catch (final InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        throw new SQLException(
            Messages.get("AwsSecretsManagerConnectionPlugin.interruptedDuringConnectRetry"),
            interruptedException);
      }

      // Double without overflowing: nextDelayMs is always <= maxDelayMs.
      this.nextDelayMs = this.nextDelayMs > this.maxDelayMs / 2
          ? this.maxDelayMs
          : this.nextDelayMs * 2;
      return true;
    }
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

    @Override
    public String toString() {
      return String.format("Secret@%s [username=%s, password=%s, expirationTime=%s]",
          Integer.toHexString(System.identityHashCode(this)),
          StringUtils.mask(this.username, 1, 1),
          this.password == null ? "<null>" : "***",
          this.expirationTime);
    }
  }
}
