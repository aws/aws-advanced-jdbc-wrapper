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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.AwsSecretsManagerConnectionPlugin.Secret;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Stale-While-Revalidate (SWR) version of the AWS Secrets Manager connection plugin.
 *
 * <p>This plugin caches credentials and always connects immediately using cached values,
 * even if they are stale (expired). When stale credentials are used, an asynchronous
 * background refresh is triggered. This ensures connections are never blocked waiting
 * for AWS Secrets Manager, and the driver remains functional during Secrets Manager outages
 * as long as cached credentials exist.
 *
 * <p>On the first connection (cache miss), a synchronous fetch is performed.
 * If the connection fails with a login error and credentials haven't been freshly fetched,
 * a synchronous re-fetch and retry is attempted.
 */
public class AwsSecretsManagerConnectionPlugin2 extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AwsSecretsManagerConnectionPlugin2.class.getName());
  private static final String TELEMETRY_UPDATE_SECRETS = "fetch credentials";
  private static final String TELEMETRY_FETCH_CREDENTIALS_COUNTER = "secretsManager.fetchCredentials.count";
  private static final int SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS = 60;
  private static final long MIN_EXPIRATION_TIME_SECONDS = 300;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });

  private static final Pattern SECRETS_ARN_PATTERN =
      Pattern.compile("^arn:aws:secretsmanager:(?<region>[^:\\n]*):[^:\\n]*:([^:/\\n]*[:/])?(.*)$");

  protected static final RegionUtils regionUtils = new RegionUtils();

  static final AtomicInteger counter = new AtomicInteger(0);
  static final ExecutorService refreshExecutor = Executors.newCachedThreadPool(r -> {
    final Thread t = new Thread(r, "aws-sm-swr-refresh-" + counter.getAndIncrement());
    t.setDaemon(true);
    return t;
  });

  static final ConcurrentMap<SecretKey, CompletableFuture<Secret>> pendingRefreshes =
      new ConcurrentHashMap<>();

  final SecretKey secretKey;
  private final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc;
  private final Function<String, GetSecretValueRequest> getSecretValueRequestFunc;
  private final String secretUsername;
  private final String secretPassword;
  private final long secretExpirationTime;
  protected final PluginService pluginService;

  private final TelemetryCounter fetchCredentialsCounter;

  static {
    PropertyDefinition.registerPluginProperties(AwsSecretsManagerConnectionPlugin2.class);
  }

  public AwsSecretsManagerConnectionPlugin2(final PluginService pluginService, final Properties props) {
    this(
        pluginService,
        props,
        (hostSpec, region) -> {
          final String endpoint =
              AwsSecretsManagerConnectionPlugin.ENDPOINT_PROPERTY.getString(props);
          if (endpoint != null && !endpoint.isEmpty()) {
            try {
              final URI endpointURI = new URI(endpoint);
              return SecretsManagerClient.builder()
                  .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props))
                  .endpointOverride(endpointURI)
                  .region(region)
                  .build();
            } catch (final URISyntaxException e) {
              throw new RuntimeException(Messages.get(
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
        (secretId) -> GetSecretValueRequest.builder()
            .secretId(secretId)
            .build()
    );
  }

  AwsSecretsManagerConnectionPlugin2(
      final PluginService pluginService,
      final Properties props,
      final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc,
      final Function<String, GetSecretValueRequest> getSecretValueRequestFunc) {
    this.pluginService = pluginService;

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

    final String secretId =
        AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(secretId)) {
      throw new RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {AwsSecretsManagerConnectionPlugin.SECRET_ID_PROPERTY.name}));
    }

    Region region = regionUtils.getRegion(props,
        AwsSecretsManagerConnectionPlugin.REGION_PROPERTY.name);
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
              new Object[] {AwsSecretsManagerConnectionPlugin.REGION_PROPERTY.name}));
    }

    this.secretUsername =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_USERNAME_PROPERTY.getString(props);
    this.secretPassword =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY.getString(props);
    final long configuredExpiration =
        AwsSecretsManagerConnectionPlugin.SECRETS_MANAGER_EXPIRATION_SEC_PROPERTY.getInteger(props);
    if (configuredExpiration < MIN_EXPIRATION_TIME_SECONDS) {
      LOGGER.warning(
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin2.expirationTimeTooLow",
              new Object[] {configuredExpiration, MIN_EXPIRATION_TIME_SECONDS}));
      this.secretExpirationTime = MIN_EXPIRATION_TIME_SECONDS;
    } else {
      this.secretExpirationTime = configuredExpiration;
    }
    this.secretKey = new SecretKey(secretId, region.id());

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

  private Connection connectInternal(final HostSpec hostSpec, final Properties props,
                                     final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    if (StringUtils.isNullOrEmpty(this.secretUsername)) {
      throw new SQLException("secretsManagerSecretUsernameProperty shouldn't be an empty string.");
    }

    if (StringUtils.isNullOrEmpty(this.secretPassword)) {
      throw new SQLException("secretsManagerSecretPasswordProperty shouldn't be an empty string.");
    }

    final SecretResult result = resolveSecret(hostSpec, false);
    applySecretToProperties(result.secret, props);

    try {
      return connectFunc.call();
    } catch (final SQLException exception) {
      if (this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())
          && !result.wasFreshlyFetched) {
        // Login unsuccessful with cached/stale credentials.
        // Try synchronous re-fetch and retry.
        try {
          final SecretResult freshSecret = resolveSecret(hostSpec, true);
          applySecretToProperties(freshSecret.secret, props);
          return connectFunc.call();
        } catch (final Exception ex) {
          exception.addSuppressed(ex);
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
   * Resolves the secret using SWR semantics:
   * - If no cached secret exists, fetch synchronously (blocking).
   * - If cached secret is not expired, use it as-is.
   * - If cached secret is expired, use it immediately and trigger an async background refresh.
   *
   * @return a {@link SecretResult} containing the secret and whether it was freshly fetched.
   */
  private SecretResult resolveSecret(final HostSpec hostSpec, final boolean forceFetch) throws SQLException {
    final TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    final TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_UPDATE_SECRETS, TelemetryTraceLevel.NESTED);
    if (telemetryContext != null) {
      telemetryContext.setAttribute("forceFetch", Boolean.toString(forceFetch));
    }

    try {
      final Secret cachedSecret = AwsSecretsManagerCacheHolder.secretsCache.get(this.secretKey.cacheKey);

      if (cachedSecret == null || forceFetch) {
        // Cache miss — first connection OR forced fetch. Must fetch synchronously.
        final Secret fetched = forceFetchSecret(hostSpec);
        return new SecretResult(fetched, true);
      }

      if (cachedSecret.isExpired()) {
        // Stale cache — use immediately, trigger async refresh in background.
        LOGGER.fine(
            () -> Messages.get(
                "AwsSecretsManagerConnectionPlugin2.usingStaleCredentials",
                new Object[] {this.secretKey.secretId}));
        triggerAsyncRefreshIfNeeded(hostSpec);
        if (telemetryContext != null) {
            telemetryContext.setAttribute("usedStaleCredentials", "true");
        }
        return new SecretResult(cachedSecret, false);
      }

      // Cache is fresh — use as-is.
      return new SecretResult(cachedSecret, false);

    } catch (final Exception ex) {
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
   * Synchronously fetches the latest credentials and updates the cache.
   *
   * @return the freshly fetched {@link Secret}.
   * @throws SQLException if fetching fails.
   */
  private Secret forceFetchSecret(final HostSpec hostSpec) throws SQLException {
    try {
      return triggerAsyncRefreshIfNeeded(hostSpec).get(SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while fetching credentials", exception);
    } catch (final TimeoutException exception) {
      throw new SQLException(
              "Timed out after " + SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS
              + "s waiting for AWS Secrets Manager credentials", exception);
    } catch (final Throwable t) {
      if (t.getCause() instanceof AwsSecretsManagerFetchException) {
        final Throwable fetchCause = t.getCause().getCause();
        if (fetchCause instanceof SdkClientException) {
          LOGGER.log(Level.WARNING, t,
                     () -> Messages.get("AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
                                        new Object[]{t.getMessage()}));
          throw new SQLException(
                  Messages.get("AwsSecretsManagerConnectionPlugin.endpointOverrideInvalidConnection",
                               new Object[]{t.getMessage()}), t);
        }
        if (fetchCause instanceof URISyntaxException) {
          LOGGER.log(Level.WARNING, t,
                     () -> Messages.get(
                             "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                             new Object[] {t.getCause().getMessage()}));
          throw new RuntimeException(Messages.get(
                  "AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured",
                  new Object[] {t.getCause().getMessage()}));
        }
        if (fetchCause instanceof SecretsManagerException || fetchCause instanceof JsonProcessingException) {
          LOGGER.log(Level.WARNING, t,
                     () -> Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
          throw new SQLException(
                  Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"), t);
        }
        if (fetchCause instanceof SQLException) {
          throw (SQLException) fetchCause;
        }
      }
      LOGGER.log(Level.WARNING, t,
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.unhandledException",
              new Object[] {t.getMessage()}));
      throw new SQLException(t);
    }
  }

  /**
   * Triggers a single asynchronous background refresh for the given secret key.
   * Uses ConcurrentHashMap.compute() to ensure at most one in-flight refresh per key
   * (thundering herd prevention).
   */
  CompletableFuture<Secret> triggerAsyncRefreshIfNeeded(final HostSpec hostSpec) {
    final AtomicBoolean isRefreshTriggered = new AtomicBoolean(false);
    final CompletableFuture<Secret> future = pendingRefreshes.compute(this.secretKey, (key, existing) -> {
      if (existing != null && !existing.isDone()) {
        return existing; // already refreshing
      }
      isRefreshTriggered.set(true);
      return supplyAsync(() -> {
        try {
          final Secret secret = fetchLatestCredentials(hostSpec);
          AwsSecretsManagerCacheHolder.secretsCache.put(this.secretKey.cacheKey, secret);
          return secret;
        } catch (final Throwable t) {
          throw new AwsSecretsManagerFetchException(t);
        }
      }, refreshExecutor);
    });
    if (isRefreshTriggered.get()) {
      future.whenComplete((fetchedSecret, ex) -> {
        if (ex != null) {
          LOGGER.log(Level.WARNING, ex,
                     () -> Messages.get(
                             "AwsSecretsManagerConnectionPlugin2.asyncRefreshFailed",
                             new Object[]{this.secretKey.secretId}));
        }
        pendingRefreshes.remove(this.secretKey, future);
      });
    }
    return future;
  }

  /**
   * Fetches the current credentials from AWS Secrets Manager service.
   */
  Secret fetchLatestCredentials(final HostSpec hostSpec)
      throws JsonProcessingException, SQLException {
    if (this.fetchCredentialsCounter != null) {
      this.fetchCredentialsCounter.inc();
    }

    final SecretsManagerClient client = secretsManagerClientFunc.apply(
        hostSpec,
        Region.of(this.secretKey.region));

    final GetSecretValueResponse valueResponse;
    try {
      final GetSecretValueRequest request = getSecretValueRequestFunc.apply(this.secretKey.secretId);
      valueResponse = client.getSecretValue(request);
    } finally {
      client.close();
    }

    final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueResponse.secretString());

    if (!jsonNode.has(this.secretUsername) || !jsonNode.has(this.secretPassword)) {
      throw new SQLException(Messages.get(
          "AwsSecretsManagerConnectionPlugin.invalidSecretFormat",
          new Object[] {this.secretUsername, this.secretPassword}));
    }

    final Instant secretExpiry = Instant.now().plus(this.secretExpirationTime, ChronoUnit.SECONDS);
    return new Secret(
        jsonNode.get(this.secretUsername).asText(),
        jsonNode.get(this.secretPassword).asText(),
        secretExpiry);
  }

  private void applySecretToProperties(final Secret secret, final Properties properties) {
    PropertyDefinition.USER.set(properties, secret.getUsername());
    PropertyDefinition.PASSWORD.set(properties, secret.getPassword());
  }

  public static void clearCache() {
    AwsSecretsManagerCacheHolder.clearCache();
  }

  static final class AwsSecretsManagerFetchException extends RuntimeException {
    AwsSecretsManagerFetchException(final Throwable cause) {
      super(cause);
    }
  }

  static final class SecretResult {
    final Secret secret;
    final boolean wasFreshlyFetched;

    SecretResult(final Secret secret, final boolean wasFreshlyFetched) {
      this.secret = secret;
      this.wasFreshlyFetched = wasFreshlyFetched;
    }
  }

  static final class SecretKey {
    final String secretId;
    final String region;
    final Pair<String, String> cacheKey;

    SecretKey(final String secretId, final String region) {
      this.secretId = secretId;
      this.region = region;
      this.cacheKey = Pair.create(secretId, region);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SecretKey that = (SecretKey) o;
      return Objects.equals(secretId, that.secretId) && Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
      return Objects.hash(secretId, region);
    }

    @Override
    public String toString() {
      return "SecretKey{secretId='" + secretId + "', region='" + region + "'}";
    }
  }
}
