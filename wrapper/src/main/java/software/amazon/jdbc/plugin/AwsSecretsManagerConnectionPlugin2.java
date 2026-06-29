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

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

/**
 * Stale-While-Revalidate (SWR) variant of {@link AwsSecretsManagerConnectionPlugin}.
 *
 * <p>This plugin reuses all of the credential fetching, caching, parsing, and error handling of the
 * original plugin, and only changes the cache-refresh strategy:
 *
 * <ul>
 *   <li><b>Cache hit (fresh):</b> use cached credentials immediately (same as v1).</li>
 *   <li><b>Cache hit (stale):</b> use the stale credentials immediately and trigger a single
 *       asynchronous background refresh. The connection is never blocked.</li>
 *   <li><b>Cache miss (first connection):</b> fetch synchronously (same as v1).</li>
 *   <li><b>Login failure with stale credentials:</b> the inherited connect logic performs a
 *       synchronous re-fetch and retries the connection.</li>
 * </ul>
 *
 * <p>At most one in-flight refresh exists per {@code (secretId, region)} key
 * ({@link #pendingRefreshes}), which prevents a thundering herd of Secrets Manager calls when many
 * connections are opened concurrently with an expired cache.
 *
 * <p>Both plugins share the same credential cache (the {@code StorageService}), so credentials
 * cached by one are visible to the other.
 */
public class AwsSecretsManagerConnectionPlugin2 extends AwsSecretsManagerConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(AwsSecretsManagerConnectionPlugin2.class.getName());
  private static final int SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS = 60;
  protected static final long MIN_EXPIRATION_TIME_SECONDS = 300;

  /** Daemon thread pool shared by all instances for background credential refreshes. */
  static final ExecutorService refreshExecutor = ExecutorFactory.newCachedThreadPool("aws-sm2-refresh");

  /** At most one in-flight refresh per {@code (secretId, region)} key. */
  static final ConcurrentMap<Pair<String, String>, CompletableFuture<Secret>> pendingRefreshes =
      new ConcurrentHashMap<>();

  public AwsSecretsManagerConnectionPlugin2(final FullServicesContainer servicesContainer, final Properties props) {
    super(servicesContainer, props);
  }

  AwsSecretsManagerConnectionPlugin2(
      final FullServicesContainer servicesContainer,
      final Properties props,
      final BiFunction<HostSpec, Region, SecretsManagerClient> secretsManagerClientFunc,
      final Function<String, GetSecretValueRequest> getSecretValueRequestFunc) {
    super(servicesContainer, props, secretsManagerClientFunc, getSecretValueRequestFunc);
  }

  /**
   * Clamp the configured expiration time to a minimum to avoid excessively frequent background
   * refresh triggers. Values below the minimum are raised with a warning.
   */
  @Override
  protected long resolveSecretExpirationTime(final long configuredExpirationTime) {
    if (configuredExpirationTime < MIN_EXPIRATION_TIME_SECONDS) {
      LOGGER.warning(
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin2.expirationTimeTooLow",
              new Object[] {configuredExpirationTime, MIN_EXPIRATION_TIME_SECONDS}));
      return MIN_EXPIRATION_TIME_SECONDS;
    }
    return configuredExpirationTime;
  }

  /**
   * SWR override of the inherited cache-refresh logic.
   *
   * @param forceReFetch when {@code true} (login-failure retry path) a synchronous fetch is always
   *                     performed; otherwise stale credentials are served immediately while a
   *                     background refresh runs.
   * @return {@code true} only when credentials were freshly fetched from the service, matching the
   *         contract of the superclass (a {@code true} result suppresses the login-failure retry).
   */
  @Override
  protected boolean updateSecret(final HostSpec hostSpec, final boolean forceReFetch) throws SQLException {
    final TelemetryFactory telemetryFactory = this.pluginService.getTelemetryFactory();
    final TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_UPDATE_SECRETS, TelemetryTraceLevel.NESTED);
    if (telemetryContext != null) {
      telemetryContext.setAttribute("forceFetch", Boolean.toString(forceReFetch));
    }

    try {
      final Secret cachedSecret = this.servicesContainer.getStorageService().get(Secret.class, this.secretKey);

      if (cachedSecret == null || forceReFetch) {
        // Cache miss (first connection) or forced re-fetch (login-failure retry): block once.
        this.secret = fetchSynchronously(hostSpec);
        return this.secret != null;
      }

      if (cachedSecret.isExpired()) {
        // Stale cache: serve immediately, refresh in the background.
        LOGGER.fine(
            () -> Messages.get(
                "AwsSecretsManagerConnectionPlugin2.usingStaleCredentials",
                new Object[] {this.secretKey.getValue1()}));
        if (telemetryContext != null) {
          telemetryContext.setAttribute("usedStaleCredentials", "true");
        }
        this.secret = cachedSecret;
        triggerAsyncRefresh(hostSpec);
        return false;
      }

      // Cache fresh: use as-is.
      this.secret = cachedSecret;
      return false;

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
   * Synchronously obtains the latest credentials, joining an in-flight background refresh if one
   * exists (so a login-failure retry does not start a redundant fetch).
   */
  private Secret fetchSynchronously(final HostSpec hostSpec) throws SQLException {
    try {
      return triggerAsyncRefresh(hostSpec).get(SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new SQLException(
          Messages.get("AwsSecretsManagerConnectionPlugin2.interruptedWhileFetching"), exception);
    } catch (final TimeoutException exception) {
      throw new SQLException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin2.fetchTimeout",
              new Object[] {SYNC_FETCH_CREDENTIALS_TIMEOUT_SECONDS}),
          exception);
    } catch (final ExecutionException exception) {
      final Throwable cause = exception.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      if (cause instanceof RuntimeException) {
        // Preserves the superclass behavior of throwing RuntimeException for a misconfigured endpoint.
        throw (RuntimeException) cause;
      }
      throw new SQLException(cause != null ? cause : exception);
    }
  }

  /**
   * Triggers a single asynchronous background refresh for this secret key. Uses
   * {@link ConcurrentMap#compute} to guarantee at most one in-flight refresh per key. The actual
   * Secrets Manager call (and the {@code fetchCredentials} telemetry counter increment) happens
   * inside the refresh task, so the counter reflects real API calls only.
   *
   * @return the future for the (possibly already running) refresh.
   */
  CompletableFuture<Secret> triggerAsyncRefresh(final HostSpec hostSpec) {
    final AtomicBoolean triggered = new AtomicBoolean(false);
    final CompletableFuture<Secret> future = pendingRefreshes.compute(this.secretKey, (key, existing) -> {
      if (existing != null && !existing.isDone()) {
        return existing; // a refresh is already in flight
      }
      triggered.set(true);
      return CompletableFuture.supplyAsync(() -> {
        try {
          if (this.fetchCredentialsCounter != null) {
            this.fetchCredentialsCounter.inc();
          }
          return fetchAndStoreSecret(hostSpec);
        } catch (final SQLException e) {
          throw new CompletionException(e);
        }
      }, refreshExecutor);
    });

    if (triggered.get()) {
      future.whenComplete((fetchedSecret, ex) -> {
        if (ex != null) {
          LOGGER.log(Level.FINE, ex,
              () -> Messages.get(
                  "AwsSecretsManagerConnectionPlugin2.asyncRefreshFailed",
                  new Object[] {this.secretKey.getValue1()}));
        }
        pendingRefreshes.remove(this.secretKey, future);
      });
    }
    return future;
  }
}
