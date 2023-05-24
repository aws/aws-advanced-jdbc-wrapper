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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class IamAuthPrefetchConnectionPlugin extends IamAuthConnectionPlugin implements
    CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(IamAuthPrefetchConnectionPlugin.class.getName());

  static final Map<String, String> tokenCache = new HashMap<>();
  static final ConcurrentHashMap<String, Future<?>> cacheThread = new ConcurrentHashMap<>();

  static final ExecutorService executorService = Executors.newCachedThreadPool(
      r -> {
        final Thread prefetchThread = new Thread(r);
        prefetchThread.setDaemon(true);
        return prefetchThread;
      });

  public IamAuthPrefetchConnectionPlugin(@NonNull PluginService pluginService) {
    super(pluginService);
  }

  @Override
  protected Connection connectInternal(String driverProtocol, HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    final long startTime = System.nanoTime();

    if (StringUtils.isNullOrEmpty(PropertyDefinition.USER.getString(props))) {
      throw new SQLException(PropertyDefinition.USER.name + " is null or empty.");
    }

    String host = hostSpec.getHost();
    if (!StringUtils.isNullOrEmpty(IAM_HOST.getString(props))) {
      host = IAM_HOST.getString(props);
    }

    int port = hostSpec.getPort();
    if (!hostSpec.isPortSpecified()) {
      if (StringUtils.isNullOrEmpty(IAM_DEFAULT_PORT.getString(props))) {
        port = this.pluginService.getDialect().getDefaultPort();
      } else {
        port = IAM_DEFAULT_PORT.getInteger(props);
        if (port <= 0) {
          throw new IllegalArgumentException(
              Messages.get(
                  "IamAuthConnectionPlugin.invalidPort",
                  new Object[] {port}));
        }
      }
    }

    final String iamRegion = IAM_REGION.getString(props);
    final Region region = StringUtils.isNullOrEmpty(iamRegion)
        ? getRdsRegion(host)
        : Region.of(iamRegion);

    final int tokenExpirationSec = IAM_EXPIRATION.getInteger(props);

    final String cacheKey = getCacheKey(
        PropertyDefinition.USER.getString(props),
        host,
        port,
        region);
    final String token = tokenCache.get(cacheKey);
    final boolean isCachedToken = token != null;

    if (isCachedToken) {
      LOGGER.finest(
          () -> Messages.get(
              "IamAuthConnectionPlugin.useCachedIamToken",
              new Object[] {token}));
    } else {
      updateToken(cacheKey, fetchNewToken(
          hostSpec,
          props,
          host,
          port,
          region));
      final String threadHost = host;
      final int threadPort = port;
      cacheThread.putIfAbsent(
          cacheKey,
          executorService.submit(() -> {
            while (true) {
              TimeUnit.SECONDS.sleep(tokenExpirationSec);
              tokenCache.computeIfPresent(cacheKey, (k, v) -> fetchNewToken(
                  hostSpec,
                  props,
                  threadHost,
                  threadPort,
                  region));
            }
          })
      );
    }

    try {
      PropertyDefinition.PASSWORD.set(props, tokenCache.get(cacheKey));
      final Connection conn = connectFunc.call();
      final long elapsedTimeNanos = System.nanoTime() - startTime;
      LOGGER.info("IAM Connection Time in nanoseconds: " + elapsedTimeNanos);
      return conn;
    } catch (final SQLException exception) {
      LOGGER.finest(
          () -> Messages.get(
              "IamAuthConnectionPlugin.connectException",
              new Object[] {exception}));

      if (!this.pluginService.isLoginException(exception) || !isCachedToken) {
        throw exception;
      }

      updateToken(cacheKey, fetchNewToken(
          hostSpec,
          props,
          host,
          port,
          region));
      PropertyDefinition.PASSWORD.set(props, token);

      final Connection conn = connectFunc.call();
      final long elapsedTimeNanos = System.nanoTime() - startTime;
      LOGGER.fine("IAM Connection Time in nanoseconds: " + elapsedTimeNanos);
      return conn;

    } catch (final Exception exception) {
      LOGGER.warning(
          () -> Messages.get(
              "IamAuthConnectionPlugin.unhandledException",
              new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  private String fetchNewToken(
      final HostSpec hostSpec,
      final Properties props,
      final String host,
      final int port,
      final Region region) {
    final String newToken = generateAuthenticationToken(
        hostSpec,
        props,
        host,
        port,
        region);
    LOGGER.finest(
        () -> Messages.get(
            "IamAuthConnectionPlugin.generatedNewIamToken",
            new Object[] {newToken}));
    return newToken;
  }

  private void updateToken(final String cacheKey, final String token) {
    tokenCache.put(cacheKey, token);
  }

  @Override
  public void releaseResources() {
    cacheThread.forEach((k, thread) -> thread.cancel(true));
    tokenCache.clear();
    cacheThread.clear();
  }
}
