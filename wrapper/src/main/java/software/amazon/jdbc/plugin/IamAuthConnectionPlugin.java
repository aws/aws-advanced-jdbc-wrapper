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
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class IamAuthConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(IamAuthConnectionPlugin.class.getName());
  static final ConcurrentHashMap<String, TokenInfo> tokenCache = new ConcurrentHashMap<>();
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60;
  public static final int PG_PORT = 5432;
  public static final int MYSQL_PORT = 3306;

  public static final AwsWrapperProperty IAM_HOST = new AwsWrapperProperty(
      "iamHost", null,
      "Overrides the host that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty(
          "iamDefaultPort", null,
          "Overrides default port that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty(
          "iamRegion", null,
          "Overrides AWS region that is used to generate the IAM token");

  public static final AwsWrapperProperty IAM_EXPIRATION = new AwsWrapperProperty(
          "iamExpiration", String.valueOf(DEFAULT_TOKEN_EXPIRATION_SEC),
          "IAM token cache expiration in seconds");

  protected final RdsUtils rdsUtils = new RdsUtils();

  @Override
  public Set<String> getSubscribedMethods() {
    return new HashSet<>(Collections.singletonList("connect"));
  }

  @Override
  public Connection connect(
          final String driverProtocol,
          final HostSpec hostSpec,
          final Properties props,
          final boolean isInitialConnection,
          final JdbcCallable<Connection, SQLException> connectFunc)
          throws SQLException {

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
        if (!driverProtocol.startsWith("jdbc:postgresql:") && !driverProtocol.startsWith("jdbc:mysql:")) {
          throw new RuntimeException(Messages.get("IamAuthConnectionPlugin.missingPort"));
        } else if (driverProtocol.startsWith("jdbc:mysql:")) {
          port = MYSQL_PORT;
        } else {
          port = PG_PORT;
        }
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
    final TokenInfo tokenInfo = tokenCache.get(cacheKey);

    if (tokenInfo != null && !tokenInfo.isExpired()) {
      LOGGER.finest(
          () -> Messages.get(
              "IamAuthConnectionPlugin.useCachedIamToken",
              new Object[] {tokenInfo.getToken()}));
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      final String token = generateAuthenticationToken(
          PropertyDefinition.USER.getString(props),
          host,
          port,
          region);
      LOGGER.finest(
          () -> Messages.get(
              "IamAuthConnectionPlugin.generatedNewIamToken",
              new Object[] {token}));
      PropertyDefinition.PASSWORD.set(props, token);
      tokenCache.put(
              cacheKey,
              new TokenInfo(token, Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS)));
    }
    return connectFunc.call();
  }

  String generateAuthenticationToken(
          final String user,
          final String hostname,
          final int port,
          final Region region) {
    final RdsUtilities utilities = RdsUtilities.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(region)
            .build();
    return utilities.generateAuthenticationToken((builder) ->
            builder
                    .hostname(hostname)
                    .port(port)
                    .username(user)
    );
  }

  private String getCacheKey(
          final String user,
          final String hostname,
          final int port,
          final Region region) {

    return String.format("%s:%s:%d:%s", region, hostname, port, user);
  }

  static void clearCache() {
    tokenCache.clear();
  }

  private Region getRdsRegion(final String hostname) throws SQLException {

    // Get Region
    final String rdsRegion = rdsUtils.getRdsRegion(hostname);

    if (StringUtils.isNullOrEmpty(rdsRegion)) {
      // Does not match Amazon's Hostname, throw exception
      final String exceptionMessage = Messages.get(
          "IamAuthConnectionPlugin.unsupportedHostname",
          new Object[] {hostname});

      LOGGER.fine(() -> exceptionMessage);
      throw new SQLException(exceptionMessage);
    }

    // Check Region
    final Optional<Region> regionOptional = Region.regions().stream()
            .filter(r -> r.id().equalsIgnoreCase(rdsRegion))
            .findFirst();

    if (!regionOptional.isPresent()) {
      final String exceptionMessage = Messages.get(
          "IamAuthConnectionPlugin.unsupportedRegion",
          new Object[] {rdsRegion});

      LOGGER.fine(() -> exceptionMessage);
      throw new SQLException((exceptionMessage));
    }

    return regionOptional.get();
  }

  static class TokenInfo {

    private final String token;
    private final Instant expiration;

    public TokenInfo(final String token, final Instant expiration) {
      this.token = token;
      this.expiration = expiration;
    }

    public String getToken() {
      return this.token;
    }

    public Instant getExpiration() {
      return this.expiration;
    }

    public boolean isExpired() {
      return Instant.now().isAfter(this.expiration);
    }
  }
}
