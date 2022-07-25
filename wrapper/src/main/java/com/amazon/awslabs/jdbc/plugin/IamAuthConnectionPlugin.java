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

package com.amazon.awslabs.jdbc.plugin;

import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.PropertyDefinition;
import com.amazon.awslabs.jdbc.ProxyDriverProperty;
import com.amazon.awslabs.jdbc.util.StringUtils;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;

public class IamAuthConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(IamAuthConnectionPlugin.class.getName());
  static final ConcurrentHashMap<String, TokenInfo> tokenCache = new ConcurrentHashMap<>();
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60;
  public static final int PG_PORT = 5342;
  public static final int MYSQL_PORT = 3306;

  protected static final ProxyDriverProperty SPECIFIED_PORT = new ProxyDriverProperty(
          "iamDefaultPort", null,
          "Overrides default port that is used to generate IAM token");

  protected static final ProxyDriverProperty SPECIFIED_REGION = new ProxyDriverProperty(
          "iamRegion", null,
          "Overrides AWS region that is used to generate IAM token");

  protected static final ProxyDriverProperty SPECIFIED_EXPIRATION = new ProxyDriverProperty(
          "iamExpiration", null,
          "IAM token cache expiration in seconds");

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

    final String host = hostSpec.getHost();

    int port = hostSpec.getPort();
    if (!hostSpec.isPortSpecified()) {
      if (StringUtils.isNullOrEmpty(SPECIFIED_PORT.get(props))) {
        if (!driverProtocol.startsWith("jdbc:postgresql:") && !driverProtocol.startsWith("jdbc:mysql:")) {
          throw new RuntimeException("Port is required");
        } else if (driverProtocol.startsWith("jdbc:mysql:")) {
          port = MYSQL_PORT;
        } else {
          port = PG_PORT;
        }
      } else {
        port = SPECIFIED_PORT.getInteger(props);
        if (port <= 0) {
          throw new IllegalArgumentException("Port number: " + port + " is not valid. "
              + "Port number should be greater than zero.");
        }
      }
    }

    final Region region;
    if (StringUtils.isNullOrEmpty(SPECIFIED_REGION.get(props))) {
      region = getRdsRegion(host);
    } else {
      region = Region.of(SPECIFIED_REGION.get(props));
    }

    final int tokenExpirationSec;
    if (StringUtils.isNullOrEmpty(SPECIFIED_EXPIRATION.get(props))) {
      tokenExpirationSec = DEFAULT_TOKEN_EXPIRATION_SEC;
    } else {
      tokenExpirationSec = SPECIFIED_EXPIRATION.getInteger(props);
    }

    final String cacheKey = getCacheKey(
            PropertyDefinition.USER.getString(props),
            host,
            port,
            region);
    final TokenInfo tokenInfo = tokenCache.get(cacheKey);

    if (tokenInfo != null && !tokenInfo.isExpired()) {
      LOGGER.log(Level.FINEST, "use cached IAM token = " + tokenInfo.getToken());
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      final String token = generateAuthenticationToken(PropertyDefinition.USER.getString(props),
              hostSpec.getHost(), port, region);
      LOGGER.log(Level.FINEST, "generated new IAM token = " + token);
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
    // Check Hostname
    final Pattern auroraDnsPattern =
            Pattern.compile(
                    "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+"
                        + "\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
                    Pattern.CASE_INSENSITIVE);
    final Matcher matcher = auroraDnsPattern.matcher(hostname);
    if (!matcher.find()) {
      // Does not match Amazon's Hostname, throw exception
      final String exceptionMessage = String.format("Unsupported AWS hostname '%s'. "
              + "Amazon domain name in format *.AWS-Region.rds.amazonaws.com is expected", hostname);

      LOGGER.log(Level.FINEST, exceptionMessage);
      throw new SQLException(
              (exceptionMessage));
    }

    // Get Region
    final String rdsRegion = matcher.group(3);

    // Check Region
    final Optional<Region> regionOptional = Region.regions().stream()
            .filter(r -> r.id().equalsIgnoreCase(rdsRegion))
            .findFirst();

    if (!regionOptional.isPresent()) {
      final String exceptionMessage = String.format("Unsupported AWS region '%s'. "
                      + "For supported regions, please read "
                      + "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html\n",
              rdsRegion);

      LOGGER.log(Level.FINEST, exceptionMessage);
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
