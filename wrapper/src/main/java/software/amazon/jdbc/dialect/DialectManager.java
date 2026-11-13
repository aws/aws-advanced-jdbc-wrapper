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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;
import software.amazon.jdbc.util.storage.CacheMap;

public class DialectManager implements DialectProvider {

  private static final Logger LOGGER = Logger.getLogger(DialectManager.class.getName());

  public static final AwsWrapperProperty DIALECT = new AwsWrapperProperty(
      "wrapperDialect", "",
      "A unique identifier for the supported database dialect.");

  /**
   * Every Dialect implementation SHOULD BE stateless!!!
   * Dialect objects are shared between different connections.
   */
  protected static final Map<String, Dialect> knownDialectsByCode =
      new HashMap<String, Dialect>() {
        {
          put(DialectCodes.MYSQL, new MysqlDialect());
          put(DialectCodes.PG, new PgDialect());
          put(DialectCodes.MARIADB, new MariaDbDialect());
          put(DialectCodes.RDS_MYSQL, new RdsMysqlDialect());
          put(DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER, new MultiAzClusterMysqlDialect());
          put(DialectCodes.RDS_PG, new RdsPgDialect());
          put(DialectCodes.RDS_MULTI_AZ_PG_CLUSTER, new MultiAzClusterPgDialect());
          put(DialectCodes.GLOBAL_AURORA_MYSQL, new GlobalAuroraMysqlDialect());
          put(DialectCodes.AURORA_MYSQL, new AuroraMysqlDialect());
          put(DialectCodes.GLOBAL_AURORA_PG, new GlobalAuroraPgDialect());
          put(DialectCodes.AURORA_PG, new AuroraPgDialect());
          put(DialectCodes.UNKNOWN, new UnknownDialect());
        }
      };

  /**
   * In order to simplify dialect detection, there's an internal host-to-dialect cache.
   * The cache contains host endpoints and identified dialect. Cache expiration time
   * is defined by the variable below.
   */
  protected static final long ENDPOINT_CACHE_EXPIRATION = TimeUnit.HOURS.toNanos(24);

  // Keys are "host" or "host:port" of the endpoint the application connects to; values are dialect codes.
  protected static final CacheMap<String, String> knownEndpointDialects = new CacheMap<>();

  private final RdsUtils rdsHelper = new RdsUtils();
  private final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  private final DialectDetector dialectDetector;
  private boolean canUpdate = false;
  private @Nullable Dialect dialect = null;
  private String dialectCode;
  private boolean confirmed = false;

  private final PluginService pluginService;

  static {
    PropertyDefinition.registerPluginProperties(DialectManager.class);
  }

  public DialectManager(PluginService pluginService) {
    this(pluginService, new DialectDetector());
  }

  public DialectManager(PluginService pluginService, DialectDetector dialectDetector) {
    this.pluginService = pluginService;
    this.dialectDetector = dialectDetector;
  }

  public static void resetEndpointCache() {
    knownEndpointDialects.clear();
  }

  @Override
  public boolean isConfirmedDialect() {
    return this.confirmed;
  }

  @Override
  public Dialect getDialect(
      final @NonNull String driverProtocol,
      final @NonNull String url,
      final @NonNull Properties props)
      throws SQLException {

    this.canUpdate = false;
    this.confirmed = false;
    this.dialect = null;

    final Dialect customDialect = Driver.getCustomDialect();
    if (customDialect != null) {
      this.dialectCode = DialectCodes.CUSTOM;
      this.dialect = customDialect;
      this.logCurrentDialect();
      return customDialect;
    }

    final String userDialectSetting = DIALECT.getString(props);
    if (!StringUtils.isNullOrEmpty(userDialectSetting)) {
      return this.useKnownDialect(userDialectSetting);
    }

    if (StringUtils.isNullOrEmpty(driverProtocol)) {
      throw new IllegalArgumentException("protocol");
    }

    String host = url;
    String cacheKey = url;
    final List<HostSpec> hosts = this.connectionUrlParser.getHostsFromConnectionUrl(
        url, true, pluginService::getHostSpecBuilder);
    if (!Utils.isNullOrEmpty(hosts)) {
      host = hosts.get(0).getHost();
      cacheKey = hosts.get(0).getHostAndPort();
    }

    final String cachedDialectCode = knownEndpointDialects.get(cacheKey);
    if (!StringUtils.isNullOrEmpty(cachedDialectCode)) {
      return this.useKnownDialect(cachedDialectCode);
    }

    if (driverProtocol.contains("mysql")) {
      RdsUrlType type = this.rdsHelper.identifyRdsType(host);
      if (type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) {
        final Dialect resolved = getDialectByCode(DialectCodes.GLOBAL_AURORA_MYSQL);
        this.canUpdate = false;
        this.dialectCode = DialectCodes.GLOBAL_AURORA_MYSQL;
        this.dialect = resolved;
        this.confirmed = true;
        return resolved;
      }
      if (type.isRdsCluster()) {
        final Dialect resolved = getDialectByCode(DialectCodes.AURORA_MYSQL);
        this.canUpdate = true;
        this.dialectCode = DialectCodes.AURORA_MYSQL;
        this.dialect = resolved;
        return resolved;
      }
      if (type.isRds()) {
        final Dialect resolved = getDialectByCode(DialectCodes.RDS_MYSQL);
        this.canUpdate = true;
        this.dialectCode = DialectCodes.RDS_MYSQL;
        this.dialect = resolved;
        this.logCurrentDialect();
        return resolved;
      }
      final Dialect resolved = getDialectByCode(DialectCodes.MYSQL);
      this.canUpdate = true;
      this.dialectCode = DialectCodes.MYSQL;
      this.dialect = resolved;
      this.logCurrentDialect();
      return resolved;
    }

    if (driverProtocol.contains("postgresql")) {
      RdsUrlType type = this.rdsHelper.identifyRdsType(host);
      if (RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP.equals(type)) {
        final Dialect resolved = getDialectByCode(DialectCodes.AURORA_PG);
        this.canUpdate = false;
        this.dialectCode = DialectCodes.AURORA_PG;
        this.dialect = resolved;
        this.confirmed = true;
        return resolved;
      }
      if (RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER.equals(type)) {
        final Dialect resolved = getDialectByCode(DialectCodes.GLOBAL_AURORA_PG);
        this.canUpdate = false;
        this.dialectCode = DialectCodes.GLOBAL_AURORA_PG;
        this.dialect = resolved;
        this.confirmed = true;
        return resolved;
      }
      if (type.isRdsCluster()) {
        final Dialect resolved = getDialectByCode(DialectCodes.AURORA_PG);
        this.canUpdate = true;
        this.dialectCode = DialectCodes.AURORA_PG;
        this.dialect = resolved;
        return resolved;
      }
      if (type.isRds()) {
        final Dialect resolved = getDialectByCode(DialectCodes.RDS_PG);
        this.canUpdate = true;
        this.dialectCode = DialectCodes.RDS_PG;
        this.dialect = resolved;
        this.logCurrentDialect();
        return resolved;
      }
      final Dialect resolved = getDialectByCode(DialectCodes.PG);
      this.canUpdate = true;
      this.dialectCode = DialectCodes.PG;
      this.dialect = resolved;
      this.logCurrentDialect();
      return resolved;
    }

    if (driverProtocol.contains("mariadb")) {
      final Dialect resolved = getDialectByCode(DialectCodes.MARIADB);
      this.canUpdate = true;
      this.dialectCode = DialectCodes.MARIADB;
      this.dialect = resolved;
      this.logCurrentDialect();
      return resolved;
    }

    final Dialect resolved = getDialectByCode(DialectCodes.UNKNOWN);
    this.canUpdate = true;
    this.dialectCode = DialectCodes.UNKNOWN;
    this.dialect = resolved;
    this.logCurrentDialect();
    return resolved;
  }

  @Override
  public Dialect getDialect(
      final @NonNull String originalUrl,
      final @NonNull HostSpec hostSpec,
      final @NonNull Connection connection) throws SQLException {

    // this.dialect is always populated by a prior getDialect(protocol, url, props) call; guard
    // for null here to preserve the non-null return contract of DialectProvider.getDialect.
    final Dialect currentDialect = this.dialect;

    if (!this.canUpdate) {
      this.confirmed = true;
      this.logCurrentDialect();
      if (currentDialect == null) {
        throw new SQLException(Messages.get("DialectManager.unknownDialect"));
      }
      return currentDialect;
    }

    if (currentDialect == null) {
      throw new SQLException(Messages.get("DialectManager.unknownDialect"));
    }

    // The provisional dialect only narrows the search to an engine family. The fingerprint of the
    // connected server is the single source of truth for the deployment type.
    final String provisionalCode = this.dialectCode;
    final DialectFingerprint fingerprint =
        this.dialectDetector.collect(connection, DialectDetector.familyOf(provisionalCode));

    final String fingerprintCode = fingerprint == null ? null : DialectDetector.resolve(fingerprint);
    if (fingerprint != null) {
      LOGGER.finest(() -> Messages.get("DialectManager.fingerprint", new Object[] {fingerprint, fingerprintCode}));
    }

    final String resolvedCode;
    if (fingerprintCode == null || DialectCodes.UNKNOWN.equals(fingerprintCode)) {
      if (DialectCodes.UNKNOWN.equals(provisionalCode)) {
        throw new SQLException(Messages.get("DialectManager.unknownDialect"));
      }
      LOGGER.fine(() -> Messages.get("DialectManager.fingerprintUnavailable", new Object[] {provisionalCode}));
      resolvedCode = provisionalCode;
    } else {
      resolvedCode = fingerprintCode;
    }

    final Dialect resolved = getDialectByCode(resolvedCode);
    this.canUpdate = false;
    this.confirmed = true;
    this.dialectCode = resolvedCode;
    this.dialect = resolved;

    knownEndpointDialects.put(hostSpec.getHostAndPort(), resolvedCode, ENDPOINT_CACHE_EXPIRATION);

    this.logCurrentDialect();
    return resolved;
  }

  private Dialect useKnownDialect(final String dialectCode) throws SQLException {
    final Dialect known = getDialectByCode(dialectCode);
    this.dialectCode = dialectCode;
    this.dialect = known;
    this.confirmed = true;
    this.logCurrentDialect();
    return known;
  }

  private static @NonNull Dialect getDialectByCode(final String dialectCode) throws SQLException {
    // All codes passed here are constants that are registered in knownDialectsByCode, so this
    // lookup never returns null in practice; the guard preserves the non-null return contract.
    final Dialect resolved = knownDialectsByCode.get(dialectCode);
    if (resolved == null) {
      throw new SQLException(Messages.get("DialectManager.unknownDialectCode", new Object[] {dialectCode}));
    }
    return resolved;
  }

  private void logCurrentDialect() {
    LOGGER.finest(() -> Messages.get(
        "DialectManager.currentDialect",
        new Object[] {
            this.dialectCode,
            this.dialect == null ? "<null>" : this.dialect,
            this.canUpdate,
            this.confirmed
        }));
  }
}
