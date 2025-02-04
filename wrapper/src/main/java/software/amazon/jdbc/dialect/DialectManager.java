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
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.CacheMap;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.Utils;

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
          put(DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER, new RdsMultiAzDbClusterMysqlDialect());
          put(DialectCodes.RDS_PG, new RdsPgDialect());
          put(DialectCodes.RDS_MULTI_AZ_PG_CLUSTER, new RdsMultiAzDbClusterPgDialect());
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

  // Map of host name, or url, by dialect code.
  protected static final CacheMap<String, String> knownEndpointDialects = new CacheMap<>();

  private final RdsUtils rdsHelper = new RdsUtils();
  private final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  private boolean canUpdate = false;
  private Dialect dialect = null;
  private String dialectCode;

  private PluginService pluginService;

  public DialectManager(PluginService pluginService) {
    this.pluginService = pluginService;
  }

  /**
   * Sets a custom dialect handler.
   *
   * @deprecated Use software.amazon.jdbc.Driver instead
   */
  @Deprecated
  public static void setCustomDialect(final @NonNull Dialect dialect) {
    Driver.setCustomDialect(dialect);
  }

  /**
   * Resets a custom dialect handler.
   *
   * @deprecated Use software.amazon.jdbc.Driver instead
   */
  @Deprecated
  public static void resetCustomDialect() {
    Driver.resetCustomDialect();
  }

  public static void resetEndpointCache() {
    knownEndpointDialects.clear();
  }

  @Override
  public Dialect getDialect(
      final @NonNull String driverProtocol,
      final @NonNull String url,
      final @NonNull Properties props)
      throws SQLException {

    this.canUpdate = false;
    this.dialect = null;

    final Dialect customDialect = Driver.getCustomDialect();
    if (customDialect != null) {
      this.dialectCode = DialectCodes.CUSTOM;
      this.dialect = customDialect;
      this.logCurrentDialect();
      return this.dialect;
    }

    final String userDialectSetting = DIALECT.getString(props);
    final String dialectCode = !StringUtils.isNullOrEmpty(userDialectSetting)
        ? userDialectSetting
        : knownEndpointDialects.get(url);

    if (!StringUtils.isNullOrEmpty(dialectCode)) {
      final Dialect userDialect = knownDialectsByCode.get(dialectCode);
      if (userDialect != null) {
        this.dialectCode = dialectCode;
        this.dialect = userDialect;
        this.logCurrentDialect();
        return userDialect;
      } else {
        throw new SQLException(
            Messages.get("DialectManager.unknownDialectCode", new Object[] {dialectCode}));
      }
    }

    if (StringUtils.isNullOrEmpty(driverProtocol)) {
      throw new IllegalArgumentException("protocol");
    }

    String host = url;
    final List<HostSpec> hosts = this.connectionUrlParser.getHostsFromConnectionUrl(url, true,
        () -> pluginService.getHostSpecBuilder());
    if (!Utils.isNullOrEmpty(hosts)) {
      host = hosts.get(0).getHost();
    }

    if (driverProtocol.contains("mysql")) {
      RdsUrlType type = this.rdsHelper.identifyRdsType(host);
      if (type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER) {
        this.canUpdate = false;
        this.dialectCode = DialectCodes.GLOBAL_AURORA_MYSQL;
        this.dialect = knownDialectsByCode.get(DialectCodes.GLOBAL_AURORA_MYSQL);
        return this.dialect;
      }
      if (type.isRdsCluster()) {
        this.canUpdate = true;
        this.dialectCode = DialectCodes.AURORA_MYSQL;
        this.dialect = knownDialectsByCode.get(DialectCodes.AURORA_MYSQL);
        return this.dialect;
      }
      if (type.isRds()) {
        this.canUpdate = true;
        this.dialectCode = DialectCodes.RDS_MYSQL;
        this.dialect = knownDialectsByCode.get(DialectCodes.RDS_MYSQL);
        this.logCurrentDialect();
        return this.dialect;
      }
      this.canUpdate = true;
      this.dialectCode = DialectCodes.MYSQL;
      this.dialect = knownDialectsByCode.get(DialectCodes.MYSQL);
      this.logCurrentDialect();
      return this.dialect;
    }

    if (driverProtocol.contains("postgresql")) {
      RdsUrlType type = this.rdsHelper.identifyRdsType(host);
      if (RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP.equals(type)) {
        this.canUpdate = false;
        this.dialectCode = DialectCodes.AURORA_PG;
        this.dialect = knownDialectsByCode.get(DialectCodes.AURORA_PG);
        return this.dialect;
      }
      if (RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER.equals(type)) {
        this.canUpdate = false;
        this.dialectCode = DialectCodes.GLOBAL_AURORA_PG;
        this.dialect = knownDialectsByCode.get(DialectCodes.GLOBAL_AURORA_PG);
        return this.dialect;
      }
      if (type.isRdsCluster()) {
        this.canUpdate = true;
        this.dialectCode = DialectCodes.AURORA_PG;
        this.dialect = knownDialectsByCode.get(DialectCodes.AURORA_PG);
        return this.dialect;
      }
      if (type.isRds()) {
        this.canUpdate = true;
        this.dialectCode = DialectCodes.RDS_PG;
        this.dialect = knownDialectsByCode.get(DialectCodes.RDS_PG);
        this.logCurrentDialect();
        return this.dialect;
      }
      this.canUpdate = true;
      this.dialectCode = DialectCodes.PG;
      this.dialect = knownDialectsByCode.get(DialectCodes.PG);
      this.logCurrentDialect();
      return this.dialect;
    }

    if (driverProtocol.contains("mariadb")) {
      this.canUpdate = true;
      this.dialectCode = DialectCodes.MARIADB;
      this.dialect = knownDialectsByCode.get(DialectCodes.MARIADB);
      this.logCurrentDialect();
      return this.dialect;
    }

    this.canUpdate = true;
    this.dialectCode = DialectCodes.UNKNOWN;
    this.dialect = knownDialectsByCode.get(DialectCodes.UNKNOWN);
    this.logCurrentDialect();
    return this.dialect;
  }

  @Override
  public Dialect getDialect(
      final @NonNull String originalUrl,
      final @NonNull HostSpec hostSpec,
      final @NonNull Connection connection) throws SQLException {

    if (!this.canUpdate) {
      this.logCurrentDialect();
      return this.dialect;
    }

    final List<String> dialectCandidates = this.dialect.getDialectUpdateCandidates();
    if (dialectCandidates != null) {
      for (String dialectCandidateCode : dialectCandidates) {
        Dialect dialectCandidate = knownDialectsByCode.get(dialectCandidateCode);
        if (dialectCandidate == null) {
          throw new SQLException(
              Messages.get("DialectManager.unknownDialectCode", new Object[] {dialectCandidateCode}));
        }
        boolean isDialect = dialectCandidate.isDialect(connection);
        if (isDialect) {
          this.canUpdate = false;
          this.dialectCode = dialectCandidateCode;
          this.dialect = dialectCandidate;

          knownEndpointDialects.put(originalUrl, dialectCandidateCode, ENDPOINT_CACHE_EXPIRATION);
          knownEndpointDialects.put(hostSpec.getUrl(), dialectCandidateCode, ENDPOINT_CACHE_EXPIRATION);

          this.logCurrentDialect();
          return this.dialect;
        }
      }
    }

    if (DialectCodes.UNKNOWN.equals(this.dialectCode)) {
      throw new SQLException(Messages.get("DialectManager.unknownDialect"));
    }

    this.canUpdate = false;

    knownEndpointDialects.put(originalUrl, this.dialectCode, ENDPOINT_CACHE_EXPIRATION);
    knownEndpointDialects.put(hostSpec.getUrl(), this.dialectCode, ENDPOINT_CACHE_EXPIRATION);

    this.logCurrentDialect();
    return this.dialect;
  }

  private void logCurrentDialect() {
    LOGGER.finest(() -> String.format("Current dialect: %s, %s, canUpdate: %b",
        this.dialectCode,
        this.dialect == null ? "<null>" : this.dialect,
        this.canUpdate));
  }
}
