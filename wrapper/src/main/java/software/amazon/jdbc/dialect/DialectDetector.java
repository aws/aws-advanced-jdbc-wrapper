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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.dialect.DialectFingerprint.Family;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * Identifies the database dialect of a live connection in two steps: collect a
 * {@link DialectFingerprint} with one error-free statement per engine family (plus at most one
 * follow-up for data-dependent deployments), then map the fingerprint to a dialect code with a
 * pure decision table in {@link #resolve(DialectFingerprint)}.
 *
 * <p>Every statement is written so that it succeeds on any server of its family, regardless of
 * deployment. Detection therefore never relies on a server error as a signal and never leaves
 * error entries in the server log.
 */
public class DialectDetector {

  private static final Logger LOGGER = Logger.getLogger(DialectDetector.class.getName());

  // --- PostgreSQL family ---

  private static String pgProcExists(final String schema, final String... names) {
    final StringBuilder sb = new StringBuilder();
    sb.append("EXISTS (SELECT 1 FROM pg_catalog.pg_proc p")
        .append(" JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace")
        .append(" WHERE n.nspname OPERATOR(pg_catalog.=) '").append(schema).append("'")
        .append(" AND (");
    for (int i = 0; i < names.length; i++) {
      if (i > 0) {
        sb.append(" OR ");
      }
      sb.append("p.proname OPERATOR(pg_catalog.=) '").append(names[i]).append("'");
    }
    sb.append("))");
    return sb.toString();
  }

  /**
   * Single statement that reports the presence of each deployment-specific object. Valid on any
   * PostgreSQL-compatible server because it only reads {@code pg_catalog}.
   */
  protected static final String PG_FINGERPRINT_QUERY = "SELECT "
      + pgProcExists("pg_catalog", "aurora_version", "aurora_replica_status") + " AS aurora, "
      + pgProcExists("pg_catalog", "aurora_limitless_router_endpoints") + " AS limitless, "
      + pgProcExists("pg_catalog", "aurora_global_db_status") + " AS global_db_capable, "
      + pgProcExists("rds_tools", "multi_az_db_cluster_source_dbi_resource_id") + " AS multi_az_capable, "
      + "EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'rds.extensions') AS rds, "
      + "pg_catalog.current_setting('server_version') AS server_version";

  // --- MySQL family ---

  /**
   * Returns one row per variable that exists; a variable unknown to the server is simply absent,
   * so the statement never fails on community MySQL, MariaDB, RDS or Aurora.
   */
  protected static final String MYSQL_VARIABLES_QUERY =
      "SHOW VARIABLES WHERE Variable_name IN ('version', 'version_comment', 'aurora_version', 'report_host')";

  /** Presence of RDS- and Aurora-installed catalog objects, from information_schema only. */
  protected static final String MYSQL_CATALOG_QUERY = "SELECT"
      + " (SELECT COUNT(*) FROM information_schema.tables"
      + " WHERE table_schema = 'mysql' AND table_name = 'rds_topology') AS rds_topology,"
      + " (SELECT COUNT(*) FROM information_schema.routines"
      + " WHERE routine_schema = 'mysql' AND routine_name = 'rds_set_configuration') AS rds_routines,"
      + " (SELECT COUNT(*) FROM information_schema.tables"
      + " WHERE upper(table_schema) = 'INFORMATION_SCHEMA'"
      + " AND upper(table_name) = 'AURORA_GLOBAL_DB_STATUS') AS global_db_status";

  private static final String RDS_MYSQL_VERSION_COMMENT = "Source distribution";

  /**
   * Maps a provisional dialect code (derived from the URL before connecting) to the engine family
   * whose fingerprint statements should be used.
   */
  public static Family familyOf(final @Nullable String dialectCode) {
    if (dialectCode == null) {
      return Family.UNKNOWN;
    }
    switch (dialectCode) {
      case DialectCodes.PG:
      case DialectCodes.RDS_PG:
      case DialectCodes.RDS_MULTI_AZ_PG_CLUSTER:
      case DialectCodes.AURORA_PG:
      case DialectCodes.GLOBAL_AURORA_PG:
        return Family.PG;
      case DialectCodes.MYSQL:
      case DialectCodes.RDS_MYSQL:
      case DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER:
      case DialectCodes.AURORA_MYSQL:
      case DialectCodes.GLOBAL_AURORA_MYSQL:
      case DialectCodes.MARIADB:
        return Family.MYSQL;
      default:
        return Family.UNKNOWN;
    }
  }

  /**
   * Determines the engine family without a round trip, from the target driver's product name
   * (every supported target driver answers from a constant or from handshake data).
   */
  public Family familyFromMetadata(final Connection connection) {
    try {
      final String product = connection.getMetaData().getDatabaseProductName();
      if (product == null) {
        return Family.UNKNOWN;
      }
      final String lower = product.toLowerCase(Locale.ROOT);
      if (lower.contains("postgres")) {
        return Family.PG;
      }
      if (lower.contains("mysql") || lower.contains("mariadb")) {
        return Family.MYSQL;
      }
    } catch (final SQLException | RuntimeException ex) {
      LOGGER.finest(() -> Messages.get("DialectDetector.productNameUnavailable", new Object[] {ex.getMessage()}));
    }
    return Family.UNKNOWN;
  }

  /**
   * Collects the fingerprint of the connected server.
   *
   * @param connection an open connection to the server
   * @param familyHint the family suggested by the URL and protocol; {@link Family#UNKNOWN} lets
   *                   the detector find out from driver metadata and, failing that, by trying
   *                   each family in turn
   * @return the fingerprint, or null if no family's fingerprint statement could be executed
   */
  public @Nullable DialectFingerprint collect(final @NonNull Connection connection, final Family familyHint) {
    Family family = familyHint;
    if (family == Family.UNKNOWN) {
      family = this.familyFromMetadata(connection);
    }

    switch (family) {
      case PG:
        return this.collectPg(connection);
      case MYSQL:
        return this.collectMysql(connection);
      default:
        final DialectFingerprint pg = this.collectPg(connection);
        return pg != null ? pg : this.collectMysql(connection);
    }
  }

  /**
   * Pure decision table: maps a fingerprint to a dialect code. Has no side effects and runs no
   * SQL, so every row of the table can be verified by a unit test without a database.
   */
  public static String resolve(final @NonNull DialectFingerprint fp) {
    switch (fp.getFamily()) {
      case PG:
        if (fp.isAurora() || fp.isLimitless()) {
          return fp.getGlobalDbRegionCount() > 1 ? DialectCodes.GLOBAL_AURORA_PG : DialectCodes.AURORA_PG;
        }
        if (fp.isRds()) {
          return fp.isMultiAzCluster() ? DialectCodes.RDS_MULTI_AZ_PG_CLUSTER : DialectCodes.RDS_PG;
        }
        return DialectCodes.PG;

      case MYSQL:
        if (fp.isMariaDb()) {
          return DialectCodes.MARIADB;
        }
        if (fp.isAurora()) {
          return fp.getGlobalDbRegionCount() > 1 ? DialectCodes.GLOBAL_AURORA_MYSQL : DialectCodes.AURORA_MYSQL;
        }
        if (fp.isRds()) {
          return fp.isMultiAzCluster() ? DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER : DialectCodes.RDS_MYSQL;
        }
        return DialectCodes.MYSQL;

      default:
        return DialectCodes.UNKNOWN;
    }
  }

  protected @Nullable DialectFingerprint collectPg(final Connection connection) {
    final DialectFingerprint.Builder b = DialectFingerprint.builder(Family.PG);
    final boolean aurora;
    final boolean rds;
    final boolean globalDbCapable;
    final boolean multiAzCapable;
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(PG_FINGERPRINT_QUERY)) {
      if (!rs.next()) {
        return null;
      }
      aurora = rs.getBoolean("aurora");
      rds = rs.getBoolean("rds");
      globalDbCapable = rs.getBoolean("global_db_capable");
      multiAzCapable = rs.getBoolean("multi_az_capable");
      b.aurora(aurora)
          .limitless(rs.getBoolean("limitless"))
          .globalDbCapable(globalDbCapable)
          .multiAzClusterCapable(multiAzCapable)
          .rds(rds)
          .serverVersion(rs.getString("server_version"));
    } catch (final SQLException ex) {
      LOGGER.finest(() -> Messages.get("DialectDetector.fingerprintFailed", new Object[] {Family.PG, ex.getMessage()}));
      return null;
    }

    if (aurora && globalDbCapable) {
      b.globalDbRegionCount(this.queryInt(connection, GlobalAuroraPgDialect.REGION_COUNT_QUERY));
    }
    if (!aurora && rds && multiAzCapable) {
      b.multiAzCluster(this.queryStringIsPresent(connection, MultiAzClusterPgDialect.IS_RDS_CLUSTER_QUERY));
    }
    return b.build();
  }

  protected @Nullable DialectFingerprint collectMysql(final Connection connection) {
    final Map<String, @Nullable String> vars = new HashMap<>();
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(MYSQL_VARIABLES_QUERY)) {
      while (rs.next()) {
        final String name = rs.getString(1);
        if (name != null) {
          vars.put(name.toLowerCase(Locale.ROOT), rs.getString(2));
        }
      }
    } catch (final SQLException ex) {
      LOGGER.finest(() ->
          Messages.get("DialectDetector.fingerprintFailed", new Object[] {Family.MYSQL, ex.getMessage()}));
      return null;
    }

    final String version = vars.get("version");
    final String versionComment = vars.get("version_comment");
    final boolean aurora = vars.containsKey("aurora_version");
    final boolean mariaDb = containsIgnoreCase(version, "mariadb") || containsIgnoreCase(versionComment, "mariadb");
    final boolean reportHostSet = !StringUtils.isNullOrEmpty(vars.get("report_host"));

    int rdsTopology = 0;
    int rdsRoutines = 0;
    int globalDbStatus = 0;
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(MYSQL_CATALOG_QUERY)) {
      if (rs.next()) {
        rdsTopology = rs.getInt("rds_topology");
        rdsRoutines = rs.getInt("rds_routines");
        globalDbStatus = rs.getInt("global_db_status");
      }
    } catch (final SQLException ex) {
      // The variables alone still identify MariaDB, Aurora and community MySQL; only the RDS
      // catalog markers are lost, and the version comment below still covers RDS MySQL.
      LOGGER.finest(() -> Messages.get("DialectDetector.catalogFailed", new Object[] {ex.getMessage()}));
    }

    final boolean rds = rdsRoutines > 0 || rdsTopology > 0
        || RDS_MYSQL_VERSION_COMMENT.equalsIgnoreCase(versionComment);
    final boolean globalDbCapable = globalDbStatus > 0;

    final DialectFingerprint.Builder b = DialectFingerprint.builder(Family.MYSQL)
        .serverVersion(version)
        .mariaDb(mariaDb)
        .aurora(aurora)
        .globalDbCapable(globalDbCapable)
        .rds(rds)
        .multiAzClusterCapable(rdsTopology > 0)
        .multiAzCluster(!aurora && rdsTopology > 0 && reportHostSet);

    if (aurora && !mariaDb && globalDbCapable) {
      b.globalDbRegionCount(this.queryInt(connection, GlobalAuroraMysqlDialect.REGION_COUNT_QUERY));
    }
    return b.build();
  }

  private int queryInt(final Connection connection, final String query) {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        return rs.getInt(1);
      }
    } catch (final SQLException ex) {
      LOGGER.finest(() -> Messages.get("DialectDetector.followUpFailed", new Object[] {query, ex.getMessage()}));
    }
    return DialectFingerprint.REGION_COUNT_NOT_QUERIED;
  }

  private boolean queryStringIsPresent(final Connection connection, final String query) {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      return rs.next() && rs.getString(1) != null;
    } catch (final SQLException ex) {
      LOGGER.finest(() -> Messages.get("DialectDetector.followUpFailed", new Object[] {query, ex.getMessage()}));
      return false;
    }
  }

  private static boolean containsIgnoreCase(final @Nullable String value, final String needle) {
    return value != null && value.toLowerCase(Locale.ROOT).contains(needle);
  }
}
