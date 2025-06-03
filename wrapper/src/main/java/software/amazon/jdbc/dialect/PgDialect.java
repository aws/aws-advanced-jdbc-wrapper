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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.PgExceptionHandler;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.DriverInfo;

/**
 * Generic dialect for any Postgresql database.
 */
public class PgDialect implements Dialect {

  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.AURORA_PG,
      DialectCodes.RDS_MULTI_AZ_PG_CLUSTER,
      DialectCodes.RDS_PG);

  private static PgExceptionHandler pgExceptionHandler;

  private static final EnumSet<FailoverRestriction> NO_RESTRICTIONS = EnumSet.noneOf(FailoverRestriction.class);

  @Override
  public int getDefaultPort() {
    return 5432;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (pgExceptionHandler == null) {
      pgExceptionHandler = new PgExceptionHandler();
    }
    return pgExceptionHandler;
  }

  @Override
  public String getHostAliasQuery() {
    return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";
  }

  @Override
  public String getServerVersionQuery(final Properties properties) {
    // We'd like to make this query as "unique" as possible so we add such wierd WHERE conditions.
    // That helps to keep this query in pg_stat_statements.
    return String.format(
        "SELECT /* _driver:aws_jdbc_driver,_driver_version:%s,_jdbc_wrapper_plugins:%s */ 'version', VERSION()"
            + " WHERE 1 > 0 AND 0 < 1",
        DriverInfo.DRIVER_VERSION,
        properties.getProperty(ConnectionPluginManager.EFFECTIVE_PLUGIN_CODES_PROPERTY));
  }

  @Override
  public boolean isDialect(final Connection connection, final Properties properties) {
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(this.getServerVersionQuery(properties))) {
        if (!rs.next()) {
          return false;
        }

        String version = rs.getString(2);
        return version != null && version.toLowerCase().contains("postgresql");
      }
    } catch (SQLException ex) {
      return false;
    }
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, servicesContainer) ->
        new ConnectionStringHostListProvider(properties, initialUrl, servicesContainer.getHostListProviderService());
  }

  @Override
  public void prepareConnectProperties(
      final @NonNull Properties connectProperties, final @NonNull String protocol, final @NonNull HostSpec hostSpec) {
    // do nothing
  }

  @Override
  public EnumSet<FailoverRestriction> getFailoverRestrictions() {
    return NO_RESTRICTIONS;
  }

  @Override
  public void reportMetadata(@NonNull Connection connection, @NonNull Properties properties) {
    // We'd like to execute a query that includes a comment with a driver metadata.
    this.isDialect(connection, properties);
  }
}
