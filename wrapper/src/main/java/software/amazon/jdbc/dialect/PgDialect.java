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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.PgExceptionHandler;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;

/**
 * Generic dialect for any Postgresql database.
 */
public class PgDialect implements Dialect {

  protected static final String PG_PROC_EXISTS_QUERY = "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1";
  protected static final String VERSION_QUERY = "SELECT 'version', pg_catalog.VERSION()";
  protected static final String HOST_ALIAS_QUERY =
      "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())";

  protected static final DialectUtils dialectUtils = new DialectUtils();

  private static PgExceptionHandler pgExceptionHandler;
  private static final EnumSet<FailoverRestriction> NO_FAILOVER_RESTRICTIONS =
      EnumSet.noneOf(FailoverRestriction.class);
  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.GLOBAL_AURORA_PG,
      DialectCodes.AURORA_PG,
      DialectCodes.RDS_MULTI_AZ_PG_CLUSTER,
      DialectCodes.RDS_PG);

  @Override
  public boolean isDialect(final Connection connection) {
    return dialectUtils.checkExistenceQueries(connection, PG_PROC_EXISTS_QUERY);
  }

  @Override
  public int getDefaultPort() {
    return 5432;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (pgExceptionHandler == null) {
      pgExceptionHandler = new PgExceptionHandler();
    }
    return pgExceptionHandler;
  }

  @Override
  public HostListProviderSupplier getHostListProviderSupplier() {
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
    return NO_FAILOVER_RESTRICTIONS;
  }

  @Override
  public String getServerVersionQuery() {
    return VERSION_QUERY;
  }

  @Override
  public String getHostAliasQuery() {
    return HOST_ALIAS_QUERY;
  }
}
