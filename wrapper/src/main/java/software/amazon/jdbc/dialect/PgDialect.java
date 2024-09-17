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
  public String getServerVersionQuery() {
    return "SELECT 'version', VERSION()";
  }

  @Override
  public boolean isDialect(final Connection connection) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery("SELECT 1 FROM pg_proc LIMIT 1");
      if (rs.next()) {
        return true;
      }
    } catch (final SQLException ex) {
      // ignore
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
    }
    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService, pluginService) ->
        new ConnectionStringHostListProvider(properties, initialUrl, hostListProviderService);
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
}
